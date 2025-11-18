#!/h20/home/lab/miniconda3/envs/mesospim_dev/bin/python -i

import os
from dask import delayed
import dask.array as da
from typing import Union
from pathlib import Path
import math
import tifffile
import threading, queue
import numpy as np
import zarr
# from zarr.codecs import BloscCodec, BloscShuffle, ShardingCodec
from dataclasses import dataclass
from typing import Tuple
from enum import Enum
import os, concurrent.futures
from zarr_stores.h5_nested_store import H5_Nested_Store
from numcodecs import Blosc
from fli_loading import fli_loading
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
VERBOSE = 2

class mesospim_btf_helper:

    def __init__(self,path: Union[str, Path]):
        self.path = path if isinstance(path,Path) else Path(path)
        self.tif = tifffile.TiffFile(self.path, mode='r')
        self.sample_data = self.tif.series[0].asarray()
        self.zdim = len(self.tif.series)

        self.build_lazy_array()

    def __getitem__(self, item):
        if VERBOSE > 1: print(f"GETTING: {item}")
        return self.lazy_array[item].compute()

    def build_lazy_array(self):
        if VERBOSE: print('Building Array')
        delayed_image_reads = [delayed(self.get_z_plane)(x) for x in range(self.zdim)]
        delayed_arrays = [da.from_delayed(x, shape=self.sample_data.shape, dtype=self.sample_data.dtype)[0] for x in delayed_image_reads]
        self.lazy_array = da.stack(delayed_arrays)
        if VERBOSE > 1: print(self.lazy_array)

    def get_z_plane(self,z_plane: int):
        return self.tif.series[z_plane].asarray()

    @property
    def shape(self):
        return self.lazy_array.shape

    @property
    def chunksize(self):
        return self.lazy_array.chunksize

    @property
    def nbytes(self):
        return self.lazy_array.nbytes

    @property
    def dtype(self):
        return self.lazy_array.dtype

    @property
    def chunks(self):
        return self.lazy_array.chunks

    @property
    def ndim(self):
        return self.lazy_array.ndim

    def __del__(self):
        del self.lazy_array
        self.tif.close()
        del self.tif


### Multiscale writer ###

# at top-level (after imports)
try:
    import blosc2            # zarr v3 uses python-blosc2
    blosc2.set_nthreads(max(1, os.cpu_count() // 2))  # e.g., half your cores
except Exception:
    pass

STORE_PATH = "volume.ome.zarr"
CHUNK_YX = 1024  # tune for your detector/tile size

# ---------- Helpers ----------
def ceil_div(a, b):  # integer ceil
    return -(-a // b)



def ds2_mean_uint16(img: np.ndarray) -> np.ndarray:
    y, x = img.shape
    y2 = y - (y & 1); x2 = x - (x & 1)
    a = img[:y2:2, :x2:2].astype(np.uint32)
    b = img[1:y2:2, :x2:2].astype(np.uint32)
    c = img[:y2:2, 1:x2:2].astype(np.uint32)
    d = img[1:y2:2, 1:x2:2].astype(np.uint32)
    out = (a + b + c + d + 2) >> 2              # +2 for rounding
    # pad edge by replication if odd dims:
    if y & 1: out = np.vstack([out, out[-1:]])
    if x & 1: out = np.hstack([out, out[:, -1:]])
    return out.astype(np.uint16)

   

def dsZ2_mean_uint16(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Mean of two uint16 slices -> uint16."""
    return ((a.astype(np.uint32) + b.astype(np.uint32) + 1) >> 1).astype(np.uint16)

def infer_n_levels(y, x, z_estimate, min_dim=256):
    """Stop when any axis would shrink below min_dim (spatial) or z_estimate//2**L < 1."""
    levels = 1
    while (min(y, x) // (2 ** levels) >= min_dim) and (z_estimate // (2 ** levels) >= 1):
        levels += 1
    return levels


def compute_xy_only_levels(voxel_size):
    """Return how many leading pyramid levels should be XY-only (no Z decimation),
    so that XY spacing never exceeds Z spacing."""
    dz, dy, dx = map(float, voxel_size)
    # how many 2x steps can XY take without exceeding dz?
    ky = 0 if dy <= 0 else max(0, math.floor(math.log2(dz / dy)))
    kx = 0 if dx <= 0 else max(0, math.floor(math.log2(dz / dx)))
    return int(max(0, min(ky, kx)))  # lockstep XY downsampling

def level_factors(level: int, xy_levels: int):
    """Per-level physical scaling factors relative to level 0 for (z,y,x)."""
    if level <= xy_levels:
        zf = 1
    else:
        zf = 2 ** (level - xy_levels)
    yf = 2 ** level
    xf = 2 ** level
    return zf, yf, xf

def plan_levels(y, x, z_estimate, xy_levels, min_dim=256):
    """Total levels given XY-only prelude, then 3D, stopping when
       min(y_l, x_l) < min_dim or z_l < 1."""
    L = 1
    while True:
        zf, yf, xf = level_factors(L, xy_levels)
        y_l = ceil_div(y, yf)
        x_l = ceil_div(x, xf)
        z_l = ceil_div(z_estimate, zf)
        if min(y_l, x_l) < min_dim or z_l < 1:
            break
        L += 1
    return L


@dataclass
class ChunkSpec:
    z: int = 8
    y: int = 512
    x: int = 512

@dataclass
class PyramidSpec:
    z_size_estimate: int   # upper bound; we trim on close()
    y: int
    x: int
    levels: int




@dataclass
class ChunkScheme:
    """
    Per-level chunking policy that evolves from base -> target as levels increase.
    - base applies at level 0 (e.g., z=8, y=512, x=512)
    - at each level l, we:
        zc = min(target.z, base.z * 2**l)         # z chunk grows toward target
        yc = max(target.y, max(1, base.y // 2**l))# y/x chunks shrink toward target
        xc = max(target.x, max(1, base.x // 2**l))
    Then we clamp to the level's array shape.
    """
    base: Tuple[int, int, int] = (1, 1024, 1024)    # (z, y, x) at level 0
    target: Tuple[int, int, int] = (64, 64, 64)   # desired asymptote

    def chunks_for_level(self, level: int, zyx_level_shape: Tuple[int, int, int]) -> Tuple[int, int, int]:
        z_l, y_l, x_l = zyx_level_shape
        bz, by, bx = self.base
        tz, ty, tx = self.target

        zc = min(tz, max(1, bz * (2 ** level)))
        yc = max(ty, max(1, by // (2 ** level)))
        xc = max(tx, max(1, bx // (2 ** level)))

        # Clamp to the level's actual dimensions
        return (min(zc, z_l), min(yc, y_l), min(xc, x_l))


import math
from typing import Tuple, Optional

def _validate_divisible(chunks: Tuple[int,int,int], shards: Tuple[int,int,int]) -> bool:
    return all(c % s == 0 for c, s in zip(chunks, shards))

def _coerce_shards(chunks: Tuple[int,int,int],
                   desired: Tuple[int,int,int]) -> Tuple[int,int,int]:
    """
    Guaranteed-valid shards for Zarr v3: choose a divisor of each chunk dim,
    <= desired, never 0.
    """
    out = []
    for d, c in zip(desired, chunks):
        d = int(d); c = int(c)
        # clamp to chunk dim first
        s = min(max(1, d), c)
        # step down by gcd until it divides
        g = math.gcd(s, c)
        if g == 0:   # extremely defensive; shouldn't happen
            g = 1
        # If gcd(s, c) < s, try gcd(down, c) until it divides
        while c % g != 0 and g > 1:
            s = max(1, g)
            g = math.gcd(s, c)
        if c % g != 0:
            # worst-case fallback: 1
            g = 1
        out.append(g)
    return tuple(out)


def init_h5_nested_store_v2(
    spec: PyramidSpec,
    path=STORE_PATH,
    chunk_scheme: ChunkScheme = ChunkScheme(),
    compressor=None,  # pass a v2 compressor like numcodecs.Blosc
    voxel_size=(1.0, 1.0, 1.0),
    unit="micrometer",
    xy_levels: int = 0,
    shard_shape: Tuple[int, int, int] | None = None,  # ignored in v2
):
    """
    Initialize a multiscale pyramid inside an H5_Nested_Store using zarr v2 APIs.
    Creates one 5D dataset per level named s{l} with shape (1,1,z,y,x).
    """
    # Open H5-backed store and zarr v2 group (overwrite)
    store = H5_Nested_Store(path)
    root = zarr.open_group(store=store, mode="w")
    arrs = []
    for l in range(spec.levels):
        zf, yf, xf = level_factors(l, xy_levels)
        z_l = ceil_div(spec.z_size_estimate, zf)
        y_l = ceil_div(spec.y, yf)
        x_l = ceil_div(spec.x, xf)
        lvl_shape_zyx = (z_l, y_l, x_l)
        print(f"Level {l} shape (z,y,x): {lvl_shape_zyx}")

        # evolve chunk sizes per your policy
        zc, yc, xc = chunk_scheme.chunks_for_level(l, lvl_shape_zyx)

        # 5D shape/chunks (t=1, c=1)
        shape_5d  = (1, 1, z_l, y_l, x_l)
        chunks_5d = (1, 1, zc,  yc,  xc)

        name = f"s{l}"
        a = root.create_dataset(
            name,
            shape=shape_5d,
            chunks=chunks_5d,
            dtype="uint16",
            compressor=compressor,   # <-- v2 compressor (numcodecs.Blosc)
        )
        a.attrs["dimension_names"] = ["t", "c", "z", "y", "x"]
        arrs.append(a)

    # NGFF-like multiscales metadata for 5D (t,c,z,y,x)
    dz, dy, dx = voxel_size
    datasets = []
    for l in range(spec.levels):
        zf, yf, xf = level_factors(l, xy_levels)
        s = [1.0, 1.0, dz * zf, dy * yf, dx * xf]
        datasets.append({
            "path": f"s{l}",
            "coordinateTransformations": [{"type": "scale", "scale": s}],
        })

    axes = [
        {"name": "t", "type": "time", "unit": "millisecond"},
        {"name": "c", "type": "channel"},
        {"name": "z", "type": "space", "unit": "micrometer"},
        {"name": "y", "type": "space", "unit": "micrometer"},
        {"name": "x", "type": "space", "unit": "micrometer"},
    ]

    root.attrs["multiscales"] = [{
        "axes": axes,
        "datasets": datasets,
        "name": "image",
        "type": "image",
    }]
    root.attrs["version"] = "0.4"
    return root, arrs


class FlushPad(Enum):
    DUPLICATE_LAST = "duplicate_last"  # repeat last plane to fill the chunk
    ZEROS = "zeros"                    # pad with zeros
    DROP = "drop"                      # do not flush the tail (you'll lose last planes)


# ---------- Live writer: true 3D decimation pipeline ----------
class Live3DPyramidWriter:
    """
    Streams true-3D (2x in z,y,x) pyramid while you acquire slices.
    Buffers complete Z-chunks per level and flushes only when full -> no read-modify-write.
    """

    def __init__(self, spec: PyramidSpec, voxel_size=(1.0, 1.0, 1.0), path=STORE_PATH, max_workers=None,
                 chunk_scheme: ChunkScheme = ChunkScheme(), compressor=None,
                 flush_pad: FlushPad = FlushPad.DUPLICATE_LAST,
                 ingest_queue_size: int = 8,
                 max_inflight_chunks: int | None = None,
                 async_close: bool = True,
                 shard_shape: Tuple[int, int, int] | None = None,
                 xy_levels: int | None = None):

        self.spec = spec
        self.chunk_scheme = chunk_scheme
        self.flush_pad = flush_pad
        self.xy_levels =(compute_xy_only_levels(voxel_size)if xy_levels is None
                        else int(xy_levels))
        self.max_workers = max_workers or min(8, os.cpu_count() or 4)
        self.async_close = async_close
        self.finalize_future = None


        self.root, self.arrs = init_h5_nested_store_v2(
            spec, path, chunk_scheme=chunk_scheme, compressor=compressor,
            voxel_size=voxel_size, xy_levels=self.xy_levels,
            shard_shape=shard_shape,  # ignored in v2
        )



        self.levels = spec.levels
        self.z_counts = [0] * self.levels
        self.buffers = [None] * self.levels
        self.buf_fill = [0] * self.levels
        self.buf_start = [0] * self.levels
        self.zc = []
        self.yx_shapes = []

        for l in range(self.levels):
            zf, yf, xf = level_factors(l, self.xy_levels)
            z_l = ceil_div(self.spec.z_size_estimate, zf)
            y_l = ceil_div(self.spec.y, yf)
            x_l = ceil_div(self.spec.x, xf)
            zc, yc, xc = self.chunk_scheme.chunks_for_level(l, (z_l, y_l, x_l))
            self.zc.append(zc)
            self.yx_shapes.append((y_l, x_l))

        # Concurrency primitives
        self.q = queue.Queue(maxsize=ingest_queue_size)
        self.stop = threading.Event()
        self.lock = threading.Lock()  # sequences z-indices & buffers (single writer to RAM)
        self.pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        )
        # Allow at most ~2 chunks per worker in flight (tweak as you like)
        self.max_inflight_chunks = max_inflight_chunks or (self.max_workers * 40)
        self._inflight_sem = threading.Semaphore(self.max_inflight_chunks)

        self.worker = threading.Thread(target=self._consume, daemon=True)
        self.worker.start()

    # ---------- Public API ----------
    def push_slice(self, slice_u16: np.ndarray):
        assert slice_u16.dtype == np.uint16, "slice must be uint16"
        assert slice_u16.shape == (self.spec.y, self.spec.x), \
            f"got {slice_u16.shape}, expected {(self.spec.y, self.spec.x)}"
        self.q.put(slice_u16, block=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.async_close:
            self.close_async()    # Background finalize
        else:
            self.close()          # synchronous finalize
        return False

    def close(self):
        self.stop.set()
        self.q.put(None)
        self.worker.join()

        with self.lock:
            # flush odd Z-pair tails at levels >= 1
            self._flush_pair_tails_all_the_way()

            # Flush any partially filled chunks w/o RMW by padding to full chunk size
            for l in range(self.levels):
                if self.buffers[l] is not None and self.buf_fill[l] > 0:
                    self._pad_and_flush_partial_chunk(l)

        self.pool.shutdown(wait=True)

        for l, a in enumerate(self.arrs):
            if a.ndim == 3:
                a.resize((self.z_counts[l], a.shape[1], a.shape[2]))
            else:  # 5D (t=1, c=1, z, y, x)
                a.resize((1, 1, self.z_counts[l], a.shape[3], a.shape[4]))


    # inside class Live3DPyramidWriter

    def close_async(self):
        """
        Start flushing/closing in a background thread and return a Future.
        You can call future.result() later to wait for completion.
        """
        if getattr(self, "_finalize_future", None) is not None:
            raise RuntimeError("close_async already called")
        self._finalize_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.finalize_future = self._finalize_executor.submit(self._finalize)
        return self.finalize_future

    def _finalize(self):
        # exactly what your current close() does, but moved here
        self.stop.set()
        self.q.put(None)
        self.worker.join()

        with self.lock:
            self._flush_pair_tails_all_the_way()
            for l in range(self.levels):
                if self.buffers[l] is not None and self.buf_fill[l] > 0:
                    self._pad_and_flush_partial_chunk(l)

        # for fut in self.pending_futs:
        #     fut.result()
        self.pool.shutdown(wait=True)

        for l, a in enumerate(self.arrs):
            if a.ndim == 3:
                a.resize((self.z_counts[l], a.shape[1], a.shape[2]))
            else:  # 5D (t=1, c=1, z, y, x)
                a.resize((1, 1, self.z_counts[l], a.shape[3], a.shape[4]))


        # optional: mark completion for external watchers
        try:
            import pathlib
            store_path = getattr(self.root.store, "path", None)
            if store_path:
                pathlib.Path(store_path, ".READY").write_text("ok")
        except Exception:
            pass

    # ---------- Internals ----------

    def _flush_pair_tails_all_the_way(self):
        if not hasattr(self, "_pair_buf"):
            return
        changed = True
        while changed:
            changed = False
            for lvl in range(max(1, self.xy_levels + 1), self.levels):  # start at first 3D level
                buf = self._pair_buf[lvl]
                if buf is None:
                    continue
                if self.flush_pad == FlushPad.DUPLICATE_LAST:
                    tail = dsZ2_mean_uint16(buf, buf)
                elif self.flush_pad == FlushPad.ZEROS:
                    tail = dsZ2_mean_uint16(buf, np.zeros_like(buf, dtype=np.uint16))
                else:  # DROP
                    self._pair_buf[lvl] = None
                    continue
                self._pair_buf[lvl] = None
                zL = self._reserve_z(lvl)
                self._append_to_active_buffer(lvl, zL, tail)
                self._emit_next(lvl + 1, ds2_mean_uint16(tail))
                changed = True

    def _consume(self):
        while True:
            item = self.q.get()
            if item is None:
                break
            self._ingest_raw(item)

    def _reserve_z(self, level: int) -> int:
        z = self.z_counts[level]
        self.z_counts[level] += 1
        return z

    def _submit_write_chunk(self, level: int, z0: int, buf3d: np.ndarray):
        # bound in-flight tasks; acquire before submitting
        self._inflight_sem.acquire()
        fut = self.pool.submit(self._write_chunk_slice, self.arrs[level], z0, buf3d)
        # Release the slot when done (and drop ref to the future immediately)
        fut.add_done_callback(lambda _f: self._inflight_sem.release())

    # @staticmethod
    # def _write_chunk_slice(arr, z0, buf3d):
    #     arr[z0:z0 + buf3d.shape[0], :, :] = buf3d  # contiguous, aligned write

    @staticmethod
    def _write_chunk_slice(arr, z0, buf3d):
        if arr.ndim == 3:
            arr[z0:z0 + buf3d.shape[0], :, :] = buf3d
        elif arr.ndim == 5:
            arr[0, 0, z0:z0 + buf3d.shape[0], :, :] = buf3d
        else:
            raise ValueError(f"Unexpected array ndim {arr.ndim}; expected 3 or 5.")

    def _ensure_active_buffer(self, level: int, start_z: int):
        """Allocate active chunk buffer for a level if absent, starting at start_z."""
        if self.buffers[level] is None:
            zc = self.zc[level]
            y_l, x_l = self.yx_shapes[level]
            self.buffers[level] = np.empty((zc, y_l, x_l), dtype=np.uint16)
            self.buf_fill[level] = 0
            self.buf_start[level] = start_z

    def _append_to_active_buffer(self, level: int, z_index: int, plane: np.ndarray):
        """Append plane into the active buffer; flush when full."""
        zc = self.zc[level]
        if self.buf_fill[level] == 0:
            # Align start to chunk boundary; with strictly increasing z, z_index should already align when new chunk begins
            start_z = (z_index // zc) * zc
            self._ensure_active_buffer(level, start_z)

        offset = z_index - self.buf_start[level]
        self.buffers[level][offset, :, :] = plane
        self.buf_fill[level] += 1

        if self.buf_fill[level] == zc:
            # Full chunk -> flush and reset
            buf = self.buffers[level]
            z0 = self.buf_start[level]
            # hand a copy to the pool to avoid mutation races
            self._submit_write_chunk(level, z0, buf)
            self.buffers[level] = None
            self.buf_fill[level] = 0
            self.buf_start[level] = z0 + zc

    # def _pad_and_flush_partial_chunk(self, level: int):
    #     """Pad the active buffer to full chunk size (duplicate last or zeros) and flush."""
    #     zc = self.zc[level]
    #     fill = self.buf_fill[level]
    #     if fill == 0:
    #         return
    #     buf = self.buffers[level]
    #     if self.flush_pad == FlushPad.DUPLICATE_LAST:
    #         last = buf[fill - 1:fill, :, :]
    #         repeat = np.repeat(last, zc - fill, axis=0)
    #         padded = np.concatenate([buf[:fill], repeat], axis=0)
    #     elif self.flush_pad == FlushPad.ZEROS:
    #         pad = np.zeros((zc - fill, buf.shape[1], buf.shape[2]), dtype=np.uint16)
    #         padded = np.concatenate([buf[:fill], pad], axis=0)
    #     else:  # DROP
    #         # simply discard and roll back z_counts to the start of the partial chunk
    #         self.z_counts[level] = self.buf_start[level]
    #         self.buffers[level] = None
    #         self.buf_fill[level] = 0
    #         return

    #     self._submit_write_chunk(level, self.buf_start[level], padded)
    #     self.buffers[level] = None
    #     self.buf_fill[level] = 0
    #     self.buf_start[level] += zc
    def _pad_and_flush_partial_chunk(self, level: int):
        """Flush the active (partial) Z-chunk by writing only the valid planes."""
        zc = self.zc[level]
        fill = self.buf_fill[level]
        if fill == 0:
            return

        buf = self.buffers[level]
        # Write only the valid 'fill' planes from the active buffer
        # This avoids writing padded data beyond the actual Z extent.
        if self.arrs[level].ndim == 3:
            self.arrs[level][self.buf_start[level]:self.buf_start[level] + fill, :, :] = buf[:fill]
        elif self.arrs[level].ndim == 5:
            self.arrs[level][0, 0, self.buf_start[level]:self.buf_start[level] + fill, :, :] = buf[:fill]
        else:
            raise ValueError(f"Unexpected array ndim {self.arrs[level].ndim}; expected 3 or 5.")

        # reset the active buffer; NOTE we do NOT advance buf_start by zc here,
        # because we only wrote 'fill' planes and z_counts already reflects that
        self.buffers[level] = None
        self.buf_fill[level] = 0
        # do not change self.buf_start[level]

    def _ingest_raw(self, img0: np.ndarray):
        with self.lock:
            # Level 0: write into active chunk
            z0 = self._reserve_z(0)
            self._append_to_active_buffer(0, z0, img0)

            # Build and cascade upper levels (true 3D, factor 2^L)
            if self.levels > 1:
                self._emit_next(level=1, candidate_xy=ds2_mean_uint16(img0))



    def _emit_next(self, level: int, candidate_xy: np.ndarray):
        if level >= self.levels:
            return

        if level <= self.xy_levels:
            # XY-only stage: append every incoming slice (no Z pairing)
            zL = self._reserve_z(level)
            self._append_to_active_buffer(level, zL, candidate_xy)
            # continue XY decimation upward
            self._emit_next(level + 1, ds2_mean_uint16(candidate_xy))
            return

        # 3D stage: pair consecutive planes along Z
        if not hasattr(self, "_pair_buf"):
            self._pair_buf = [None] * self.levels

        buf = self._pair_buf[level]
        if buf is None:
            self._pair_buf[level] = candidate_xy
            return

        out_3d = dsZ2_mean_uint16(buf, candidate_xy)
        self._pair_buf[level] = None

        zL = self._reserve_z(level)
        self._append_to_active_buffer(level, zL, out_3d)

        # propagate upward with further XY decimation
        self._emit_next(level + 1, ds2_mean_uint16(out_3d))


def convert_one(file_path: str, out_dir: str, voxel=(1, 1, 1), xy_levels=0):
    # Keep Blosc from oversubscribing inside each process
    # os.environ.setdefault("BLOSC_NTHREADS", "4")   # tune if needed
    pid = os.getpid()
    print(f"[PID {pid}] start processing {file_path}")

    # --- load (you already know this works) ---
    image = fli_loading(file_path, frames_at_once=3000).compute()
    Z_EST, Y, X = image.shape

    levels = plan_levels(Y, X, Z_EST, xy_levels, min_dim=64)
    spec = PyramidSpec(z_size_estimate=Z_EST, y=Y, x=X, levels=levels)

    # Friendlier L0 Z-chunk (36000 % 32 == 0). Keeps L0 chunk-aligned.
    scheme = ChunkScheme(base=(256, 256, 256), target=(256, 256, 256))

    # Build output path
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (Path(file_path).stem + ".omehans")

    # Writer: keep internal workers modest (we’re already in a process)
    with Live3DPyramidWriter(
        spec,
        voxel_size=voxel,
        path=str(out_path),
        max_workers=max(1, os.cpu_count() // 4),   # tune: small threadpool per proc
        chunk_scheme=scheme,
        compressor=Blosc(cname="zstd", clevel=5, shuffle=True),
        shard_shape=None,
        flush_pad=FlushPad.DUPLICATE_LAST,
        async_close=False,
        xy_levels=xy_levels,
    ) as writer:
        # Stream frames into the writer
        for k in range(Z_EST):
            writer.push_slice(image[k])
    print(f"[PID {pid}] done with {file_path}")
    return str(out_path)

def worker_loop(task_q, success_q,error_q, out_dir, voxel, xy_levels):
    """Each process pulls tasks from the queue until it's empty."""
    while True:
        file_path = task_q.get()
        if file_path is None:
            task_q.task_done() 
            break
        try:
            res = convert_one(file_path, out_dir, voxel, xy_levels)
            success_q.put(res)
        except Exception as e:
            error_q.put((file_path, f"error: {e}"))
        finally:
            task_q.task_done()


def parallel_convert(files, out_dir, voxel=(1,1,1), xy_levels=0,
                     max_queue=10, num_workers=5):
    """
    Run conversion on many files with bounded memory use.

    max_queue = how many files may be "in-flight" (decoded) at once.
    """
    task_q = mp.JoinableQueue(max_queue)
    success_q = mp.Queue()
    error_q = mp.Queue()
    print(f"Starting conversion with {num_workers} workers...")
    # Start workers
    workers = [
        mp.Process(target=worker_loop,
                   args=(task_q, success_q, error_q, out_dir, voxel, xy_levels))
        for _ in range(num_workers)
    ]
    for w in workers:
        # w.daemon = True
        w.start()

    # Feed tasks gradually (producer)
    for f in files:
        task_q.put(f)

    # Send stop signals
    for _ in workers:
        task_q.put(None)

    # Wait for queue to finish
    task_q.join()

    # Collect results
    success = []
    error = []
    while not success_q.empty():
        success.append(success_q.get())
    while not error_q.empty():
        error.append(error_q.get())

    for w in workers:
        w.join()

    return success, error

# def batch_convert(files, out_dir, voxel=(2,1.17,1.04), xy_levels=0, max_procs=None):
#     files = [str(Path(f)) for f in files]
#     out_dir = str(Path(out_dir))

#     # Keep total CPU sane: compression + decode use CPU too
#     # cpu = os.cpu_count()//2
#     # if max_procs is None:
#     #     # Start conservative: 1–2 processes per socket is usually safe
#     #     max_procs = max(1, min(len(files), cpu // 2))
#     print(f"max_procs========>: {max_procs}")
#     results = {}
#     with ProcessPoolExecutor(max_workers=max_procs) as ex:
#         futs = {
#             ex.submit(convert_one, f, out_dir, voxel, xy_levels): f
#             for f in files
#         }
#         for fut in as_completed(futs):
#             src = futs[fut]
#             try:
#                 outp = fut.result()
#                 results[src] = ("ok", outp)
#                 print(f"[OK ] {src} → {outp}")
#             except Exception as e:
#                 results[src] = ("err", repr(e))
#                 print(f"[ERR] {src}: {e}")

#     return results

if __name__ == "__main__":
    # # Example: glob your FLI set
    import glob,argparse
    from pathlib import Path

 
    parser = argparse.ArgumentParser(
        description="Convert HiCAM .fli file(s) to Omehans multiscale pyramid format."
    )

    parser.add_argument("--dir", type=str, default=None,
                        help="Directory containing .fli files to convert.")
    parser.add_argument(
        "--file",
        type=str,
        nargs="+",          # <-- key change: one or more values
        default=None,
        help="One or more .fli files to convert."
    )

    parser.add_argument("--voxel", type=float, nargs=3, required=True,
                        metavar=('Z', 'Y', 'X'),
                        help="Voxel size in microns, e.g. --voxel 2 1.17 1.04")
    parser.add_argument("--output", type=str, required=True,
                        help="Output directory or file path.")
    parser.add_argument("--num_workers", type=int, default=1,
                        help="Number of concurrent worker processes for batch conversion.")

    args = parser.parse_args()
    files_list = []
    # Collect single file (if provided)
    if args.file:
            for f in args.file:
                if os.path.isfile(f):
                    if f.endswith('.fli') or f.endswith('.zst'):
                        files_list.append(f)
                else:
                    print(f"Error: The specified file '{f}' does not exist.")
    if args.dir:
        if os.path.isdir(args.dir):
            # files_raw = glob.glob(os.path.join(args.dir, "*.fli"))
            # files_compressed = glob.glob(os.path.join(args.dir, "*.zst"))
            files_raw = list(Path(args.dir).rglob("*.fli"))
            files_compressed = list(Path(args.dir).rglob("*.zst"))

            files_list.extend([str(p) for p in files_raw])
            files_list.extend([str(p) for p in files_compressed])
        else:
            print(f"Error: The specified directory '{args.dir}' does not exist.")


    voxel_size = tuple(args.voxel)
    out_path = args.output
    num_workers = args.num_workers
    if files_list:
        print(f"Starting batch conversion of {len(files_list)} files...")
        start_time = time.time()
        sucess, error = parallel_convert(files_list, out_path, voxel=voxel_size, xy_levels=0,
                                   max_queue=10, num_workers=num_workers)
        print(f"Successfully converted {len(sucess)} out of {len(files_list)} files.")
        print(f"Errors encountered for {len(error)} files: {error}")
        end_time = time.time()
        print(f"Total time for batch conversion: {end_time - start_time} seconds")
    else:
        print("No files to convert. Please specify a valid directory or file.")





























