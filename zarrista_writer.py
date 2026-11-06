"""Write an in-memory (Z, Y, X) NumPy volume to sharded OME-NGFF 0.5 via ngff-zarr/zarrista.

Backend: ngff-zarr 0.52.x, which writes through zarrista / Rust zarrs, with
Dask for chunk-level task scheduling and pyramid computation.
Install (Python >= 3.11):  pip install 'ngff-zarr>=0.52,<0.53' 'dask[array]' numcodecs

This is the integrated variant of ``ngff_zarr_zarrista_numpy_writer.py`` (the
untouched standalone reference): it writes the 5D (t, c, z, y, x) axis schema
used by fli_conversion.py, carries physical voxel scales and units on the
ngff-zarr image (no manual JSON edits), and writes a .READY completion marker
after a successful write + validation.

Semantics and differences vs the acquire backend (acquire_zarr_writer.py):

- DASK_BIN_SHRINK downsampling rounds half-up (float64 window mean,
  floor(mean + 0.5); exact for <= 32-bit integers) and trims remainder
  windows for odd sizes. acquire's MEAN truncates toward zero and clamps
  odd edges by duplication; the repo's Live3DPyramidWriter rounds half-up,
  so zarrista matches the repo writer's rounding. Level-0 pixels are exact.
- Per pyramid level, bin_shrink takes ONE window mean per level step
  (2x2x2 for a 2x ladder) from that level's source; acquire first takes a
  2x2 XY mean per frame, then averages consecutive frames in z.
- Every level halves z, y, x (scale factor 2^L relative to level 0),
  regardless of the chunk size - unlike acquire, which only decimates z
  while a level's z size exceeds its chunk size. Levels whose target size
  would floor to zero on any spatial axis are dropped (with a printed
  note), reproducing plan_levels' z_l >= 1 guarantee for small volumes.
- Pyramid datasets are named "scale0/image", "scale1/image", ... (readers
  must follow the metadata dataset paths, never the names).
- ``num_levels`` counts ADDITIONAL pyramid levels, like acquire's
  ``max_levels``: the store contains num_levels + 1 datasets
  ("scale0/image".."scale{num_levels}/image").
- Physical scales are written directly by ngff-zarr (level L = voxel x 2^L
  with t/c at 1.0; spatial units "micrometer", t "millisecond") and are
  verified after the write - normally a NO-OP that never touches the file.
- After a successful write + validation, a .READY marker is written next to
  the store, like the acquire backend.
- ``num_workers`` sets Dask worker threads (schedules chunk-level tasks;
  bin_shrink and codecs run compiled). It does NOT set the native Rust
  pool size: zarrista shares one global Rust (rayon) thread pool per
  process for all of its parallel work. Set RAYON_NUM_THREADS in the
  environment BEFORE the first zarrs call to cap it (see the zarrista_rayon
  variant in bench_zarrista_beegfs_runner.py); unset, it defaults to one
  thread per visible CPU.
- ``cache_intermediate`` False (default) avoids an extra on-disk
  pre-serialization pass for a RAM-resident array; True lets ngff-zarr
  cache intermediate levels on disk to control peak memory.

No Python-side downsampling and no full-volume copies: ``volume[None, None]``
is a zero-copy 5D view and Dask chunks reference the original buffer. Dask
may allocate chunk-sized temporary buffers, and the Rust writer may copy
chunks to ensure contiguous, native-endian input.

Performance notes (see acquire_zarr_writer.py for the /h20 BeeGFS mount
analysis and bench_zarrista_beegfs_runner.py for the A/B harness):

- Like zarr-python, zarrista buffers whole shards through the page cache,
  so the NFS-vs-native-BeeGFS client considerations of the acquire backend
  do not apply the same way: from NFS clients of /h20 this writer is
  expected to track the python backend rather than acquire. Measure with
  the bench script before drawing conclusions on a new mount.

Caution: a failed write leaves a partial store (no .READY marker). Write
to a fresh path and verify before deleting your input data. Do not mutate
the input during write.
"""

from __future__ import annotations

from contextlib import nullcontext
import json
import math
import os
from pathlib import Path
import time
import warnings
from typing import Any, Sequence

import numpy as np


SUPPORTED_DTYPES = {
    np.dtype(t) for t in (
        np.uint8, np.uint16, np.uint32, np.uint64,
        np.int8, np.int16, np.int32, np.int64,
        np.float32, np.float64,
    )
}


def _slurm_workers() -> int:
    env = os.environ.get("SLURM_CPUS_PER_TASK", "")
    if env.isdecimal() and int(env) > 0:
        return int(env)
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def _default_shard_shape(
    chunks: tuple[int, int, int]
) -> tuple[int, int, int]:
    """Default matches the repo writer: 4x the chunk along z, chunk-sized y/x."""
    return (4 * chunks[0], chunks[1], chunks[2])


def check_ome_zarr_v05_sharded_5d(
    output_path: str | Path,
    reference_volume: np.ndarray | None = None,
    *,
    voxel_size_zyx_um: tuple[float, float, float] | None = None,
    check_pixels: bool = True,
) -> list[str]:
    """Lightweight validation of the 5D group, levels, shards and a small L0 read.

    Does not load a full Zarr level into memory. For comprehensive NGFF
    schema validation, install ngff-zarr[validate] and read with validate=True.
    """
    root_path = Path(output_path)
    root = json.loads((root_path / "zarr.json").read_text(encoding="utf8"))
    if root.get("zarr_format") != 3 or root.get("node_type") != "group":
        raise AssertionError("Expected a Zarr v3 group at the output root")
    ome = root.get("attributes", {}).get("ome", {})
    if ome.get("version") != "0.5":
        raise AssertionError(f"Expected OME-Zarr v0.5, got {ome.get('version')!r}")
    multiscales = ome.get("multiscales", [])
    if len(multiscales) != 1:
        raise AssertionError("Expected exactly one OME multiscales entry")
    entry = multiscales[0]
    axes = entry.get("axes", [])
    if tuple(axis.get("name") for axis in axes) != ("t", "c", "z", "y", "x"):
        raise AssertionError(f"Unexpected axes: {axes!r}")
    if tuple(axis.get("type") for axis in axes) != (
            "time", "channel", "space", "space", "space"):
        raise AssertionError(f"Unexpected axis types: {axes!r}")
    units = tuple(axis.get("unit") for axis in axes)
    if units != ("millisecond", None, "micrometer", "micrometer", "micrometer"):
        raise AssertionError(f"Expected t millisecond and z/y/x micrometer units: {units!r}")
    paths = [dataset["path"] for dataset in entry.get("datasets", [])]
    if not paths:
        raise AssertionError("No multiscale datasets found")
    for level, path in enumerate(paths):
        array_path = root_path / path / "zarr.json"
        array_meta = json.loads(array_path.read_text(encoding="utf8"))
        if array_meta.get("zarr_format") != 3 or array_meta.get("node_type") != "array":
            raise AssertionError(f"Not a Zarr v3 array: {path}")
        if not any(codec.get("name") == "sharding_indexed"
                   for codec in array_meta.get("codecs", [])):
            raise AssertionError(f"Missing sharding_indexed codec in {path}")
        dataset = entry["datasets"][level]
        transforms = [t for t in dataset.get("coordinateTransformations", [])
                      if t.get("type") == "scale"]
        if len(transforms) != 1 or len(transforms[0].get("scale", [])) != 5:
            raise AssertionError(f"Expected one 5D scale transform: {path}")
        scale = transforms[0]["scale"]
        if scale[0] != 1.0 or scale[1] != 1.0:
            raise AssertionError(f"Expected t/c scales of 1.0 in {path}: {scale}")
        if voxel_size_zyx_um is not None:
            expected = [v * 2.0**level for v in voxel_size_zyx_um]
            if any(abs(s - e) > 1e-9 * max(1.0, abs(e))
                   for s, e in zip(scale[2:], expected)):
                raise AssertionError(
                    f"Level {level} spatial scales {scale[2:]} != voxel x 2^{level} "
                    f"{expected}")
    if check_pixels and reference_volume is not None:
        import ngff_zarr as nz
        level0 = nz.open_array(str(root_path), paths[0])
        slices = tuple(slice(0, min(n, 8)) for n in reference_volume.shape)
        result = np.asarray(level0[(slice(None), slice(None)) + slices])
        expected = reference_volume[slices]
        np.testing.assert_array_equal(result[0, 0], expected)
    return paths


def _effective_num_levels(
    volume_shape: tuple[int, int, int], num_levels: int
) -> int:
    """Drop trailing levels whose target size floors to zero on any spatial axis.

    plan_levels guards ceil-based level sizes; ngff-zarr floors
    (int(size / factor)), so e.g. z=3 at 4x would produce an empty z dim and
    crash the pyramid. Mirrors plan_levels' z_l >= 1 spirit; the caller's
    num_levels remains the upper bound.
    """
    z, y, x = (int(s) for s in volume_shape)
    for level in range(num_levels, 0, -1):
        factor = 2**level
        if min(z // factor, y // factor, x // factor) >= 1:
            return level
        print(f"Note: dropping pyramid level {level} (target size 0 on some "
              f"spatial axis of {volume_shape})", flush=True)
    return 0


def write_volume_ome_zarr_zarrista(
    volume: np.ndarray,
    output_path: str | Path,
    *,
    voxel_size_zyx_um: tuple[float, float, float],
    chunk_shape_zyx: tuple[int, int, int] = (256, 256, 256),
    shard_shape_zyx: tuple[int, int, int] | None = None,
    num_levels: int,
    num_workers: int | None = None,
    downsample_method: str = "bin_shrink",
    codec: str = "blosc:lz4",
    compression_level: int = 1,
    cache_intermediate: bool = False,
    overwrite: bool = False,
    validate_output: bool = True,
    show_progress: bool = True,
) -> Path:
    """Write a resident (Z,Y,X) NumPy volume via ngff-zarr's zarrista writer.

    Parameters
    ----------
    volume:
        A 3D NumPy array in (Z,Y,X) order, ideally native-endian and
        C-contiguous. Referenced in place through a zero-copy 5D view; no
        whole-volume ascontiguousarray() operation is used.
    voxel_size_zyx_um:
        Physical (z, y, x) voxel spacing in micrometers; written into the
        OME-NGFF metadata by ngff-zarr (t uses milliseconds, c has no unit).
    chunk_shape_zyx:
        Inner (read) chunk dimensions in voxels (z, y, x).
    shard_shape_zyx:
        Shard dimensions in VOXELS; must be a multiple of the chunk on
        every axis. Defaults to (4*chunk_z, chunk_y, chunk_x), matching
        the repo writer's default shard layout.
    num_levels:
        Number of ADDITIONAL pyramid levels (like acquire's max_levels):
        the store contains num_levels + 1 datasets, each halving z, y, x
        relative to level 0. Levels whose target size would floor to zero
        on any spatial axis are dropped (with a printed note). 0 writes
        level 0 only.
    num_workers:
        Dask worker threads. None (default) uses all visible CPUs
        (SLURM_CPUS_PER_TASK if set); ``parallel_convert`` passes an
        explicit per-worker split so batch workers do not oversubscribe.
        Does not set the native Rust pool size: set RAYON_NUM_THREADS in
        the environment before the first zarrs call for that.
    downsample_method:
        'bin_shrink' (local-mean, fast) or 'gaussian' (reduces aliasing,
        slower ITKWasm path).
    codec:
        'blosc:lz4' (fast lossless default), 'blosc:zstd', 'zstd', 'gzip'
        or 'none'.
    compression_level:
        Codec level 0..9 (blosc:lz4 default 1, zstd-style codecs 5).
    cache_intermediate:
        If True, permit ngff-zarr to cache intermediate levels on disk to
        control memory use. False avoids extra disk work with in-RAM input
        but can increase peak working memory for extremely large arrays.
    overwrite:
        Permit destructive replacement of an existing output path.
    validate_output:
        Verify the store's OME metadata (Zarr v3 group, OME-NGFF 0.5, 5D
        axes/units, physical per-level scales, sharding codecs) and a small
        level-0 pixel read after the write.
    show_progress:
        Show a Dask progress bar around the write.
    """
    if not isinstance(volume, np.ndarray):
        raise TypeError("volume must be an existing NumPy ndarray")
    if volume.ndim != 3 or any(int(v) < 1 for v in volume.shape):
        raise ValueError(f"Expected nonempty (Z,Y,X) volume; got {volume.shape}")
    if volume.dtype not in SUPPORTED_DTYPES or not volume.dtype.isnative:
        raise TypeError(
            f"Unsupported or non-native-endian dtype {volume.dtype}; "
            "convert explicitly before writing"
        )

    def _triple(name: str, seq, *, real: bool = False):
        if len(seq) != 3:
            raise ValueError(f"{name} must have three Z,Y,X entries")
        if real:
            vals = tuple(float(v) for v in seq)
            if any(not math.isfinite(v) or v <= 0 for v in vals):
                raise ValueError(f"{name} must contain positive finite values")
        else:
            vals = tuple(int(v) for v in seq)
            if any(v <= 0 for v in vals) or any(v != s for v, s in zip(vals, seq)):
                raise ValueError(f"{name} must contain positive integers")
        return vals

    voxels = _triple("voxel_size_zyx_um", voxel_size_zyx_um, real=True)
    chunks = _triple("chunk_shape_zyx", chunk_shape_zyx)
    if shard_shape_zyx is None:
        shards = _default_shard_shape(chunks)
    else:
        shards = _triple("shard_shape_zyx", shard_shape_zyx)
    for s, c, axis_name in zip(shards, chunks, "zyx"):
        if s < c or s % c != 0:
            raise ValueError(
                f"Invalid shard shape {shards}: axis '{axis_name}' value "
                f"{s} must be a multiple of the chunk value {c}."
            )
    if num_levels < 0:
        raise ValueError("num_levels must be >= 0")
    if downsample_method not in ("bin_shrink", "gaussian"):
        raise ValueError("downsample_method must be 'bin_shrink' or 'gaussian'")
    if codec not in {"blosc:lz4", "blosc:zstd", "zstd", "gzip", "none"}:
        raise ValueError(f"Unknown codec {codec!r}; use 'blosc:lz4', 'blosc:zstd', "
                         "'zstd', 'gzip' or 'none'")
    if not isinstance(compression_level, int) or not 0 <= compression_level <= 9:
        raise ValueError("compression_level must be an integer between 0 and 9")
    if not isinstance(cache_intermediate, bool):
        raise TypeError("cache_intermediate must be a bool")
    workers = _slurm_workers() if num_workers is None else int(num_workers)
    if workers <= 0:
        raise ValueError("num_workers must be positive")
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_path}")
    if output_path.suffix != ".zarr":
        warnings.warn("Output is conventionally named *.ome.zarr", stacklevel=2)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not volume.flags.c_contiguous:
        warnings.warn(
            "Input is not C-contiguous; the Rust writer may create contiguous "
            "copies of individual blocks. This may reduce throughput.",
            stacklevel=2,
        )

    # Imports are deferred until after validation so invalid parameters do
    # not create or alter output paths.
    try:
        import dask
        import dask.array as da
        from dask.diagnostics import ProgressBar
        import ngff_zarr as nz
        from ngff_zarr.codecs import codec_from_name
    except ImportError as e:
        raise ImportError(
            "ngff-zarr and dask are required for the zarrista writer backend; "
            "pip install 'ngff-zarr>=0.52,<0.53' 'dask[array]' numcodecs"
        ) from e

    shard_counts = tuple(s // c for s, c in zip(shards, chunks))
    levels = _effective_num_levels(tuple(int(s) for s in volume.shape), int(num_levels))

    # Dask references the original ndarray through a zero-copy 5D view; a
    # random name (name=False) avoids hashing the volume buffer for the
    # task name.
    lazy5d = da.from_array(volume[None, None], chunks=(1, 1, *chunks),
                           asarray=False, name=False)
    image = nz.to_ngff_image(
        lazy5d,
        dims=("t", "c", "z", "y", "x"),
        scale={"t": 1.0, "c": 1.0, "z": voxels[0], "y": voxels[1], "x": voxels[2]},
        axes_units={"t": "millisecond", "z": "micrometer",
                    "y": "micrometer", "x": "micrometer"},
        name="image",
    )
    method = (nz.Methods.DASK_BIN_SHRINK if downsample_method == "bin_shrink"
              else nz.Methods.ITKWASM_GAUSSIAN)
    # Per-axis dict factors are required for the 5D schema: absolute int
    # factors decimate the size-1 t/c dims to zero. Each level's factor is
    # absolute relative to level 0, so spatial scales come out as
    # voxel x 2^L and t/c stay at 1.0.
    scale_factors = [{"z": 2**k, "y": 2**k, "x": 2**k} for k in range(1, levels + 1)]
    # For a RAM-resident source, avoid an extra disk cache serialization.
    multiscales = nz.to_multiscales(
        image,
        scale_factors=scale_factors,
        method=method,
        chunks=(1, 1, *chunks),
        cache=cache_intermediate,
    )
    compressor = codec_from_name(codec, level=compression_level)

    raw_gib = volume.nbytes / 2**30
    chunk_mib = math.prod(chunks) * volume.dtype.itemsize / 2**20
    shard_mib = chunk_mib * math.prod(shard_counts)
    print(f"Input: {volume.shape} {volume.dtype} ({raw_gib:.2f} GiB)", flush=True)
    print(f"C-contiguous: {volume.flags.c_contiguous}", flush=True)
    print(f"Dask workers: {workers}; pyramid levels: {levels + 1}", flush=True)
    print(f"Chunks: {chunks} ({chunk_mib:.1f} MiB raw); shards (voxels): {shards} "
          f"({shard_mib:.1f} MiB raw); codec: {codec}; method: {downsample_method}",
          flush=True)
    rayon = os.environ.get("RAYON_NUM_THREADS")
    if rayon:
        print(f"RAYON_NUM_THREADS={rayon} (Rust pool)", flush=True)

    started = time.perf_counter()
    # ngff-zarr's zarrista path writes one aligned shard per task, avoiding
    # concurrent writes to the same shard. Dask threads schedule tasks;
    # zarrista and codecs use compiled Rust/native implementations.
    with dask.config.set(scheduler="threads", num_workers=workers):
        progress_ctx = ProgressBar() if show_progress else nullcontext()
        with progress_ctx:
            nz.to_ome_zarr(
                str(output_path), multiscales,
                version="0.5", overwrite=overwrite,
                chunks_per_shard=(1, 1, *shard_counts),
                compressors=([compressor] if compressor is not None else []),
                scale_strategy="exact",  # faithful physical scale metadata
            )
    elapsed = time.perf_counter() - started

    # Verify the emitted OME metadata and physical scales. ngff-zarr writes
    # them directly, so this is a verified NO-OP that never touches the file.
    paths = None
    if validate_output:
        paths = check_ome_zarr_v05_sharded_5d(
            output_path, volume, voxel_size_zyx_um=voxels)
        print(f"Verified {len(paths)} sharded Zarr v3 scale arrays and L0 pixels",
              flush=True)

    # Mark completion for external watchers (only after the write and the
    # metadata validation passed).
    (output_path / ".READY").write_text("ok")

    print(f"Done: {elapsed:.1f} s; {raw_gib / max(elapsed, 1e-9):.2f} GiB/s raw "
          f"input equivalent (includes pyramid, compression, output I/O)",
          flush=True)
    return output_path


if __name__ == "__main__":
    rng = np.random.default_rng(42)
    test_array = rng.integers(0, 4096, size=(64, 256, 256), dtype=np.uint16)
    write_volume_ome_zarr_zarrista(
        test_array,
        "test_zarrista_v05.ome.zarr",
        voxel_size_zyx_um=(4.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 64, 64),
        shard_shape_zyx=(8, 64, 64),
        num_levels=2,
        num_workers=4,
        overwrite=False,
    )
