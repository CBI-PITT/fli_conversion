"""Write an in-memory (Z, Y, X) NumPy volume to sharded OME-NGFF 0.5 via acquire-zarr.

Backend: acquire-zarr 0.10.x (compiled C++ ZarrStream).
Install:  pip install 'acquire-zarr>=0.10,<0.11' numpy

This is the integrated variant of ``acquire_zarr_numpy_writer.py`` (the
untouched standalone reference): it writes the 5D (t, c, z, y, x) axis
schema used by fli_conversion.py, carries physical voxel scales and units
on the acquire Dimension objects, and writes a .READY completion marker
after a successful close().

Semantics and differences vs the repo's Live3DPyramidWriter:

- MEAN downsampling truncates toward zero (floor for unsigned integers);
  the repo writer rounds half-up. Pyramid levels can therefore differ
  from the repo writer's output by up to 1 intensity step (LSB). Level-0
  pixels are exact.
- Per pyramid level, acquire first takes the 2x2 XY mean of each incoming
  frame (edge-clamped by duplication for odd sizes), then averages
  consecutive XY-reduced frames in z (floor((a+b)/2) pairs). z is only
  decimated while a level's z size exceeds its chunk size.
- Pyramid datasets are named "0", "1", ... (readers must follow the
  metadata dataset paths, never the names).
- acquire-zarr requires at least two dimensions with the last two being
  Space; this writer always emits t (TIME, size 1), c (CHANNEL, size 1),
  then z/y/x (SPACE, known sizes).
- ``num_levels`` is passed to acquire's ``max_levels``, which caps the
  number of ADDITIONAL pyramid levels: the store contains up to
  ``num_levels + 1`` datasets ("0".."num_levels") when the natural
  cascade allows that many. 0 is acquire's "no limit" sentinel: every
  level the natural cascade produces is written - exactly one dataset
  when the array fits inside one chunk on every axis (the convert_one
  tiny-image edge).
- After a successful close(), the store's OME metadata is verified
  (Zarr v3 group, OME-NGFF 0.5) and physical micrometer scales are
  guaranteed on every pyramid level. The primary ``Dimension.scale``/
  ``unit`` path above already writes absolute scales (level L =
  voxel x 2^L), so this is normally a verified NO-OP that never touches
  the file; if the emitted level-0 scale is relative (pixel-based)
  instead, the z/y/x entries of every dataset's 5-value scale vector
  are multiplied by the voxel spacing (t,c stay 1.0), the spatial axes
  get unit "micrometer", and zarr.json is rewritten atomically.

No Python-side downsampling and no full-volume copies: for a C-contiguous
array, ``volume[z0:z1]`` is a view into the existing buffer and at most
one slab is made contiguous per append.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
import time
import warnings

import numpy as np


SUPPORTED_DTYPES = {
    np.dtype(t) for t in (
        np.uint8, np.uint16, np.uint32, np.uint64,
        np.int8, np.int16, np.int32, np.int64,
        np.float32, np.float64,
    )
}


def _default_threads() -> int:
    """Respect SLURM's CPU allocation if provided."""
    n = os.environ.get("SLURM_CPUS_PER_TASK", "")
    if n.isdecimal() and int(n) > 0:
        return int(n)
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def _read_store_metadata(store_path: Path) -> dict:
    """Load the store root zarr.json and verify the OME-NGFF 0.5 group format."""
    meta_path = store_path / "zarr.json"
    with meta_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)
    if meta.get("zarr_format") != 3 or meta.get("node_type") != "group":
        raise RuntimeError(f"Not a Zarr v3 group: {meta_path}")
    ome = meta.get("attributes", {}).get("ome", {})
    if ome.get("version") != "0.5":
        raise RuntimeError(f"Expected OME-NGFF 0.5; got {ome.get('version')!r}")
    return meta


def _ensure_physical_scales(
    store_path: Path, voxel_size_zyx_um: tuple[float, float, float]
) -> None:
    """Guarantee physical (micrometer) scales on every pyramid level.

    A verified NO-OP when the primary ``Dimension.scale``/``unit`` path
    already emitted the physical level-0 scale ([1, 1, *voxel] with deeper
    levels at voxel x 2^L): the file is never touched. Otherwise the
    z/y/x entries (indices 2..4) of every dataset's 5-value scale vector
    are multiplied by the physical voxel spacing (t,c stay 1.0), the
    spatial axes are given unit "micrometer", and zarr.json is rewritten
    atomically (tmp file + os.replace + fsync). Pixel data is unaffected.
    """
    meta = _read_store_metadata(store_path)
    ms = meta["attributes"]["ome"].get("multiscales", [])
    if len(ms) != 1:
        raise RuntimeError("Expected exactly one multiscales entry")
    datasets = ms[0].get("datasets", [])
    if not datasets:
        raise RuntimeError("No multiscale pyramid datasets found")

    def _scale_of(dataset):
        transforms = [
            t for t in dataset.get("coordinateTransformations", [])
            if t.get("type") == "scale"
        ]
        if len(transforms) != 1 or len(transforms[0].get("scale", [])) != 5:
            raise RuntimeError(f"Expected one 5D scale transform: {dataset}")
        return transforms[0]["scale"]

    level0 = [float(v) for v in _scale_of(datasets[0])]
    if all(abs(s - e) <= 1e-9
           for s, e in zip(level0, (1.0, 1.0, *voxel_size_zyx_um))):
        return

    for dataset in datasets:
        s = _scale_of(dataset)
        s[2] = float(s[2]) * voxel_size_zyx_um[0]
        s[3] = float(s[3]) * voxel_size_zyx_um[1]
        s[4] = float(s[4]) * voxel_size_zyx_um[2]

    for axis in ms[0].get("axes", []):
        if axis.get("name") in ("z", "y", "x"):
            axis["unit"] = "micrometer"

    meta_path = store_path / "zarr.json"
    tmp = meta_path.with_name("zarr.json.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, meta_path)
    finally:
        if tmp.exists():
            tmp.unlink()


def write_volume_ome_zarr_acquire(
    volume: np.ndarray,
    output_path: str | Path,
    *,
    voxel_size_zyx_um: tuple[float, float, float],
    chunk_shape_zyx: tuple[int, int, int] = (256, 256, 256),
    shard_shape_zyx: tuple[int, int, int] | None = None,
    num_levels: int,
    num_threads: int | None = None,
    codec: str = "zstd",
    compression_level: int = 5,
    target_slab_mib: float = 256.0,
    slab_depth_z: int | None = None,
    overwrite: bool = False,
    progress_every: int = 10,
) -> Path:
    """Write a resident (Z,Y,X) NumPy volume via acquire-zarr's C++ writer.

    Parameters
    ----------
    volume:
        A 3D NumPy array in (Z,Y,X) order. C-contiguous native-endian
        arrays use view-only slicing without Python-side buffer copies.
    voxel_size_zyx_um:
        Physical (z, y, x) voxel spacing in micrometers; written as the
        Dimension scale of each spatial axis (t uses milliseconds).
    chunk_shape_zyx:
        Chunk dimensions in voxels (z, y, x).
    shard_shape_zyx:
        Shard dimensions in VOXELS; must be a multiple of the chunk on
        every axis. Defaults to (4*chunk_z, chunk_y, chunk_x), matching
        the repo writer's default shard layout.
    num_levels:
        Passed to acquire's ``max_levels``, which caps the number of
        ADDITIONAL pyramid levels; 0 means "no limit" - every level the
        natural cascade produces, exactly one dataset when the array
        fits inside one chunk on every axis (see module docstring).
    target_slab_mib:
        Target size of each input view passed to ``stream.append``. One
        full XY plane is the smallest possible input slab.
    slab_depth_z:
        Optional explicit number of Z slices per append; overrides target.
    progress_every:
        Show progress every N append calls, 0 = silent.
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
            vals = tuple(float(x) for x in seq)
            if any(not math.isfinite(x) or x <= 0 for x in vals):
                raise ValueError(f"{name} must contain positive finite values")
        else:
            vals = tuple(int(x) for x in seq)
            if any(x <= 0 for x in vals) or any(x != y for x, y in zip(vals, seq)):
                raise ValueError(f"{name} must contain positive integers")
        return vals

    voxels = _triple("voxel_size_zyx_um", voxel_size_zyx_um, real=True)
    chunks = _triple("chunk_shape_zyx", chunk_shape_zyx)
    if shard_shape_zyx is None:
        # Default matches the repo writer: 4x the chunk along z, chunk-sized y/x.
        shards = (4 * chunks[0], chunks[1], chunks[2])
    else:
        shards = _triple("shard_shape_zyx", shard_shape_zyx)
    for s, c, axis_name in zip(shards, chunks, "zyx"):
        if s < c or s % c != 0:
            raise ValueError(
                f"Invalid shard shape {shards}: axis '{axis_name}' value "
                f"{s} must be a multiple of the chunk value {c}."
            )
    if codec not in {"lz4", "zstd"}:
        raise ValueError("codec must be 'lz4' or 'zstd'")
    if not 0 <= compression_level <= 9:
        raise ValueError("compression_level must be in the range 0..9")
    if num_levels < 0:
        raise ValueError("num_levels must be >= 0")
    if target_slab_mib <= 0 or not math.isfinite(target_slab_mib):
        raise ValueError("target_slab_mib must be positive and finite")
    if progress_every < 0:
        raise ValueError("progress_every must be >= 0")

    z_total, y_size, x_size = map(int, volume.shape)
    bytes_per_plane = y_size * x_size * volume.dtype.itemsize
    if slab_depth_z is None:
        # Never materialize a big array: these are only view dimensions.
        # Keep at least one plane even if it exceeds the target size.
        slab_depth_z = max(1, int(target_slab_mib * 2**20 // bytes_per_plane))
    elif not isinstance(slab_depth_z, (int, np.integer)) or slab_depth_z < 1:
        raise ValueError("slab_depth_z must be a positive integer or None")
    slab_depth_z = min(z_total, int(slab_depth_z))

    threads = _default_threads() if num_threads is None else int(num_threads)
    if threads <= 0:
        raise ValueError("num_threads must be positive")
    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not volume.flags.c_contiguous:
        warnings.warn(
            "Input is not C-contiguous; each Z slab will be made contiguous "
            "separately (no full-volume copy). This may reduce throughput.",
            stacklevel=2,
        )

    # Import here, after input validation, so a bad input cannot modify output.
    try:
        import acquire_zarr as aqz
    except ImportError as e:
        raise ImportError(
            "acquire-zarr is required for the acquire writer backend; "
            "pip install 'acquire-zarr>=0.10,<0.11'"
        ) from e

    compressor = aqz.CompressionSettings(
        compressor=aqz.Compressor.BLOSC1,
        codec=(aqz.CompressionCodec.BLOSC_LZ4 if codec == "lz4"
               else aqz.CompressionCodec.BLOSC_ZSTD),
        level=int(compression_level),
        shuffle=2,  # bitshuffle; mirrors the repo writer's BloscCodec shuffle
    )
    settings = aqz.StreamSettings(
        store_path=str(output_path),
        overwrite=bool(overwrite),
        max_threads=threads,
        arrays=[aqz.ArraySettings(
            output_key="",
            data_type=volume.dtype.type,
            compression=compressor,
            downsampling_method=aqz.DownsamplingMethod.MEAN,
            max_levels=int(num_levels),
            dimensions=[
                aqz.Dimension(
                    name="t", kind=aqz.DimensionType.TIME,
                    array_size_px=1, chunk_size_px=1, shard_size_chunks=1,
                    unit="millisecond",
                ),
                aqz.Dimension(
                    name="c", kind=aqz.DimensionType.CHANNEL,
                    array_size_px=1, chunk_size_px=1, shard_size_chunks=1,
                ),
                aqz.Dimension(
                    name="z", kind=aqz.DimensionType.SPACE,
                    array_size_px=z_total, chunk_size_px=chunks[0],
                    shard_size_chunks=shards[0] // chunks[0],
                    scale=float(voxels[0]), unit="micrometer",
                ),
                aqz.Dimension(
                    name="y", kind=aqz.DimensionType.SPACE,
                    array_size_px=y_size, chunk_size_px=chunks[1],
                    shard_size_chunks=shards[1] // chunks[1],
                    scale=float(voxels[1]), unit="micrometer",
                ),
                aqz.Dimension(
                    name="x", kind=aqz.DimensionType.SPACE,
                    array_size_px=x_size, chunk_size_px=chunks[2],
                    shard_size_chunks=shards[2] // chunks[2],
                    scale=float(voxels[2]), unit="micrometer",
                ),
            ],
        )],
    )

    raw_gib = volume.nbytes / 2**30
    chunk_mib = math.prod(chunks) * volume.dtype.itemsize / 2**20
    shard_mib = chunk_mib * math.prod(shards)
    print(f"Input: {volume.shape} {volume.dtype}; {raw_gib:.2f} GiB", flush=True)
    print(f"C-contiguous: {volume.flags.c_contiguous}", flush=True)
    print(f"C++ threads: {threads}; Z planes per append: {slab_depth_z}; "
          f"max pyramid levels: {num_levels}", flush=True)
    print(f"Chunks: {chunks} ({chunk_mib:.1f} MiB); shards (voxels): {shards} "
          f"({shard_mib:.1f} MiB uncompressed)", flush=True)

    start = time.perf_counter()
    stream = aqz.ZarrStream(settings)
    try:
        for i, z0 in enumerate(range(0, z_total, slab_depth_z), start=1):
            z1 = min(z_total, z0 + slab_depth_z)
            slab = volume[z0:z1, :, :]  # C-order array => VIEW, no copy
            if not slab.flags.c_contiguous:
                slab = np.ascontiguousarray(slab)  # at most one slab copied
            stream.append(slab)  # compiled C++ handles pyramid/chunks/shards
            if progress_every and (i % progress_every == 0 or z1 == z_total):
                print(f"Appended Z={z1}/{z_total}", flush=True)
    finally:
        # close() completes outstanding jobs and detects failed flushes.
        # On failure it raises RuntimeError: do not treat the store as
        # complete, and never write .READY in that case.
        stream.close()

    # Verify the emitted OME metadata and guarantee physical scales.
    # Normally a NO-OP (the Dimension.scale/unit path above already wrote
    # them). On failure it raises RuntimeError: do not treat the store as
    # complete, and never write .READY in that case.
    _ensure_physical_scales(output_path, voxels)

    # Mark completion for external watchers (only after close() succeeded
    # and the metadata verification passed).
    (output_path / ".READY").write_text("ok")

    dt = time.perf_counter() - start
    print(f"Done: {dt:.1f} s; {raw_gib/dt:.2f} GiB/s raw input equivalent "
          f"(includes compression, pyramid, output I/O)", flush=True)
    return output_path
