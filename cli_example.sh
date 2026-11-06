#!/bin/bash

# ome.zarr version selection (default 0.5 = sharded zarr v3, .ome.zarr;
# 0.4 = legacy H5-backed .omehans, needs a zarr v2 env with zarr_stores)
#python fli_conversion.py --dir <dir> --voxel 2 1.3 0.75 --output <out> --ome_version 0.5
#python fli_conversion.py --dir <dir> --voxel 2 1.3 0.75 --output <out> --ome_version 0.4
# optional shard shape (z y x; multiples of the 256-chunk; smaller z = less RAM)
#python fli_conversion.py --dir <dir> --voxel 2 1.3 0.75 --output <out> --shard_shape 256 256 256
# writer backend (default acquire for 0.5 = acquire-zarr C++; 'python' = previous writer;
# 'zarrista' = ngff-zarr 0.52 through Rust zarrs, needs ngff-zarr + dask + numcodecs, Python >= 3.11)
#python fli_conversion.py --dir <dir> --voxel 2 1.3 0.75 --output <out> --writer python
#python fli_conversion.py --dir <dir> --voxel 2 1.3 0.75 --output <out> --writer zarrista
# note: --ome_version 0.4 always uses the python writer (acquire-zarr and zarrista are 0.5-only)
# note: re-running over an existing .ome.zarr raises FileExistsError with the acquire and zarrista backends (the python backend overwrote silently) - remove the old store first
# note: with --num_workers > 1 each worker's writer threads are auto-split (visible CPUs / num_workers); override with --writer_threads
# note: zarrista's Dask workers use --writer_threads; its Rust pool size is set separately via RAYON_NUM_THREADS (env, before the first write)
# performance (/h20 IS BeeGFS; measured 8 GiB random uint16, .omo/evidence/benchmark-tuning.md):
#   from pollux / other NFS clients of /h20 the python backend is 2-2.5x faster
#   than acquire-zarr (0.31 vs 0.10-0.14 GiB/s); acquire tunables do not close the
#   gap - prefer --writer python there.
#   from production nodes (native beegfs-client, e.g. deneb02) both backends are
#   equal within noise (0.22-0.24 GiB/s, mount-limited; 4 batch workers reach
#   0.345 agg GiB/s) - either backend is fine; 36 threads marginally beat 9 in
#   batch (~10%).
#   ZARR_DIRECT_IO=1 (env, O_DIRECT): probe the mount first (bench_beegfs_runner.py
#   --probe) - pollux's O_DIRECT path is ~100x slower than buffered, deneb02's is
#   symmetric; NEVER on ext4 (unaligned writes fail with EINVAL).
#   zarrista buffers through the page cache like zarr-python, so measure it with
#   bench_zarrista_beegfs_runner.py (A/B/C vs python + acquire on the same mount).
#   deneb04 (native beegfs-client, 2026-11-05, .omo/evidence/benchmark-zarrista-deneb04.md):
#   python 0.22, acquire 0.21, zarrista best (defaults) 0.18 GiB/s single-process;
#   all ~20-35% of the 0.62 GiB/s buffered ceiling (mount-limited). Zarrista knobs
#   ranked: defaults best (blosc:lz4, 128 MiB shards); big shards worst (~0.13 GiB/s);
#   rayon pin / zstd / cache no gain. Zarrista batch: 9 threads/worker reaches 0.16
#   agg GiB/s and beats 36 (0.13) - keep 9 threads in batch for zarrista; both below
#   deneb02's acquire/python batch 0.345 agg (cross-backend A/B pending) - prefer
#   python/acquire for conversion, --writer zarrista fine with default settings.

# only dir with uncompressed .fli files
#python fli_conversion.py --dir /bil/proj/rf1hillman/2025_09_04_HOLiS_NPBB328_Cortex_Slab7_test_tissueXYZ --voxel 2 1.3 0.75 --output /bil/proj/rf1hillman/fli_conversion/small_uncompressed --num_workers 20
#python fli_conversion.py --dir /bil/proj/rf1hillman/fli_conversion/more_test/uncompressed_input \
# --voxel 2 1.3 0.75 --output  /bil/proj/rf1hillman/fli_conversion/more_test/uncompressed_output --num_workers 10

# only dir with compressed .fli.zst files
python fli_conversion.py --dir /bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns \
 --voxel 2 1.3 0.75 --output  /bil/proj/rf1hillman/fli_conversion/big_compressed/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns --num_workers 10

# mix of dirs and files
# python fli_conversion.py --dir /bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns --file "/bil/proj/rf1hillman/fli_conversion/more_test/uncompressed_input/NPBB328-surface-run028-z01-y27-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-088.fli" --voxel 2 1.17 1.04 --output /h20/Public/holis/test_kelin/generation_test/batch --num_workers 10

# raw file list
#python fli_conversion.py \
#    --file '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run141-z02-y061-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run140-z02-y060-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run136-z02-y056-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run137-z02-y057-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run135-z02-y055-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run139-z02-y059-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run134-z02-y054-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run219-z03-y061-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run215-z03-y057-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07/NPBB328-Cortex-Slab07-run214-z03-y056-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    --voxel 2 1.3 0.75 --output /bil/proj/rf1hillman/fli_conversion/big_compressed/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07 --num_workers 10
