#!/bin/bash

# ome.zarr version selection (default 0.5 = sharded zarr v3, .ome.zarr;
# 0.4 = legacy H5-backed .omehans, needs a zarr v2 env with zarr_stores)
# --voxel takes (z y x): z step between planes first, then y and x pixel size
# (x is the stage-scan axis), e.g. 0.528 0.5616 2.0 = z=0.528, y=0.5616, x=2.0
#python fli_conversion.py --dir <dir> --voxel 0.528 0.5616 2.0 --output <out> --ome_version 0.5
#python fli_conversion.py --dir <dir> --voxel 0.528 0.5616 2.0 --output <out> --ome_version 0.4
# optional shard shape (z y x; multiples of the 256-chunk; smaller z = less RAM)

#python fli_conversion.py --dir <dir> --voxel 0.528 0.5616 2.0 --output <out> --shard_shape 256 256 256
# writer backend (default acquire for 0.5 = acquire-zarr C++; 'python' = previous writer)
#python fli_conversion.py --dir <dir> --voxel 0.528 0.5616 2.0 --output <out> --writer python
# note: --ome_version 0.4 always uses the python writer (acquire-zarr is 0.5-only)
# note: re-running over an existing .ome.zarr raises FileExistsError with the acquire backend (the python backend overwrote silently) - remove the old store first
# note: with --num_workers > 1 each worker's acquire-zarr writer uses all visible CPUs - reduce --num_workers on CPU-bound SLURM jobs


# only dir with uncompressed .fli files
#python fli_conversion.py --dir /bil/proj/rf1hillman/2025_09_04_HOLiS_NPBB328_Cortex_Slab7_test_tissueXYZ --voxel 0.528 0.5616 2.0 --output /bil/proj/rf1hillman/fli_conversion/small_uncompressed --num_workers 20
#python fli_conversion.py --dir /bil/proj/rf1hillman/fli_conversion/more_test/uncompressed_input \
# --voxel 0.528 0.5616 2.0 --output  /bil/proj/rf1hillman/fli_conversion/more_test/uncompressed_output --num_workers 10

# only dir with compressed .fli.zst files
python fli_conversion.py --dir /bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns \
 --voxel 0.528 0.5616 2.0 --output  /bil/proj/rf1hillman/fli_conversion/big_compressed/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns --num_workers 10

# mix of dirs and files
# python fli_conversion.py --dir /bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns --file "/bil/proj/rf1hillman/fli_conversion/more_test/uncompressed_input/NPBB328-surface-run028-z01-y27-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-088.fli" --voxel 0.528 0.5616 2.0 --output /h20/Public/holis/test_kelin/generation_test/batch --num_workers 10

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
#    --voxel 0.528 0.5616 2.0 --output /bil/proj/rf1hillman/fli_conversion/big_compressed/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07 --num_workers 10
