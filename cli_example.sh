#!/bin/bash

# only dir with uncompressed .fli files
#python fli_conversion.py --dir /bil/proj/rf1hillman/2025_09_04_HOLiS_NPBB328_Cortex_Slab7_test_tissueXYZ --voxel 2 1.3 0.75 --output /bil/proj/rf1hillman/fli_conversion/small_uncompressed --num_workers 20

# only dir with compressed .fli.zst files
# python fli_conversion.py --dir /bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns --voxel 2 1.3 0.75 --output  /bil/proj/rf1hillman/fli_conversion/big_compressed/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns --num_workers 10

# mix of compressed and uncompressed files
# python fli_conversion.py --dir /h20/Public/holis/test_kelin/test_small_data_uncompressed --file "/h20/Public/holis/test_kelin/test_big_data_compressed/NPBB328-Cortex-Slab01-run041-z01-y040-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-088.fli.zst" --voxel 2 1.17 1.04 --output /h20/Public/holis/test_kelin/generation_test/batch --num_workers 5

# file list
#python fli_conversion.py \
#    --file '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns/NPBB328-Cortex-Slab07-run002-z07-y070-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns/NPBB328-Cortex-Slab07-run003-z13-y051-Exc-488nm-561nm-594nm-660nm_HiCAM FLUO_1875-ST-272.fli.zst'\
#    '/bil/proj/rf1hillman/HOLiS_NPBB328_Cortex/Slab7/2025_09_02_HOLiS_NPBB328_Cortex_Slab07_repeatedRuns/NPBB328-Cortex-Slab07-run001-z07-y000-darkFrames_HiCAM FLUO_1875-ST-272.fli.zst'\
#    --voxel 2 1.3 0.75 --output /bil/proj/rf1hillman/fli_conversion/big_compressed --num_workers 5
