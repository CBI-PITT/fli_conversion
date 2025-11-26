#!/bin/bash

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
