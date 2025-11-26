import os
from sys import exception
import time
import zstandard as zstd
import numpy as np
import dask.array as da
from dask import delayed
from ast import literal_eval
header_info = {
    # '{FLIMIMAGE}'
    # [INFO]
    'version': None,
    'compression': None,
    # '[LAYOUT]'
    'timeStamp': None,
    'CaptureVersion': None,
    'datatype': None,  # Should be UINT12
    'channels': None,
    'x': None,  # X axis pixel dimensions
    'y': None,  # Y axis pixel dimensions
    'z': None,
    'phases': None,
    'frequencies': None,
    'frameRate': None,
    'exposureTime': None,
    'deviceName': None,
    'deviceSerial': None,
    'deviceAlias': None,
    'packing': None,
    'hasDarkImage': None,
    'lutPath': None,
    'timestamps': None,  # Number of frames collected, sometimes does not exist
    # '[DEVICE SETTINGS]'
    'Intensifier_PowerSwitch': None,
    'Intensifier_MCPvoltage': None,
    'Intensifier_MinimumMCPvoltage': None,
    'Intensifier_MaximumMCPvoltage': None,
    'Intensifier_SpecialCharacters': None,
    'Intensifier_AnodeCurrentLevelMicroAmps': None,
    'Intensifier_AnodeCurrentShutdownLevelMicroAmps': None,
    'Intensifier_AnodeCurrentProtectionSwitch': None,
    'Intensifier_UseCustomShutdownAnodeCurrentLevel': None,
    'Intensifier_CloseGateIfCameraIdleAvailable': None,
    'Intensifier_TECtargetTemperature': None,
    'Intensifier_TECheatsinkTemp': None,
    'Intensifier_TECobjectTemp': None,
    'Intensifier_GateOpenSwitch': None,
    'Intensifier_GateOpenTimeSeconds': None,
    'Intensifier_GateDelayTimeSeconds': None,
    'Intensifier_OutputAopenSwitch': None,
    'Intensifier_OutputAopenTimeSeconds': None,
    'Intensifier_OutputAdelayTimeSeconds': None,
    'Intensifier_OutputBopenSwitch': None,
    'Intensifier_OutputBopenTimeSeconds': None,
    'Intensifier_OutputBdelayTimeSeconds': None,
    'Intensifier_SyncMode': None,
    'Intensifier_GatingFixedFrequencyHz': None,
    'Intensifier_OutputAisPolarityPositive': None,
    'Intensifier_GateLoopModeSwitch': None,
    'Intensifier_BurstModeSwitch': None,
    'Intensifier_NumberOfBursts': None,
    'Intensifier_MultipleExposureSwitch': None,
    'Intensifier_NumberOfMultipleExposurePulses': None,
    'Intensifier_GateEnableInputSwitch': None,
    # 'headerLength': header_length,  # Index of last entry in the header
}


def is_compressed_fli(file_name):
    # Extract the first extension (.zst)
    file_name, ext1 = os.path.splitext(file_name)

    if ext1.lower() == '.fli':
        return False

    # Extract the second extension (.fli) from the remaining filename
    _, ext2 = os.path.splitext(file_name)

    assert ext2.lower() == '.fli', 'File does not appear to be a compressed fli.'
    return ext1.lower() == '.zst'
class FliOpen:
    """
    A context manager for opening files, handling .fli.zstd compression automatically.

    If the file ends with '.zstd', it returns a zstandard decompressor stream reader.
    Otherwise, it returns a standard file object from open().
    """

    def __init__(self, file_name):
        self.file_name = file_name
        self.compressed = is_compressed_fli(file_name)
        #self.size = get_len_fli(file_name)
        self._f_in = None
        self._file_obj = None

    def __enter__(self):
        # Open the file in binary mode for both compressed and uncompressed files
        # because zstandard works with binary streams.
        # The mode is adjusted inside the decompression logic.
        self._f_in = open(self.file_name, 'rb')

        # Check if the file has a .zstd extension
        if self.compressed:
            dctx = zstd.ZstdDecompressor()
            # Wrap the binary file handle in a zstandard stream reader context manager.
            # This is also a context manager, so we store the object it returns.
            self._file_obj = dctx.stream_reader(self._f_in)
            return self._file_obj
        else:
            # For non-zstd files, use the regular open() file handle.
            self._file_obj = self._f_in
            return self._file_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Ensure all open file handles are properly closed upon exiting the context.
        if self._file_obj:
            self._file_obj.close()
        if self._f_in:
            self._f_in.close()
        return False  # Propagate any exceptions



def read_header(file_name):
    '''
    Given a file name, read hicam header and extract parameters into a dictionary.  Make an effort to coerce
    values into appropriate types.

    Return dictionary

    Dictionary includes an extra key 'headerLength'.  All data after this index is image frame data.
    '''
    read_n = 0
    fileinfo = b''
    with FliOpen(file_name) as f:
        while read_n < 40:
            a = f.read(1000)
            fileinfo += a
            if b'{END}' in fileinfo:
                header_length = fileinfo.index(b'{END}') + 5
                # location = str(fileinfo).index('{END}')
                # print(f'Found the end of header at location {header_length}')

                # Convert fileinfo to a string and trim b' and remove anything after {END}
                header_string_end = str(fileinfo).index('{END}') + 5
                fileinfo = str(fileinfo)[2:header_string_end]
                break
            read_n += 1
        if read_n == 40:
            raise KeyError('Error while reading HICAM Header, Header Length too long?')

    raw_header_string = fileinfo
    # Extract header info
    fileinfo = fileinfo.split('\\n')
    # for idx, ii in enumerate(fileinfo):
    #     print(ii)

    for ii in fileinfo:
        for key in header_info:
            test = key.lower() + ' = '
            if ii.lower().startswith(test):
                header_info[key] = ii[len(test)::]

    # Automatically convert values to appropriate types
    for key, value in header_info.items():
        try:
            header_info[key] = literal_eval(value)
        except Exception:
            pass
    header_info['headerLength'] = header_length
    # print(header_info)
    return header_info, raw_header_string

def get_len_fli(file_name):
    compressed = is_compressed_fli(file_name)
    if not compressed:
        return os.path.getsize(file_name)
    else:
        with open(file_name, 'rb') as f:
            header_size = 18
            header_data = f.read(header_size)

            # Parse the header to get frame parameters
            frame_params = zstd.get_frame_parameters(header_data)

            # The content_size attribute holds the decompressed size
            # A value of -1 means the size is not stored in the header
            if frame_params.content_size >= 0:
                return frame_params.content_size

    return None

def get_number_of_frames(file_name, header_info=None):

    if header_info is None:
        header_info, _ = read_header(file_name)

    pixelInFrame_bit8 = int(header_info['x'] * header_info['y'] / 2 * 3)  # Number of bits in frame

    how_many_frames = header_info['timestamps']
    if how_many_frames is None:
        size_of_file = get_len_fli(file_name)
        header_len = header_info['headerLength']
        data_size = size_of_file - header_len
        num_frames_remainder = data_size % pixelInFrame_bit8
        assert num_frames_remainder == 0, 'The length of the spool file does not fit an integer number of frames'
        how_many_frames = data_size // pixelInFrame_bit8

    return how_many_frames

def get_frame_shape(file_name, header_info=None):

    if header_info is None:
        header_info, _ = read_header(file_name)

    return (header_info['y'], header_info['x'])

def get_start_stop_reads_for_frame_groups(file_name, header_info=None, frames_at_once=1024):

    if header_info is None:
        header_info, _ = read_header(file_name)

    pixelInFrame_bit8 = int(header_info['x'] * header_info['y'] / 2 * 3)  # Number of bits in frame

    # how_many_frames = header_info['timestamps']
    size_of_file = get_len_fli(file_name)
    header_len = header_info['headerLength']
    data_size = size_of_file - header_len
    # if how_many_frames is None:
    #     num_frames_remainder = data_size % pixelInFrame_bit8
    #     assert num_frames_remainder == 0, 'The length of the spool file does not fit an integer number of frames'
    #     how_many_frames = data_size // pixelInFrame_bit8


    read_len = frames_at_once * pixelInFrame_bit8
    remaining = data_size
    start = header_len
    idx = 0
    while remaining > 0:
        if remaining - read_len < 0:
            stop = start + remaining
            remaining = 0
        else:
            stop = start + read_len
            remaining -= read_len

        length = stop-start
        yield {'start':start,
               'stop':stop,
               'group':idx,
               'len':length,
               'file':file_name,
               'frames':length//pixelInFrame_bit8,
               'pixelInFrame_bit8':pixelInFrame_bit8,
               'last':remaining==0,
               'frames_at_once':frames_at_once}
        start = stop
        idx += 1

def read_uint12_c(data_chunk):
    data = np.frombuffer(data_chunk, dtype=np.uint8)
    fst_uint8, mid_uint8, lst_uint8 = np.reshape(data, (data.shape[0] // 3, 3)).astype(np.uint16).T
    fst_uint12 = ((mid_uint8 & 0x0F) << 8) | fst_uint8
    snd_uint12 = (lst_uint8 << 4) | ((mid_uint8 & 0xF0) >> 4)
    array = np.reshape(np.concatenate((fst_uint12[:, None], snd_uint12[:, None]), axis=1), 2 * fst_uint12.shape[0])
    return array


def read_uint12(data_chunk, coerce_to_uint16_values=True):
    '''
    Since numpy does not understand uint12 data, this function takes a raw bytes object and reads the 12bit integer
    data into a uint16 array.

    Input:
        data_chunk: Byte string
        coerce_to_uint16_values (bool): if True outputs an array with values that are scaled to uint16

        In general this should remain True.  Thus, the image is representative of a conversion to UINT16 precision
        and any conversion to other precision images (ie float for processing) will appropriately represent the
        original data. *Manual conversion to the original uint12 values can be obtained by division by 16

    Output:
         uint16 numpy array where each integer corresponds to the uint12 value (default)

    '''

    array = read_uint12_c(data_chunk)
    if coerce_to_uint16_values:
        array *= 16
    return array

def fli_loading(
    hicam_file,
    frames_at_once=1000,                # if None -> LCM of all fz (keeps tasks independent)
):
    start_time = time.time()
    header_dict, raw_header = read_header(hicam_file)
    Z = get_number_of_frames(hicam_file, header_info=header_dict)
    Y, X = get_frame_shape(hicam_file, header_info=header_dict)

    print(f'File dimensions: Z={Z}, Y={Y}, X={X}')

    # ---- Build Dask source A (z,y,x) from full IO groups only ----
    locs = []
    for loc in get_start_stop_reads_for_frame_groups(
        hicam_file, header_info=None, frames_at_once=frames_at_once
    ):
        # print(loc)
        if loc['frames'] != frames_at_once:
            if loc['frames'] == Z % frames_at_once + 1:
                if loc['frames'] != 1:
                    print(f"Adjusting frame group at the end: {loc}")
                    adjust_value = (1 * Y * X * 12)//8
                    loc['frames'] = loc['frames'] - 1
                    loc['len'] = loc['len'] - adjust_value
                    loc['stop'] = loc['stop'] - adjust_value
                    print(f"Adjusted to read {loc['frames']} frames for last full group: {loc}")
                    locs.append(loc)
                else:
                    print(f"Skipping last incomplete frame group: {loc}")
            else:
                raise Exception("Unhandled frame groups.")
        else:
            locs.append(loc)
        # if len(locs) == full_groups:
        #     break

    def _read_group(loc):
        data_read_start = time.time()
        print(f'data read/decode/convert_uint16/reshape in process for group {loc["group"]}...')
        with FliOpen(hicam_file) as f:
            f.seek(loc['start'])
            raw = f.read(loc['len'])
        n0 = loc['frames']  # == frames_at_once
        pix_bytes = loc['pixelInFrame_bit8']
        # out = np.zeros((n0, Y, X), dtype=np.uint16)
        # for i in range(n0):
        #     s = i * pix_bytes
        #     canvas = read_uint12(raw[s:s+pix_bytes], coerce_to_uint16_values=True)
        #     out[i] = canvas.reshape((Y, X))
        arr12 = read_uint12(raw, coerce_to_uint16_values=True)

        # 2) Reshape directly to (n0, Y, X)
        out = arr12.reshape(n0, Y, X)
        data_read_end = time.time()
        print(f'Data read and decode/convert_uint16/reshape for group {loc["group"]} took {data_read_end - data_read_start:.2f} seconds.')
        return out

    delayed_blocks = [delayed(_read_group)(loc) for loc in locs]
    blocks = [da.from_delayed(db, shape=(frames_at_once, Y, X), dtype=np.uint16) for db in delayed_blocks]
    A = da.concatenate(blocks, axis=0)  # (use_frames, Y, X)
    # A.compute()
    return A