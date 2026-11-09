"""Tests for the sharded zarr v3 (OME-NGFF 0.5) output path of fli_conversion.

Run with a zarr v3 environment (zarr>=3.1):

    python test_zarr3_writer.py

Plain-assert script (no pytest dependency). Exits non-zero on failure.
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import zarr

sys.path.insert(0, str(Path(__file__).parent))

from fli_conversion import (  # noqa: E402
    ChunkScheme,
    FlushPad,
    Live3DPyramidWriter,
    PyramidSpec,
    convert_one,
    ds2_mean_uint16,
    dsZ2_mean_uint16,
    init_sharded_zarr_v3,
    level_factors,
    plan_levels,
    plan_x_only_levels,
)

TMP = Path(tempfile.mkdtemp(prefix="fli_zarr3_test_"))
rng = np.random.default_rng(42)

_passed = 0


def check(name, fn):
    global _passed
    fn()
    _passed += 1
    print(f"PASS  {name}")


def load_json(path):
    return json.loads(Path(path).read_text())


# ---------------------------------------------------------------------------
# Expected pyramid cascade (mirrors Live3DPyramidWriter._emit_next)
# ---------------------------------------------------------------------------
def expected_level1(inp):
    out = []
    l = 0
    while 2 * l + 1 < inp.shape[0]:
        out.append(dsZ2_mean_uint16(ds2_mean_uint16(inp[2 * l]),
                                    ds2_mean_uint16(inp[2 * l + 1])))
        l += 1
    if 2 * l < inp.shape[0]:  # odd tail -> DUPLICATE_LAST
        last = ds2_mean_uint16(inp[2 * l])
        out.append(dsZ2_mean_uint16(last, last))
    return np.stack(out)


def expected_x_only(inp):
    """Independent X-pair mean reference, preserving every Z/Y position."""
    z, y, x = inp.shape
    out = np.empty((z, y, (x + 1) // 2), dtype=np.uint16)
    for j in range(x // 2):
        out[:, :, j] = ((inp[:, :, 2 * j].astype(np.uint32)
                         + inp[:, :, 2 * j + 1].astype(np.uint32) + 1) // 2)
    if x & 1:
        out[:, :, -1] = out[:, :, -2] if x > 1 else inp[:, :, 0]
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_store_format():
    """init_sharded_zarr_v3 produces a sharded zarr v3 store with OME 0.5 metadata."""
    path = TMP / "fmt.ome.zarr"
    spec = PyramidSpec(z_size_estimate=300, y=512, x=512, levels=2)
    scheme = ChunkScheme(base=(64, 64, 64), target=(64, 64, 64))
    root, arrs = init_sharded_zarr_v3(spec, str(path), chunk_scheme=scheme,
                                      voxel_size=(2.0, 1.0, 1.0), xy_levels=0)
    assert len(arrs) == 2

    rj = load_json(path / "zarr.json")
    assert rj["zarr_format"] == 3, rj["zarr_format"]
    assert rj["node_type"] == "group"
    ome = rj["attributes"]["ome"]
    assert ome["version"] == "0.5"
    ms = ome["multiscales"][0]
    assert [a["name"] for a in ms["axes"]] == ["t", "c", "z", "y", "x"]
    assert [d["path"] for d in ms["datasets"]] == ["s0", "s1"]
    for d, s in zip(ms["datasets"], ([1.0, 1.0, 2.0, 1.0, 1.0],
                                     [1.0, 1.0, 4.0, 2.0, 2.0])):
        tf = d["coordinateTransformations"][0]
        assert tf["type"] == "scale"
        assert tf["scale"] == s

    s0 = load_json(path / "s0" / "zarr.json")
    assert s0["zarr_format"] == 3
    assert s0["node_type"] == "array"
    assert s0["shape"] == [1, 1, 300, 512, 512]
    # for sharded arrays the regular chunk_grid unit IS the shard;
    # default shard: 4x chunk in z, chunk-sized y/x, clipped to the array extent
    assert s0["chunk_grid"]["configuration"]["chunk_shape"] == [1, 1, 256, 64, 64]
    shard_codec = s0["codecs"][0]["configuration"]
    assert s0["codecs"][0]["name"] == "sharding_indexed"
    assert shard_codec["chunk_shape"] == [1, 1, 64, 64, 64], shard_codec["chunk_shape"]
    blosc = shard_codec["codecs"][1]
    assert blosc["name"] == "blosc"
    assert blosc["configuration"]["cname"] == "zstd"
    assert blosc["configuration"]["clevel"] == 5
    assert blosc["configuration"]["shuffle"] == "bitshuffle"
    assert s0["dimension_names"] == ["t", "c", "z", "y", "x"]
    assert s0["data_type"] == "uint16"
    assert s0["fill_value"] == 0

    # deep level: shard z clipped to the level extent (z_l=150 -> 192)
    s1 = load_json(path / "s1" / "zarr.json")
    assert s1["chunk_grid"]["configuration"]["chunk_shape"] == [1, 1, 192, 64, 64]


def test_shard_validation():
    """User-provided shards must be a multiple of the chunk on every axis."""
    path = TMP / "bad.ome.zarr"
    spec = PyramidSpec(z_size_estimate=300, y=512, x=512, levels=1)
    scheme = ChunkScheme(base=(64, 64, 64), target=(64, 64, 64))
    for bad in ((100, 64, 64), (32, 64, 64), (64, 100, 64)):
        try:
            init_sharded_zarr_v3(spec, str(path), chunk_scheme=scheme,
                                 shard_shape=bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"shard_shape={bad} should raise ValueError")


def test_shard_clipping_small_level():
    """A level smaller than one shard gets a clipped shard (multiple of chunk)."""
    path = TMP / "small.ome.zarr"
    spec = PyramidSpec(z_size_estimate=38, y=64, x=64, levels=1)
    scheme = ChunkScheme(base=(64, 64, 64), target=(64, 64, 64))
    init_sharded_zarr_v3(spec, str(path), chunk_scheme=scheme)
    s0 = load_json(path / "s0" / "zarr.json")
    # chunks clamped to (38, 64, 64); shard z clipped to 38 (one chunk)
    assert s0["chunk_grid"]["configuration"]["chunk_shape"] == [1, 1, 38, 64, 64]
    assert s0["codecs"][0]["configuration"]["chunk_shape"] == [1, 1, 38, 64, 64]


def test_writer_roundtrip():
    """Full writer flow: sharded store, correct data at every level, resize no-op."""
    path = TMP / "rt.ome.zarr"
    z_size, y_size, x_size = 300, 512, 512
    image = rng.integers(0, 60000, size=(z_size, y_size, x_size), dtype=np.uint16)
    spec = PyramidSpec(z_size_estimate=z_size, y=y_size, x=x_size, levels=4)
    scheme = ChunkScheme(base=(64, 64, 64), target=(64, 64, 64))
    with Live3DPyramidWriter(spec, voxel_size=(1.0, 1.0, 1.0), path=str(path),
                             chunk_scheme=scheme, ome_version="0.5",
                             flush_pad=FlushPad.DUPLICATE_LAST,
                             async_close=True, xy_levels=0) as writer:
        for k in range(z_size):
            writer.push_slice(image[k])
    writer.finalize_future.result()  # wait for async finalize

    root = zarr.open(str(path), mode="r")
    a0 = np.asarray(root["s0"][:])
    assert a0.shape == (1, 1, z_size, y_size, x_size)
    assert np.array_equal(a0[0, 0], image), "level 0 data mismatch"

    l1 = np.asarray(root["s1"][:])[0, 0]
    exp1 = expected_level1(image)
    assert l1.shape == exp1.shape, (l1.shape, exp1.shape)
    assert np.array_equal(l1, exp1), "level 1 data mismatch"

    # ome metadata on the reopened store
    ome = json.loads((path / "zarr.json").read_text())["attributes"]["ome"]
    assert ome["version"] == "0.5"
    assert [d["path"] for d in ome["multiscales"][0]["datasets"]] == ["s0", "s1", "s2", "s3"]

    # shard file layout: one file per touched shard cell under s0
    # (z: ceil(300/256)=2, y: 512/64=8, x: 512/64=8 -> 128 files)
    n_shard_files = len([f for f in (path / "s0").rglob("*") if f.is_file() and f.name != "zarr.json"])
    assert n_shard_files == 2 * 8 * 8, n_shard_files
    n_s1_files = len([f for f in (path / "s1").rglob("*") if f.is_file() and f.name != "zarr.json"])
    assert n_s1_files == 1 * 4 * 4, n_s1_files

    # .READY marker written next to the store
    assert (path / ".READY").exists()


def test_overwrite_idempotency():
    """mode='w' lets a re-run overwrite the same path cleanly."""
    path = TMP / "ow.ome.zarr"
    spec = PyramidSpec(z_size_estimate=20, y=64, x=64, levels=1)
    scheme = ChunkScheme(base=(64, 64, 64), target=(64, 64, 64))
    for run in range(2):
        root, arrs = init_sharded_zarr_v3(spec, str(path), chunk_scheme=scheme)
        arrs[0][0, 0, 0:20] = np.full((20, 64, 64), run + 1, dtype=np.uint16)
    a0 = np.asarray(zarr.open(str(path / "s0"), mode="r")[:])
    assert np.array_equal(a0[0, 0], np.full((20, 64, 64), 2, dtype=np.uint16))


def test_version_flag_default_is_0_5():
    """convert_one/Live3DPyramidWriter default to ome_version 0.5."""
    import inspect
    sig = inspect.signature(Live3DPyramidWriter.__init__)
    assert sig.parameters["ome_version"].default == "0.5"
    from fli_conversion import convert_one, parallel_convert
    assert inspect.signature(convert_one).parameters["ome_version"].default == "0.5"
    assert inspect.signature(parallel_convert).parameters["ome_version"].default == "0.5"


def test_x_only_level_plan():
    """Keep existing 3D levels, then stop at the first X size <= chunk target."""
    base = plan_levels(1280, 42000, 1024, 0, min_dim=64)
    assert base == 5
    assert plan_x_only_levels(42000, base, 256) == 9
    assert [level_factors(l, 0, base) for l in (4, 5, 8)] == [
        (16, 16, 16), (16, 16, 32), (16, 16, 256)]
    assert plan_x_only_levels(256, 1, 256) == 1
    assert plan_x_only_levels(257, 1, 256) == 2


def test_x_only_writer_roundtrip():
    """After the 3D level, X shrinks but Z/Y pixels and scale stay fixed."""
    path = TMP / "x_only.ome.zarr"
    image = rng.integers(0, 4096, size=(5, 9, 19), dtype=np.uint16)
    spec = PyramidSpec(z_size_estimate=5, y=9, x=19, levels=4, x_only_start=2)
    scheme = ChunkScheme(base=(2, 4, 4), target=(2, 4, 4))
    with Live3DPyramidWriter(spec, voxel_size=(0.528, 0.5616, 2.0), path=str(path),
                             chunk_scheme=scheme, ome_version="0.5",
                             async_close=False, xy_levels=0) as writer:
        for plane in image:
            writer.push_slice(plane)

    root = zarr.open(str(path), mode="r")
    l1 = expected_level1(image)
    l2 = expected_x_only(l1)
    l3 = expected_x_only(l2)
    for level, expected in enumerate((image, l1, l2, l3)):
        actual = np.asarray(root[f"s{level}"][:])[0, 0]
        assert actual.shape == expected.shape, (level, actual.shape, expected.shape)
        np.testing.assert_array_equal(actual, expected)
    assert [l1.shape, l2.shape, l3.shape] == [(3, 5, 10), (3, 5, 5), (3, 5, 3)]

    ms = load_json(path / "zarr.json")["attributes"]["ome"]["multiscales"][0]
    assert [d["coordinateTransformations"][0]["scale"] for d in ms["datasets"]] == [
        [1.0, 1.0, 0.528, 0.5616, 2.0],
        [1.0, 1.0, 1.056, 1.1232, 4.0],
        [1.0, 1.0, 1.056, 1.1232, 8.0],
        [1.0, 1.0, 1.056, 1.1232, 16.0],
    ]


def test_convert_one_x_only_tail():
    """The FLI entry point enables X-only levels and stops once X fits a chunk."""
    frames = (np.arange(601 * 2 * 4, dtype=np.uint16) % 200).reshape(601, 2, 4)
    pairs = frames.reshape(-1, 2)
    packed = np.column_stack((
        pairs[:, 0] & 0xff,
        ((pairs[:, 0] >> 8) & 0x0f) | ((pairs[:, 1] & 0x0f) << 4),
        pairs[:, 1] >> 4,
    )).astype(np.uint8).tobytes()
    source = TMP / "x_only.fli"
    source.write_bytes(b"x = 4\ny = 2\ntimestamps = 600\n{END}" + packed)

    path = convert_one(str(source), str(TMP), voxel=(0.528, 0.5616, 2.0))
    root = zarr.open(path, mode="r")
    l0 = frames[:600].transpose(1, 2, 0) * 16
    l1 = expected_x_only(l0)
    l2 = expected_x_only(l1)
    for level, expected in enumerate((l0, l1, l2)):
        actual = np.asarray(root[f"s{level}"][:])[0, 0]
        np.testing.assert_array_equal(actual, expected)
    assert l2.shape == (2, 4, 150)
    ms = load_json(Path(path) / "zarr.json")["attributes"]["ome"]["multiscales"][0]
    assert [d["coordinateTransformations"][0]["scale"] for d in ms["datasets"]] == [
        [1.0, 1.0, 0.528, 0.5616, 2.0],
        [1.0, 1.0, 0.528, 0.5616, 4.0],
        [1.0, 1.0, 0.528, 0.5616, 8.0],
    ]


def test_fli_scan_axis_view_roundtrip():
    """Packed FLI frames reach the writer as a strided (z, y, x) view."""
    from fli_conversion import convert_one
    from fli_loading import fli_loading

    # Four scan-x frames, two microscope-z rows, four microscope-y columns.
    # The fifth frame is the trailing frame excluded by the FLI loader.
    frames = np.arange(5 * 2 * 4, dtype=np.uint16).reshape(5, 2, 4)
    pairs = frames.reshape(-1, 2)
    packed = np.column_stack((
        pairs[:, 0] & 0xff,
        ((pairs[:, 0] >> 8) & 0x0f) | ((pairs[:, 1] & 0x0f) << 4),
        pairs[:, 1] >> 4,
    )).astype(np.uint8).tobytes()
    source = TMP / "orientation.fli"
    source.write_bytes(b"x = 4\ny = 2\ntimestamps = 4\n{END}" + packed)

    # This read has one full and one short group; their declared Dask shapes
    # must reflect the actual frame counts.
    raw = fli_loading(str(source), frames_at_once=3).compute()
    assert raw.shape == (4, 2, 4)
    np.testing.assert_array_equal(raw, frames[:4] * 16)

    strides = []
    original_push = Live3DPyramidWriter.push_slice

    def capture_push(self, plane):
        strides.append(plane.strides)
        assert not plane.flags.c_contiguous
        return original_push(self, plane)

    Live3DPyramidWriter.push_slice = capture_push
    try:
        path = Path(convert_one(str(source), str(TMP), voxel=(0.528, 0.5616, 2.0)))
    finally:
        Live3DPyramidWriter.push_slice = original_push

    assert len(strides) == 2
    root = zarr.open(str(path), mode="r")
    np.testing.assert_array_equal(root["s0"][0, 0], frames[:4].transpose(1, 2, 0) * 16)
    ms = load_json(path / "zarr.json")["attributes"]["ome"]["multiscales"][0]
    assert [axis["name"] for axis in ms["axes"]] == ["t", "c", "z", "y", "x"]
    assert ms["datasets"][0]["coordinateTransformations"][0]["scale"] == [
        1.0, 1.0, 0.528, 0.5616, 2.0]


def main():
    if int(zarr.__version__.split(".")[0]) < 3:
        print(f"SKIP: requires zarr v3 (found {zarr.__version__})")
        return 0
    try:
        check("store_format", test_store_format)
        check("shard_validation", test_shard_validation)
        check("shard_clipping_small_level", test_shard_clipping_small_level)
        check("writer_roundtrip", test_writer_roundtrip)
        check("overwrite_idempotency", test_overwrite_idempotency)
        check("version_flag_default_is_0_5", test_version_flag_default_is_0_5)
        check("x_only_level_plan", test_x_only_level_plan)
        check("x_only_writer_roundtrip", test_x_only_writer_roundtrip)
        check("convert_one_x_only_tail", test_convert_one_x_only_tail)
        check("fli_scan_axis_view_roundtrip", test_fli_scan_axis_view_roundtrip)
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{_passed} tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
