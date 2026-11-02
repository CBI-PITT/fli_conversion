"""Tests for acquire_zarr_writer.write_volume_ome_zarr_acquire (T1 scope).

Run with the acquire-zarr environment (acquire-zarr>=0.10,<0.11):

    python test_acquire_zarr_writer.py

Plain-assert script (no pytest dependency). Exits non-zero on failure.

Note on the level-1 reference: acquire-zarr 0.10.0 downsamples XY first
(2x2 floor mean per incoming frame), then averages consecutive XY-reduced
frames in z with floor((a+b)/2). This was verified byte-exact against the
installed library; the plan's draft wording ("z-pairs then XY") does not
match the library's actual order and is corrected here.
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import zarr

sys.path.insert(0, str(Path(__file__).parent))

from acquire_zarr_writer import write_volume_ome_zarr_acquire  # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix="fli_acquire_test_"))
rng = np.random.default_rng(42)

_passed = 0


def check(name, fn):
    global _passed
    fn()
    _passed += 1
    print(f"PASS  {name}")


def load_json(path):
    return json.loads(Path(path).read_text())


def small_volume():
    """A valid small (Z,Y,X) uint16 volume for guard tests."""
    return rng.integers(0, 60000, size=(4, 16, 16), dtype=np.uint16)


def truncating_level1(volume):
    """Truncating-mean reference for one pyramid level (acquire 0.10.0 MEAN).

    Per input frame: 2x2 XY mean with floor((a+b+c+d)/4), edge-clamped by
    duplicating the last row/column for odd sizes; then floor((a+b)/2)
    z-pairs of the XY-reduced frames. The T1 volumes have even spatial
    dims, so the clamp is a no-op here.
    """
    z, y, x = volume.shape
    pad_y, pad_x = y % 2, x % 2
    f = volume
    if pad_y or pad_x:
        padded = np.zeros((z, y + pad_y, x + pad_x), dtype=volume.dtype)
        padded[:, :y, :x] = volume
        if pad_y:
            padded[:, y, :] = volume[:, y - 1, :]
        if pad_x:
            padded[:, :, x] = volume[:, :, x - 1]
        f = padded
    g = f.astype(np.uint32)
    xy = (g[:, 0::2, 0::2] + g[:, 0::2, 1::2]
          + g[:, 1::2, 0::2] + g[:, 1::2, 1::2]) // 4
    return ((xy[0::2] + xy[1::2]) // 2).astype(np.uint16)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_roundtrip():
    """Seeded random volume round-trips pixel-exact with a truncating-mean pyramid."""
    path = TMP / "rt.ome.zarr"
    z_size, y_size, x_size = 40, 64, 64
    volume = rng.integers(0, 60000, size=(z_size, y_size, x_size), dtype=np.uint16)
    out = write_volume_ome_zarr_acquire(
        volume,
        path,
        voxel_size_zyx_um=(2.0, 1.0, 0.5),
        chunk_shape_zyx=(8, 16, 16),
        shard_shape_zyx=(16, 16, 16),
        num_levels=2,
        num_threads=4,
        slab_depth_z=16,  # force multi-slab appends (16 + 16 + 8)
    )
    assert out == path

    # root metadata: sharded zarr v3 group with OME-NGFF 0.5
    rj = load_json(path / "zarr.json")
    assert rj["zarr_format"] == 3, rj["zarr_format"]
    assert rj["node_type"] == "group"
    assert rj["attributes"]["ome"]["version"] == "0.5"

    # num_levels=2 caps acquire's ADDITIONAL levels: datasets 0..2
    names = sorted(d["path"] for d in rj["attributes"]["ome"]["multiscales"][0]["datasets"])
    assert names == ["0", "1", "2"], names

    root = zarr.open(str(path), mode="r")
    a0 = np.asarray(root["0"][:])
    assert a0.shape == (1, 1, z_size, y_size, x_size)
    assert np.array_equal(a0[0, 0], volume), "level 0 data mismatch"

    # level shapes follow the ceil cascade on (z, y, x)
    assert np.asarray(root["1"]).shape == (1, 1, 20, 32, 32)
    assert np.asarray(root["2"]).shape == (1, 1, 10, 16, 16)

    # level 1 == truncating-mean reference (XY floor-mean then z floor-pairs)
    l1 = np.asarray(root["1"][:])[0, 0]
    exp1 = truncating_level1(volume)
    assert l1.shape == exp1.shape, (l1.shape, exp1.shape)
    assert np.array_equal(l1, exp1), "level 1 data mismatch"

    # .READY marker written next to the store after successful close
    assert (path / ".READY").read_text() == "ok"


def test_overwrite_guard():
    """Existing output + overwrite=False raises FileExistsError."""
    path = TMP / "existing.ome.zarr"
    path.mkdir()
    try:
        write_volume_ome_zarr_acquire(
            small_volume(),
            path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 8, 8),
            num_levels=1,
        )
    except FileExistsError:
        pass
    else:
        raise AssertionError("existing output should raise FileExistsError")


def test_shard_validation():
    """Shards must be a multiple of the chunk on every axis."""
    for bad in ((12, 16, 16), (4, 16, 16), (16, 12, 16)):
        try:
            write_volume_ome_zarr_acquire(
                small_volume(),
                TMP / "bad_shard.ome.zarr",
                voxel_size_zyx_um=(1.0, 1.0, 1.0),
                chunk_shape_zyx=(8, 16, 16),
                shard_shape_zyx=bad,
                num_levels=1,
            )
        except ValueError:
            pass
        else:
            raise AssertionError(f"shard_shape_zyx={bad} should raise ValueError")


def test_codec_guard():
    """codec outside {'lz4','zstd'} raises ValueError."""
    try:
        write_volume_ome_zarr_acquire(
            small_volume(),
            TMP / "bad_codec.ome.zarr",
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 8, 8),
            codec="gzip",
            num_levels=1,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("codec='gzip' should raise ValueError")


def test_dtype_guard():
    """Unsupported or non-native dtypes raise TypeError."""
    for bad in (np.float16, np.dtype(">u2")):
        volume = np.zeros((4, 16, 16), dtype=bad)
        try:
            write_volume_ome_zarr_acquire(
                volume,
                TMP / "bad_dtype.ome.zarr",
                voxel_size_zyx_um=(1.0, 1.0, 1.0),
                chunk_shape_zyx=(4, 8, 8),
                num_levels=1,
            )
        except TypeError:
            pass
        else:
            raise AssertionError(f"dtype {bad} should raise TypeError")


def test_missing_import_hint():
    """A missing acquire_zarr raises ImportError with the install hint."""
    saved = sys.modules.get("acquire_zarr")
    sys.modules["acquire_zarr"] = None  # forces ImportError on import
    try:
        write_volume_ome_zarr_acquire(
            small_volume(),
            TMP / "noimport.ome.zarr",
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 8, 8),
            num_levels=1,
        )
    except ImportError as e:
        assert "acquire-zarr>=0.10" in str(e), str(e)
    else:
        raise AssertionError("missing acquire_zarr should raise ImportError")
    finally:
        if saved is None:
            sys.modules.pop("acquire_zarr", None)
        else:
            sys.modules["acquire_zarr"] = saved


def test_close_failure():
    """A failed close() propagates RuntimeError and .READY is not written."""
    import acquire_zarr

    path = TMP / "closefail.ome.zarr"
    orig_close = acquire_zarr.ZarrStream.close

    def failing_close(self):
        raise RuntimeError("Failed to close Zarr stream: simulated flush failure")

    acquire_zarr.ZarrStream.close = failing_close
    try:
        try:
            write_volume_ome_zarr_acquire(
                small_volume(),
                path,
                voxel_size_zyx_um=(1.0, 1.0, 1.0),
                chunk_shape_zyx=(4, 8, 8),
                num_levels=1,
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError("failed close() should propagate RuntimeError")
    finally:
        acquire_zarr.ZarrStream.close = orig_close
    assert not (path / ".READY").exists()


def test_store_format():
    """Acquire emits the 5D OME 0.5 schema; primary path writes physical scales."""
    path = TMP / "fmt.ome.zarr"
    z_size, y_size, x_size = 40, 64, 64
    volume = rng.integers(0, 60000, size=(z_size, y_size, x_size), dtype=np.uint16)
    write_volume_ome_zarr_acquire(
        volume,
        path,
        voxel_size_zyx_um=(2.0, 1.0, 0.5),
        chunk_shape_zyx=(8, 16, 16),
        shard_shape_zyx=(16, 16, 16),
        num_levels=2,
        num_threads=4,
        slab_depth_z=16,
        progress_every=0,
    )
    rj = load_json(path / "zarr.json")
    assert rj["zarr_format"] == 3, rj["zarr_format"]
    assert rj["node_type"] == "group"
    assert rj["attributes"]["ome"]["version"] == "0.5"

    ms = rj["attributes"]["ome"]["multiscales"][0]
    assert [a["name"] for a in ms["axes"]] == ["t", "c", "z", "y", "x"]
    assert [a["type"] for a in ms["axes"]] == [
        "time", "channel", "space", "space", "space"]
    # c carries no unit key (absent); t uses milliseconds, space micrometers
    assert [a.get("unit") for a in ms["axes"]] == [
        "millisecond", None, "micrometer", "micrometer", "micrometer"]

    # num_levels=2 caps acquire's ADDITIONAL levels: datasets 0..2
    assert [d["path"] for d in ms["datasets"]] == ["0", "1", "2"]
    # 5-value (t,c,z,y,x) scales, physical from the primary path: voxel x 2^L
    for d, s in zip(ms["datasets"], ([1.0, 1.0, 2.0, 1.0, 0.5],
                                     [1.0, 1.0, 4.0, 2.0, 1.0],
                                     [1.0, 1.0, 8.0, 4.0, 2.0])):
        tf = d["coordinateTransformations"][0]
        assert tf["type"] == "scale"
        assert tf["scale"] == s, tf["scale"]

    # array-level zarr.json: sharded grid with the 5D inner chunk + codecs
    s0 = load_json(path / "0" / "zarr.json")
    assert s0["zarr_format"] == 3
    assert s0["node_type"] == "array"
    assert s0["dimension_names"] == ["t", "c", "z", "y", "x"]
    assert s0["data_type"] == "uint16"
    shard_codec = s0["codecs"][0]
    assert shard_codec["name"] == "sharding_indexed"
    assert shard_codec["configuration"]["chunk_shape"] == [1, 1, 8, 16, 16]
    blosc = shard_codec["configuration"]["codecs"][1]
    assert blosc["name"] == "blosc"
    assert blosc["configuration"]["cname"] == "zstd"
    assert blosc["configuration"]["shuffle"] == "bitshuffle"


def test_physical_scale_fallback():
    """_ensure_physical_scales rewrites relative scales; no-op when already physical."""
    import acquire_zarr_writer

    path = TMP / "fallback.ome.zarr"
    voxel = (2.0, 1.0, 0.5)
    volume = rng.integers(0, 60000, size=(40, 64, 64), dtype=np.uint16)
    write_volume_ome_zarr_acquire(
        volume,
        path,
        voxel_size_zyx_um=voxel,
        chunk_shape_zyx=(8, 16, 16),
        shard_shape_zyx=(16, 16, 16),
        num_levels=2,
        num_threads=4,
        slab_depth_z=16,
        progress_every=0,
    )
    meta_path = path / "zarr.json"

    # No-op path: the primary Dimension.scale/unit emission is already
    # physical, so the call must leave the file byte-identical.
    before = meta_path.read_bytes()
    acquire_zarr_writer._ensure_physical_scales(path, voxel)
    assert meta_path.read_bytes() == before, (
        "no-op call must not rewrite an already-physical zarr.json")

    # Simulate a relative-scale emission: divide ONLY the last three (z,y,x)
    # entries of every 5-value scale by the voxel size (t,c stay 1.0).
    rj = json.loads(meta_path.read_text())
    datasets = rj["attributes"]["ome"]["multiscales"][0]["datasets"]
    for d in datasets:
        s = d["coordinateTransformations"][0]["scale"]
        for i, v in enumerate(voxel):
            s[2 + i] = s[2 + i] / v
    meta_path.write_text(json.dumps(rj, indent=2))

    acquire_zarr_writer._ensure_physical_scales(path, voxel)

    # File still parses; scales are physical again with t,c untouched.
    rj2 = json.loads(meta_path.read_text())
    ms2 = rj2["attributes"]["ome"]["multiscales"][0]
    for d, s in zip(ms2["datasets"], ([1.0, 1.0, 2.0, 1.0, 0.5],
                                      [1.0, 1.0, 4.0, 2.0, 1.0],
                                      [1.0, 1.0, 8.0, 4.0, 2.0])):
        scale = d["coordinateTransformations"][0]["scale"]
        assert scale == s, scale
        assert scale[0] == 1.0 and scale[1] == 1.0, scale
    assert [a.get("unit") for a in ms2["axes"]] == [
        "millisecond", None, "micrometer", "micrometer", "micrometer"]


# ---------------------------------------------------------------------------
# T3 wiring tests: writer-backend selection in fli_conversion.py
# ---------------------------------------------------------------------------
def test_wiring_signatures():
    """convert_one/parallel_convert default to writer_backend='acquire'; the python writer gains no param."""
    import inspect
    from fli_conversion import Live3DPyramidWriter, convert_one, parallel_convert
    assert inspect.signature(convert_one).parameters["writer_backend"].default == "acquire"
    assert inspect.signature(parallel_convert).parameters["writer_backend"].default == "acquire"
    assert "writer_backend" not in inspect.signature(Live3DPyramidWriter.__init__).parameters


def test_convert_one_guards():
    """Backend/ome_version/xy_levels guards raise ValueError before any file I/O."""
    from fli_conversion import convert_one
    for kwargs in (
        {"ome_version": "0.4", "writer_backend": "acquire"},
        {"xy_levels": 1, "writer_backend": "acquire"},
        {"writer_backend": "zarr"},
    ):
        try:
            # "x"/"y" do not exist: without the guards, fli_loading would fail differently.
            convert_one("x", "y", **kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError(f"convert_one({kwargs}) should raise ValueError")


def test_resolve_writer_backend():
    """resolve_writer_backend maps (--writer, --ome_version) per the CLI matrix."""
    from fli_conversion import resolve_writer_backend
    assert resolve_writer_backend(None, "0.5") == "acquire"
    assert resolve_writer_backend(None, "0.4") == "python"
    assert resolve_writer_backend("python", "0.4") == "python"
    assert resolve_writer_backend("acquire", "0.5") == "acquire"
    assert resolve_writer_backend("python", "0.5") == "python"
    try:
        resolve_writer_backend("acquire", "0.4")
    except ValueError:
        pass
    else:
        raise AssertionError("resolve_writer_backend('acquire', '0.4') should raise ValueError")


def test_single_level_zero_max_levels():
    """num_levels=0 is accepted; with the array inside one chunk the store is single-level.

    acquire's max_levels=0 means "no limit" (probe-verified 0.10.0), so the
    dataset count follows the natural cascade; chunk (4,16,16) >= the array
    dims on every axis gives an empty cascade -> exactly one dataset.
    """
    path = TMP / "single_level.ome.zarr"
    volume = small_volume()
    write_volume_ome_zarr_acquire(
        volume,
        path,
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=0,
        num_threads=2,
        progress_every=0,
    )
    rj = load_json(path / "zarr.json")
    assert rj["zarr_format"] == 3, rj["zarr_format"]
    assert rj["node_type"] == "group"
    assert rj["attributes"]["ome"]["version"] == "0.5"
    names = [d["path"] for d in rj["attributes"]["ome"]["multiscales"][0]["datasets"]]
    assert names == ["0"], names
    root = zarr.open(str(path), mode="r")
    a0 = np.asarray(root["0"][:])
    assert a0.shape == (1, 1, 4, 16, 16)
    assert np.array_equal(a0[0, 0], volume), "level 0 data mismatch"


def main():
    try:
        check("roundtrip", test_roundtrip)
        check("overwrite_guard", test_overwrite_guard)
        check("shard_validation", test_shard_validation)
        check("codec_guard", test_codec_guard)
        check("dtype_guard", test_dtype_guard)
        check("missing_import_hint", test_missing_import_hint)
        check("close_failure", test_close_failure)
        check("store_format", test_store_format)
        check("physical_scale_fallback", test_physical_scale_fallback)
        check("wiring_signatures", test_wiring_signatures)
        check("convert_one_guards", test_convert_one_guards)
        check("resolve_writer_backend", test_resolve_writer_backend)
        check("single_level_zero_max_levels", test_single_level_zero_max_levels)
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{_passed} tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
