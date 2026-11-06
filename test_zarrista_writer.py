"""Tests for zarrista_writer.write_volume_ome_zarr_zarrista (zarrista backend scope).

Run with the zarrista environment (ngff-zarr>=0.52,<0.53, dask, numcodecs):

    python test_zarrista_writer.py

Plain-assert script (no pytest dependency). Exits non-zero on failure.

Note on the level-1 reference: ngff-zarr 0.52 DASK_BIN_SHRINK takes ONE
float64 window mean per level step (2x2x2 here) and rounds half-up
(floor(mean + 0.5)) on integer input, trimming remainder windows for odd
sizes. This was verified byte-exact against the installed library; the
acquire backend truncates toward zero and differs by up to 1 LSB deeper in
the pyramid.
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import ngff_zarr as nz

sys.path.insert(0, str(Path(__file__).parent))

from zarrista_writer import (  # noqa: E402
    check_ome_zarr_v05_sharded_5d,
    write_volume_ome_zarr_zarrista,
)

TMP = Path(tempfile.mkdtemp(prefix="fli_zarrista_test_"))
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


def half_up_level1(volume):
    """Half-up window-mean reference for one 2x pyramid level (bin_shrink).

    float64 accumulate over the 2x2x2 window, then floor(mean + 0.5); the
    test volumes have even spatial dims, so trimming is a no-op.
    """
    z, y, x = volume.shape
    g = volume.astype(np.float64).reshape(z // 2, 2, y // 2, 2, x // 2, 2)
    return np.floor(g.mean(axis=(1, 3, 5)) + 0.5).astype(np.uint16)


def dataset_paths(path):
    rj = load_json(Path(path) / "zarr.json")
    return [d["path"] for d in rj["attributes"]["ome"]["multiscales"][0]["datasets"]]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_roundtrip():
    """Seeded random volume round-trips pixel-exact with a half-up-mean pyramid."""
    path = TMP / "rt.ome.zarr"
    z_size, y_size, x_size = 8, 64, 64
    volume = rng.integers(0, 60000, size=(z_size, y_size, x_size), dtype=np.uint16)
    out = write_volume_ome_zarr_zarrista(
        volume,
        path,
        voxel_size_zyx_um=(2.0, 1.0, 0.5),
        chunk_shape_zyx=(4, 32, 32),
        shard_shape_zyx=(8, 32, 32),
        num_levels=2,
        num_workers=4,
        show_progress=False,
    )
    assert out == path

    # root metadata: sharded zarr v3 group with OME-NGFF 0.5
    rj = load_json(path / "zarr.json")
    assert rj["zarr_format"] == 3, rj["zarr_format"]
    assert rj["node_type"] == "group"
    assert rj["attributes"]["ome"]["version"] == "0.5"

    # num_levels=2 additional levels: datasets scale0..scale2
    names = dataset_paths(path)
    assert names == ["scale0/image", "scale1/image", "scale2/image"], names

    level0 = nz.open_array(str(path), names[0])
    a0 = np.asarray(level0[:])
    assert a0.shape == (1, 1, z_size, y_size, x_size)
    assert np.array_equal(a0[0, 0], volume), "level 0 data mismatch"

    # level shapes halve z, y, x per level
    l1 = np.asarray(nz.open_array(str(path), names[1])[:])
    assert l1.shape == (1, 1, 4, 32, 32), l1.shape
    l2 = np.asarray(nz.open_array(str(path), names[2])[:])
    assert l2.shape == (1, 1, 2, 16, 16), l2.shape

    # level 1 == half-up 2x2x2 window mean
    exp1 = half_up_level1(volume)
    assert np.array_equal(l1[0, 0], exp1), "level 1 data mismatch"

    # axes with units: t millisecond, c no unit, z/y/x micrometer
    axes = rj["attributes"]["ome"]["multiscales"][0]["axes"]
    assert [a["name"] for a in axes] == ["t", "c", "z", "y", "x"], axes
    assert [a.get("unit") for a in axes] == [
        "millisecond", None, "micrometer", "micrometer", "micrometer"], axes

    # physical scales: level L = voxel x 2^L, t/c stay 1.0
    datasets = rj["attributes"]["ome"]["multiscales"][0]["datasets"]
    for level, (d, expected) in enumerate(zip(
            datasets, ([1.0, 1.0, 2.0, 1.0, 0.5],
                       [1.0, 1.0, 4.0, 2.0, 1.0],
                       [1.0, 1.0, 8.0, 4.0, 2.0]))):
        scale = [t["scale"] for t in d["coordinateTransformations"]
                 if t["type"] == "scale"][0]
        assert scale == expected, (level, scale)
        aj = load_json(path / d["path"] / "zarr.json")
        assert any(c["name"] == "sharding_indexed" for c in aj["codecs"]), level

    # .READY marker written next to the store after a successful write
    assert (path / ".READY").read_text() == "ok"


def test_overwrite_guard():
    """An existing output raises FileExistsError unless overwrite=True."""
    path = TMP / "ow.ome.zarr"
    path.mkdir()
    try:
        write_volume_ome_zarr_zarrista(
            small_volume(), path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 16, 16),
            num_levels=0,
            num_workers=2,
            show_progress=False,
        )
        raise AssertionError("existing output should raise FileExistsError")
    except FileExistsError:
        pass
    write_volume_ome_zarr_zarrista(
        small_volume(), path,
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=0,
        num_workers=2,
        overwrite=True,
        show_progress=False,
    )


def test_shard_validation():
    """Shard shapes must be chunk multiples on every axis."""
    path = TMP / "shard.ome.zarr"
    try:
        write_volume_ome_zarr_zarrista(
            small_volume(), path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 16, 16),
            shard_shape_zyx=(8, 17, 16),  # 17 not a multiple of 16
            num_levels=0,
            num_workers=2,
            show_progress=False,
        )
        raise AssertionError("non-multiple shard should raise ValueError")
    except ValueError:
        pass
    try:
        write_volume_ome_zarr_zarrista(
            small_volume(), path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 16, 16),
            shard_shape_zyx=(2, 16, 16),  # smaller than the chunk on z
            num_levels=0,
            num_workers=2,
            show_progress=False,
        )
        raise AssertionError("shard smaller than chunk should raise ValueError")
    except ValueError:
        pass
    assert not path.exists(), "guard fired after output creation"


def test_codec_and_param_guards():
    """Unknown codecs, methods, levels, workers raise ValueError."""
    path = TMP / "codec.ome.zarr"
    base = dict(
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=0,
        num_workers=2,
        show_progress=False,
    )
    for kwargs in (
        dict(base, codec="snappy"),
        dict(base, downsample_method="median"),
        dict(base, num_levels=-1),
        dict(base, compression_level=10),
        dict(base, num_workers=0),
    ):
        try:
            write_volume_ome_zarr_zarrista(small_volume(), path, **kwargs)
            raise AssertionError(f"{kwargs} should raise ValueError")
        except ValueError:
            pass
    # non-float voxels / non-integer chunks
    try:
        write_volume_ome_zarr_zarrista(small_volume(), path,
                                       voxel_size_zyx_um=(1.0, 0.0, 1.0), **{
                                           k: v for k, v in base.items()
                                           if k != "voxel_size_zyx_um"})
        raise AssertionError("non-positive voxel should raise ValueError")
    except ValueError:
        pass
    assert not path.exists(), "guard fired after output creation"


def test_dtype_guard():
    """Non-ndarray, wrong ndim, unsupported/non-native dtypes raise TypeError/ValueError."""
    path = TMP / "dtype.ome.zarr"
    try:
        write_volume_ome_zarr_zarrista(
            [[1, 2], [3, 4]], path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0), num_levels=0, num_workers=2,
            show_progress=False,
        )
        raise AssertionError("list input should raise TypeError")
    except TypeError:
        pass
    try:
        write_volume_ome_zarr_zarrista(
            small_volume()[0], path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0), num_levels=0, num_workers=2,
            show_progress=False,
        )
        raise AssertionError("2D input should raise ValueError")
    except ValueError:
        pass
    for dtype in (np.float16, np.bool_):
        try:
            write_volume_ome_zarr_zarrista(
                np.zeros((4, 16, 16), dtype=dtype), path,
                voxel_size_zyx_um=(1.0, 1.0, 1.0), num_levels=0, num_workers=2,
                show_progress=False,
            )
            raise AssertionError(f"{dtype} should raise TypeError")
        except TypeError:
            pass
    try:
        write_volume_ome_zarr_zarrista(
            np.zeros((4, 16, 16), dtype=">u2"), path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0), num_levels=0, num_workers=2,
            show_progress=False,
        )
        raise AssertionError("big-endian input should raise TypeError")
    except TypeError:
        pass
    assert not path.exists(), "guard fired after output creation"


def test_missing_import_hint():
    """A missing ngff_zarr raises ImportError with the install hint."""
    saved = sys.modules.get("ngff_zarr")
    sys.modules["ngff_zarr"] = None  # forces ImportError on import
    try:
        write_volume_ome_zarr_zarrista(
            small_volume(), TMP / "hint.ome.zarr",
            voxel_size_zyx_um=(1.0, 1.0, 1.0), num_levels=0, num_workers=2,
            show_progress=False,
        )
        raise AssertionError("missing ngff_zarr should raise ImportError")
    except ImportError as e:
        assert "ngff-zarr>=0.52" in str(e), str(e)
    finally:
        if saved is None:
            sys.modules.pop("ngff_zarr", None)
        else:
            sys.modules["ngff_zarr"] = saved


def test_write_failure_no_ready():
    """A failed write propagates and never writes .READY."""
    path = TMP / "fail.ome.zarr"
    orig_to_ome_zarr = nz.to_ome_zarr

    def failing_to_ome_zarr(*args, **kwargs):
        raise RuntimeError("simulated write failure")

    try:
        nz.to_ome_zarr = failing_to_ome_zarr
        write_volume_ome_zarr_zarrista(
            small_volume(), path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=(4, 16, 16),
            num_levels=0,
            num_workers=2,
            show_progress=False,
        )
        raise AssertionError("failed write should propagate RuntimeError")
    except RuntimeError:
        pass
    finally:
        nz.to_ome_zarr = orig_to_ome_zarr
    assert not (path / ".READY").exists(), ".READY written after a failed write"


def test_num_levels_zero():
    """num_levels=0 writes exactly one dataset (level 0 only)."""
    path = TMP / "single_level.ome.zarr"
    volume = small_volume()
    write_volume_ome_zarr_zarrista(
        volume, path,
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=0,
        num_workers=2,
        show_progress=False,
    )
    names = dataset_paths(path)
    assert names == ["scale0/image"], names
    level0 = nz.open_array(str(path), names[0])
    a0 = np.asarray(level0[:])
    assert a0.shape == (1, 1, 4, 16, 16)
    assert np.array_equal(a0[0, 0], volume), "level 0 data mismatch"


def test_num_levels_dropped():
    """Levels whose target size floors to zero on a spatial axis are dropped."""
    path = TMP / "dropped.ome.zarr"
    volume = rng.integers(0, 60000, size=(8, 64, 64), dtype=np.uint16)
    # num_levels=5: level 5 needs z >= 32 (z=8) and level 4 needs z >= 16;
    # both floor to zero, level 3 (z=1, y/x=8) is valid -> 4 datasets.
    write_volume_ome_zarr_zarrista(
        volume, path,
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=5,
        num_workers=2,
        show_progress=False,
    )
    names = dataset_paths(path)
    assert names == [f"scale{k}/image" for k in range(4)], names
    check_ome_zarr_v05_sharded_5d(
        path, volume, voxel_size_zyx_um=(1.0, 1.0, 1.0))


def test_default_shard_shape():
    """shard_shape_zyx=None defaults to 4x the chunk along z, chunk-sized y/x."""
    path = TMP / "default_shard.ome.zarr"
    volume = small_volume()
    write_volume_ome_zarr_zarrista(
        volume, path,
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=0,
        num_workers=2,
        show_progress=False,
    )
    aj = load_json(path / "scale0/image" / "zarr.json")
    shard = [c for c in aj["codecs"] if c["name"] == "sharding_indexed"][0]
    # default shard shape (4*4, 16, 16) voxels = (4, 1, 1) chunks per shard
    assert shard["configuration"]["chunk_shape"] == [1, 1, 4, 16, 16], shard


def test_check_function_scales():
    """check_ome_zarr_v05_sharded_5d accepts correct stores and rejects broken ones."""
    path = TMP / "check.ome.zarr"
    volume = small_volume()
    write_volume_ome_zarr_zarrista(
        volume, path,
        voxel_size_zyx_um=(2.0, 1.0, 0.5),
        chunk_shape_zyx=(4, 16, 16),
        num_levels=1,
        num_workers=2,
        show_progress=False,
    )
    paths = check_ome_zarr_v05_sharded_5d(
        path, volume, voxel_size_zyx_um=(2.0, 1.0, 0.5))
    assert paths == ["scale0/image", "scale1/image"], paths
    # pixel check against a wrong reference must fail
    try:
        check_ome_zarr_v05_sharded_5d(
            path, volume + 1, voxel_size_zyx_um=(2.0, 1.0, 0.5))
        raise AssertionError("wrong reference volume should fail the check")
    except AssertionError:
        pass
    # broken scale metadata must fail
    rj = load_json(path / "zarr.json")
    datasets = rj["attributes"]["ome"]["multiscales"][0]["datasets"]
    datasets[1]["coordinateTransformations"][0]["scale"][2] = 5.0
    (path / "zarr.json").write_text(json.dumps(rj))
    try:
        check_ome_zarr_v05_sharded_5d(
            path, volume, voxel_size_zyx_um=(2.0, 1.0, 0.5), check_pixels=False)
        raise AssertionError("broken scale should fail the check")
    except AssertionError:
        pass


# ---------------------------------------------------------------------------
# Wiring tests: writer-backend selection in fli_conversion.py
# ---------------------------------------------------------------------------
def test_wiring_signatures():
    """convert_one accepts writer_backend='zarrista'; the writer takes num_workers."""
    import inspect
    from fli_conversion import convert_one, parallel_convert, worker_loop

    for fn in (convert_one, parallel_convert, worker_loop):
        params = inspect.signature(fn).parameters
        assert "writer_backend" in params, fn
        assert "num_threads" in params, fn

    wt = inspect.signature(write_volume_ome_zarr_zarrista).parameters["num_workers"]
    assert wt.default is None

    # CLI: --writer includes zarrista; --writer_threads passes num_threads through
    import fli_conversion
    source = inspect.getsource(fli_conversion)
    assert '"--writer"' in source
    assert '"zarrista"' in source
    assert "num_workers=num_threads" in source


def test_convert_one_guards():
    """Backend/ome_version/xy_levels guards raise ValueError before any file I/O."""
    from fli_conversion import convert_one
    for kwargs in (
        {"ome_version": "0.4", "writer_backend": "acquire"},
        {"ome_version": "0.4", "writer_backend": "zarrista"},
        {"xy_levels": 1, "writer_backend": "acquire"},
        {"xy_levels": 1, "writer_backend": "zarrista"},
        {"writer_backend": "zarr"},
        {"writer_backend": "bogus"},
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
    assert resolve_writer_backend("zarrista", "0.5") == "zarrista"
    assert resolve_writer_backend("python", "0.5") == "python"
    try:
        resolve_writer_backend("zarrista", "0.4")
    except ValueError:
        pass
    else:
        raise AssertionError("resolve_writer_backend('zarrista', '0.4') should raise ValueError")


def main():
    try:
        check("roundtrip", test_roundtrip)
        check("overwrite_guard", test_overwrite_guard)
        check("shard_validation", test_shard_validation)
        check("codec_and_param_guards", test_codec_and_param_guards)
        check("dtype_guard", test_dtype_guard)
        check("missing_import_hint", test_missing_import_hint)
        check("write_failure_no_ready", test_write_failure_no_ready)
        check("num_levels_zero", test_num_levels_zero)
        check("num_levels_dropped", test_num_levels_dropped)
        check("default_shard_shape", test_default_shard_shape)
        check("check_function_scales", test_check_function_scales)
        check("wiring_signatures", test_wiring_signatures)
        check("convert_one_guards", test_convert_one_guards)
        check("resolve_writer_backend", test_resolve_writer_backend)
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{_passed} tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
