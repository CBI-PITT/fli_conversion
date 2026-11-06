"""BeeGFS / production-mount A/B/C benchmark: zarrista (Rust zarrs) vs acquire vs python.

Portable companion of bench_beegfs_runner.py (see .omo/evidence/ for that
harness's results). Runs the SAME mount characterization and
batch-contention tests ON THE MOUNT YOU POINT IT AT, plus the zarrista
variants - ngff-zarr 0.52 writing through zarrista / Rust zarrs - alongside
the python and acquire reference backends for direct comparison.

Copy this file plus fli_conversion.py, fli_loading.py, zarrista_writer.py
and acquire_zarr_writer.py to the production node (keep them in one
directory); activate the conversion env (Python >= 3.11, ngff-zarr>=0.52,
dask, numcodecs, zarr>=3.1, acquire-zarr, numpy) and run:

    # 1) characterize the mount (raw write throughput + O_DIRECT viability)
    python bench_zarrista_beegfs_runner.py --store-dir /beegfs/path/bench_tmp --probe

    # 2) full suite: mount probe + 8 single-process variants + 2 batch tests
    #    (8.59 GiB volume, 2 reps each; batch needs ~40 GiB RAM and ~40 GiB
    #     of scratch for 4 concurrent 8.59 GiB stores)
    python bench_zarrista_beegfs_runner.py --store-dir /beegfs/path/bench_tmp

    # targeted rerun of one variant / batch test
    python bench_zarrista_beegfs_runner.py --store-dir /beegfs/path/bench_tmp --variant zarrista_tuned
    python bench_zarrista_beegfs_runner.py --store-dir /beegfs/path/bench_tmp --batch 9 --batch-backend zarrista

All stores are deleted after each rep; the scratch dir is removed at the end
of the full suite. Paste the PROBE / RESULT / AGGREGATE / SUMMARY lines back
for the evidence record.

Output lines:
  PROBE   <what> ...            (mount characterization; O_DIRECT info only)
  RESULT  <variant> rep=<k> wall_s=<t> gib_s=<g> store_mib=<m> threads=<n> rayon=<r>
  AGGREGATE batch threads=<n> backend=<b> wall_s=<max> total_gib=<g> agg_gib_s=<g>
  SUMMARY <variant> ok|failed|skipped <note>

Variants (zarrista knobs, see zarrista_writer.py):
  zarrista_default     writer defaults (workers = SLURM_CPUS_PER_TASK /
                       affinity, blosc:lz4 level 1, default shards)
  zarrista_tuned       --threads Dask workers
  zarrista_big_shards  --threads Dask workers, shards (2048, 512, 512)
  zarrista_rayon       --threads Dask workers AND RAYON_NUM_THREADS=--threads
                       (the Rust pool must latch before the first zarrs call)
  zarrista_zstd        --threads Dask workers, blosc:zstd level 5
  zarrista_cache       --threads Dask workers, cache_intermediate=True
"""
import argparse
import mmap
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

Z, Y, X = 1024, 2048, 2048  # module defaults; overridden by --shape
CHUNK = (256, 256, 256)
VARIANT = None  # set in main() before any writer is touched
ZARRISTA_VARIANTS = ("zarrista_default", "zarrista_tuned", "zarrista_big_shards",
                     "zarrista_rayon", "zarrista_zstd", "zarrista_cache")


def _maybe_set_rayon(threads: int) -> None:
    """RAYON_NUM_THREADS latches when the Rust pool starts: set it before anything."""
    if VARIANT == "zarrista_rayon":
        os.environ["RAYON_NUM_THREADS"] = str(threads)


def _store_size_mib(path: Path) -> float:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            fp = Path(root) / f
            try:
                total += fp.stat().st_size
            except OSError:
                pass
    return total / 2**20


def _make_volume(z: int, y: int, x: int):
    import numpy as np
    print(f"Generating ({z}, {y}, {x}) uint16 volume ...", flush=True)
    t0 = time.perf_counter()
    vol = np.random.default_rng(42).integers(
        0, 60000, size=(z, y, x), dtype=np.uint16)
    print(f"Generated in {time.perf_counter() - t0:.1f} s "
          f"({vol.nbytes / 2**30:.2f} GiB)", flush=True)
    return vol


def _store_path(store_dir: Path, variant: str, tag: str) -> Path:
    return store_dir / f"{variant}_{tag}.ome.zarr"


def _zarrista_kwargs(levels: int, tuned_threads: int, variant: str) -> dict:
    kwargs = dict(
        voxel_size_zyx_um=(1.0, 1.0, 1.0),
        chunk_shape_zyx=CHUNK,
        num_levels=max(0, levels - 1),
        overwrite=True,
        show_progress=False,
    )
    if variant != "zarrista_default":
        kwargs["num_workers"] = tuned_threads
    if variant == "zarrista_big_shards":
        kwargs["shard_shape_zyx"] = (2048, 512, 512)
    elif variant == "zarrista_zstd":
        kwargs["codec"] = "blosc:zstd"
        kwargs["compression_level"] = 5
    elif variant == "zarrista_cache":
        kwargs["cache_intermediate"] = True
    return kwargs


def _run_variant(vol, variant: str, store_dir: Path, reps: int,
                 tuned_threads: int) -> None:
    from fli_conversion import plan_levels
    z, y, x = vol.shape
    levels = plan_levels(y, x, z, 0, min_dim=64)
    raw_gib = vol.nbytes / 2**30
    rayon = os.environ.get("RAYON_NUM_THREADS", "none")

    for rep in range(1, reps + 1):
        path = _store_path(store_dir, variant, f"rep{rep}")
        # Parallel FS occasionally returns EIO under concurrent atomic
        # writes; retry the whole rep once with a clean store.
        for attempt in (1, 2):
            if path.exists():
                shutil.rmtree(path)
            try:
                t0 = time.perf_counter()
                if variant == "python":
                    from fli_conversion import (ChunkScheme, FlushPad,
                                                Live3DPyramidWriter, PyramidSpec)
                    spec = PyramidSpec(z_size_estimate=z, y=y, x=x, levels=levels)
                    scheme = ChunkScheme(base=CHUNK, target=CHUNK)
                    with Live3DPyramidWriter(
                        spec, voxel_size=(1.0, 1.0, 1.0), path=str(path),
                        max_workers=9, chunk_scheme=scheme, compressor=None,
                        flush_pad=FlushPad.DUPLICATE_LAST, async_close=False,
                        shard_shape=None, xy_levels=0, ome_version="0.5",
                        max_inflight_chunks=9,
                    ) as writer:
                        for k in range(z):
                            writer.push_slice(vol[k])
                    threads = 9
                elif variant == "acquire_default":
                    from acquire_zarr_writer import write_volume_ome_zarr_acquire
                    write_volume_ome_zarr_acquire(
                        vol, path,
                        voxel_size_zyx_um=(1.0, 1.0, 1.0),
                        chunk_shape_zyx=CHUNK,
                        num_levels=max(0, levels - 1),
                        overwrite=True,
                        progress_every=0,
                    )
                    threads = 0  # writer default
                else:
                    from zarrista_writer import write_volume_ome_zarr_zarrista
                    kwargs = _zarrista_kwargs(levels, tuned_threads, variant)
                    write_volume_ome_zarr_zarrista(vol, path, **kwargs)
                    threads = kwargs.get("num_workers") or 0  # 0 = writer default
                dt = time.perf_counter() - t0
                store_mib = _store_size_mib(path)
            except OSError as e:
                if attempt == 1 and getattr(e, "errno", None) == 5:
                    print(f"  rep={rep} attempt {attempt} hit EIO; retrying",
                          flush=True)
                    time.sleep(2)
                    continue
                raise
            print(f"RESULT {variant} rep={rep} wall_s={dt:.2f} "
                  f"gib_s={raw_gib / dt:.3f} store_mib={store_mib:.0f} "
                  f"threads={threads} rayon={rayon}", flush=True)
            shutil.rmtree(path, ignore_errors=True)
            break


def _batch_worker(worker_id: int, threads: int, backend: str, store_dir: Path,
                  shape, q) -> None:
    from fli_conversion import plan_levels
    z, y, x = shape
    vol = _make_volume(z, y, x)
    levels = plan_levels(y, x, z, 0, min_dim=64)
    path = _store_path(store_dir, f"batch{threads}", f"w{worker_id}")
    if path.exists():
        shutil.rmtree(path)
    t0 = time.perf_counter()
    if backend == "zarrista":
        from zarrista_writer import write_volume_ome_zarr_zarrista
        write_volume_ome_zarr_zarrista(
            vol, path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=CHUNK,
            num_levels=max(0, levels - 1),
            num_workers=threads,
            overwrite=True,
            show_progress=False,
        )
    else:
        from acquire_zarr_writer import write_volume_ome_zarr_acquire
        write_volume_ome_zarr_acquire(
            vol, path,
            voxel_size_zyx_um=(1.0, 1.0, 1.0),
            chunk_shape_zyx=CHUNK,
            num_levels=max(0, levels - 1),
            num_threads=threads,
            overwrite=True,
            progress_every=0,
        )
    dt = time.perf_counter() - t0
    print(f"RESULT batch{threads} backend={backend} worker={worker_id} "
          f"wall_s={dt:.2f} gib_s={vol.nbytes / 2**30 / dt:.3f} "
          f"threads={threads}", flush=True)
    shutil.rmtree(path, ignore_errors=True)
    q.put(dt)


def _run_batch(store_dir: Path, threads: int, backend: str, n_workers: int,
               shape) -> None:
    import multiprocessing as mp
    q = mp.Queue()
    workers = [
        mp.Process(target=_batch_worker,
                   args=(i, threads, backend, store_dir, shape, q))
        for i in range(n_workers)
    ]
    t0 = time.perf_counter()
    for w in workers:
        w.start()
    walls = [q.get() for _ in range(n_workers)]
    for w in workers:
        w.join()
    total_wall = time.perf_counter() - t0
    z, y, x = shape
    total_gib = n_workers * z * y * x * 2 / 2**30
    print(f"AGGREGATE batch threads={threads} backend={backend} "
          f"wall_s={max(walls):.2f} total_gib={total_gib:.2f} "
          f"agg_gib_s={total_gib / max(walls):.3f} "
          f"(driver_wait_s={total_wall:.2f})", flush=True)


def _o_direct_unaligned_probe(store_dir: Path) -> tuple[bool, str]:
    """Can this FS accept the UNALIGNED O_DIRECT writes acquire produces?

    Informational for this harness (zarrista buffers through the page cache;
    acquire_default does not use O_DIRECT either) - kept so the probe record
    matches bench_beegfs_runner.py runs on the same mount.
    """
    path = store_dir / "o_direct_probe.tmp"
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_DIRECT, 0o644)
    except OSError as e:
        return False, f"open: {e}"
    try:
        os.pwrite(fd, b"x" * 17, 5)    # odd length, unaligned buffer/offset
        os.pwrite(fd, b"y" * 33, 1000)  # second unaligned offset
    except OSError as e:
        return False, f"pwrite: {e}"
    finally:
        os.close(fd)
        path.unlink(missing_ok=True)
    return True, ""


def _raw_write_probe(store_dir: Path, mib: int) -> None:
    """Sequential 1 MiB-write throughput: buffered (page-cache model of the
    zarrista and python writers) and aligned O_DIRECT (direct ceiling)."""
    gib = mib / 1024
    path = store_dir / "raw_write_probe.tmp"
    blob = os.urandom(1024 * 1024)

    t0 = time.perf_counter()
    with open(path, "wb") as f:  # buffered: what zarrista / zarr-python do
        for _ in range(mib):
            f.write(blob)
    dt = time.perf_counter() - t0
    print(f"PROBE buffered_seq_write wall_s={dt:.2f} gib_s={gib / dt:.3f} "
          f"(no fsync; page-cache model of the zarrista and python writers)",
          flush=True)
    path.unlink(missing_ok=True)

    buf = mmap.mmap(-1, 1024 * 1024)  # page-aligned buffer
    buf[:] = blob
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_DIRECT, 0o644)
        try:
            t0 = time.perf_counter()
            for _ in range(mib):
                os.write(fd, buf)  # 1 MiB-aligned offsets, sequential
            dt = time.perf_counter() - t0
            print(f"PROBE o_direct_seq_write wall_s={dt:.2f} "
                  f"gib_s={gib / dt:.3f}", flush=True)
        finally:
            os.close(fd)
    except OSError as e:
        print(f"PROBE o_direct_seq_write UNSUPPORTED: {e}", flush=True)
    finally:
        buf.close()
        path.unlink(missing_ok=True)


def _self(argv_extra: list[str], env: dict | None = None) -> int:
    cmd = [sys.executable, str(Path(__file__).resolve())] + argv_extra
    return subprocess.run(cmd, env=env).returncode


def _run_full_suite(args) -> int:
    store_dir = Path(args.store_dir)
    store_dir.mkdir(parents=True, exist_ok=True)
    failures = []

    print("=== mount probe ===", flush=True)
    _raw_write_probe(store_dir, args.probe_mib)
    dio_ok, dio_err = _o_direct_unaligned_probe(store_dir)
    if dio_ok:
        print("PROBE o_direct_unaligned OK (informational)", flush=True)
    else:
        print(f"PROBE o_direct_unaligned FAILED ({dio_err}); informational only",
              flush=True)

    print("=== single-process variants "
          f"({args.reps} reps, {args.threads} tuned threads) ===", flush=True)
    variants = ["python", "acquire_default"] + list(ZARRISTA_VARIANTS)
    base = ["--store-dir", args.store_dir, "--reps", str(args.reps),
            "--threads", str(args.threads),
            "--shape", str(Z), str(Y), str(X)]
    for v in variants:
        print(f"== running variant {v} ==", flush=True)
        env = dict(os.environ)
        if v == "zarrista_rayon":
            # The Rust pool latches at first use: fresh subprocess per variant.
            env["RAYON_NUM_THREADS"] = str(args.threads)
        if _self(["--variant", v] + base, env=env) != 0:
            failures.append(v)

    print("=== batch contention ===", flush=True)
    for threads in (36, 9):
        print(f"== batch: {args.batch_workers} workers x {threads} threads "
              f"({args.batch_backend}) ==", flush=True)
        if _self(["--batch", str(threads), "--store-dir", args.store_dir,
                  "--batch-workers", str(args.batch_workers),
                  "--batch-backend", args.batch_backend,
                  "--shape", str(Z), str(Y), str(X)]) != 0:
            failures.append(f"batch{threads}")

    for tag in variants + [f"batch{t}" for t in (36, 9)]:
        status = "failed" if tag in failures else "ok"
        print(f"SUMMARY {tag} {status}", flush=True)
    try:
        store_dir.rmdir()  # only succeeds when empty; leaves it otherwise
    except OSError:
        pass
    return 1 if failures else 0


def main():
    global VARIANT, Z, Y, X
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--store-dir", required=True,
                    help="scratch dir for benchmark stores (deleted after)")
    ap.add_argument("--variant", choices=["python", "acquire_default"]
                    + list(ZARRISTA_VARIANTS), default=None,
                    help="run one variant in-process (full suite otherwise)")
    ap.add_argument("--probe", action="store_true",
                    help="mount characterization only")
    ap.add_argument("--probe-mib", type=int, default=4096,
                    help="MiB for each raw sequential probe write (default 4096)")
    ap.add_argument("--batch", type=int, default=None,
                    help="batch-contention test only, with N threads/worker")
    ap.add_argument("--batch-workers", type=int, default=4)
    ap.add_argument("--batch-backend", choices=["zarrista", "acquire"],
                    default="zarrista",
                    help="backend for the batch-contention test (default zarrista)")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--threads", type=int, default=9,
                    help="thread count for the tuned variants (default 9)")
    ap.add_argument("--shape", type=int, nargs=3, default=[1024, 2048, 2048],
                    metavar=("Z", "Y", "X"),
                    help="benchmark volume shape (default 1024 2048 2048)")
    args = ap.parse_args()

    Z, Y, X = (int(v) for v in args.shape)
    store_dir = Path(args.store_dir)
    store_dir.mkdir(parents=True, exist_ok=True)

    if args.probe:
        _raw_write_probe(store_dir, args.probe_mib)
        dio_ok, dio_err = _o_direct_unaligned_probe(store_dir)
        print(f"PROBE o_direct_unaligned "
              f"{'OK' if dio_ok else f'FAILED ({dio_err})'} (informational)",
              flush=True)
        return 0

    if args.batch is not None:
        _run_batch(store_dir, args.batch, args.batch_backend,
                   args.batch_workers, (Z, Y, X))
        return 0

    if args.variant is not None:
        VARIANT = args.variant
        # RAYON_NUM_THREADS must latch before the first zarrs call.
        _maybe_set_rayon(args.threads)
        vol = _make_volume(Z, Y, X)
        try:
            _run_variant(vol, VARIANT, store_dir, args.reps, args.threads)
        except Exception as e:  # noqa: BLE001  (record failures as results)
            print(f"RESULT {VARIANT} FAILED: {e!r}", flush=True)
            return 1
        return 0

    return _run_full_suite(args)


if __name__ == "__main__":
    sys.exit(main())
