"""Supervised runs, retained failure evidence, and byte comparison."""

import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time

import psutil

from .config import Config


RUNTIME_KEYS = {"customer_group_size", "workers", "merge_fan_in", "export_rows", "memory_limit_mb",
                "disk_limit_mb", "timeout_seconds", "threads"}


def write_json(path, value):
    """Write stable, readable JSON and create its parent directory."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _disk_bytes(root):
    """Count retained file bytes while tolerating concurrent spool cleanup."""
    total = 0
    for directory, _, filenames in os.walk(root):
        for name in filenames:
            try:
                total += (Path(directory) / name).stat().st_size
            except FileNotFoundError:
                continue
    return total


def _temporary_disk_bytes(root):
    """Count export spools and validation partitions separately from source files."""
    return _disk_bytes(root / "_work") + sum(
        _disk_bytes(path) for path in root.glob("validation-*") if path.is_dir()
    )


def _process_tree_sample(pid):
    """Measure aggregate resident memory and active non-tracker processes."""
    try:
        root = psutil.Process(pid)
        processes = [root] + root.children(recursive=True)
    except psutil.NoSuchProcess:
        return 0, 0
    rss = 0
    count = 0
    for process in processes:
        try:
            if process.status() != psutil.STATUS_ZOMBIE:
                rss += process.memory_info().rss
                # Spawn uses a small resource tracker, which does not simulate a group.
                # Its RSS still counts toward the same aggregate memory budget.
                tracker = any(argument.startswith("from multiprocessing.resource_tracker import main;")
                              for argument in process.cmdline())
                if not tracker:
                    count += 1
        except psutil.NoSuchProcess:
            continue
    return rss, count


def _stop_process_tree(process):
    """Stop the coordinator and its descendants without leaving workers running."""
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        # The coordinator can exit before a child, so always kill the group too.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    else:
        try:
            descendants = psutil.Process(process.pid).children(recursive=True)
        except psutil.NoSuchProcess:
            descendants = []
        for child in descendants:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        if process.poll() is None:
            process.kill()
        process.wait()


def run(config: Config, output: Path):
    """Supervise a bounded worker and retain evidence if it cannot finish safely."""
    output = Path(output).absolute()
    partial = output.with_name(output.name + ".partial")
    if output.exists() or partial.exists() or output.is_symlink() or partial.is_symlink():
        raise FileExistsError("The output or partial directory already exists. Choose a new run directory.")
    if not 1 <= config.workers <= 2:
        raise ValueError("A run permits at most two worker processes, including the coordinator.")
    partial.mkdir(parents=True)
    config_path = partial / "config.json"
    write_json(config_path, config.as_dict())
    environment = {**os.environ, "POLARS_MAX_THREADS": str(config.threads), "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
    started = time.monotonic()
    maximum_rss = 0
    maximum_disk = 0
    maximum_temporary_disk = 0
    maximum_workers = 0
    next_disk_check = 0.0
    failure = None
    process = None
    try:
        with (partial / "worker.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                [sys.executable, "-m", "data.data_source.generator", "_worker", "--config", str(config_path), "--output", str(partial)],
                env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            while process.poll() is None:
                elapsed = time.monotonic() - started
                # Include this supervisor and every worker, not only the coordinator.
                rss, _ = _process_tree_sample(os.getpid())
                _, worker_count = _process_tree_sample(process.pid)
                maximum_rss = max(maximum_rss, rss)
                maximum_workers = max(maximum_workers, worker_count)
                if elapsed >= next_disk_check:
                    maximum_disk = max(maximum_disk, _disk_bytes(partial))
                    maximum_temporary_disk = max(maximum_temporary_disk, _temporary_disk_bytes(partial))
                    next_disk_check = elapsed + 2
                if maximum_rss > config.memory_limit_mb * 1024**2:
                    failure = "The aggregate process-tree RSS exceeded the memory budget."
                elif maximum_workers > config.workers:
                    failure = "The process tree exceeded the configured worker count."
                elif maximum_disk > config.disk_limit_mb * 1024**2:
                    failure = "The sampled disk usage exceeded the disk budget."
                elif elapsed > config.timeout_seconds:
                    failure = "The run exceeded its time budget."
                if failure:
                    _stop_process_tree(process)
                    break
                time.sleep(0.1)
            if process.returncode != 0 and failure is None:
                failure = f"The worker exited with status {process.returncode}. See worker.log."
        supervision = {"elapsed_seconds": time.monotonic() - started, "sampled_peak_rss_bytes": maximum_rss,
                       "sampled_peak_disk_bytes": max(maximum_disk, _disk_bytes(partial)),
                       "sampled_peak_temporary_disk_bytes": max(maximum_temporary_disk, _temporary_disk_bytes(partial)),
                       "maximum_worker_processes": maximum_workers, "memory_sampling_seconds": 0.1,
                       "disk_sampling_seconds": 2, "memory_scope": "supervisor and all descendants"}
        if failure:
            raise RuntimeError(failure)
        manifest = json.loads((partial / "manifest.json").read_text())
        if manifest.get("status") != "validated":
            raise RuntimeError("The worker did not produce a validated manifest.")
        if manifest["peak_rss_bytes"] > config.memory_limit_mb * 1024**2 or supervision["sampled_peak_disk_bytes"] > config.disk_limit_mb * 1024**2:
            raise RuntimeError("The completed worker exceeded a resource budget.")
        manifest["peak_rss_bytes"] = max(manifest["peak_rss_bytes"], maximum_rss)
        manifest["supervision"] = supervision
        write_json(partial / "manifest.json", manifest)
        summary = json.loads((partial / "summary.json").read_text())
        summary["runtime"]["supervision"] = supervision
        summary["runtime"]["peak_rss_bytes"] = manifest["peak_rss_bytes"]
        write_json(partial / "summary.json", summary)
        write_json(partial / "supervision.json", supervision)
        if _disk_bytes(partial) > config.disk_limit_mb * 1024**2:
            raise RuntimeError("The completed run metadata exceeded the disk budget.")
        work = partial / "_work"
        if work.exists():
            shutil.rmtree(work)
        partial.rename(output)
        return manifest
    except BaseException as error:
        if process is not None:
            _stop_process_tree(process)
        write_json(partial / "failure.json", {"reason": str(error) or type(error).__name__,
                   "elapsed_seconds": time.monotonic() - started, "sampled_peak_rss_bytes": maximum_rss,
                   "sampled_peak_disk_bytes": max(maximum_disk, _disk_bytes(partial)),
                   "maximum_worker_processes": maximum_workers})
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeError(f"{error} Retained files: {partial}") from error


def inventory_summary(inventory):
    """Aggregate source bytes and rows by family, delivery, and date."""
    totals = {"initial": {"rows": 0, "bytes": 0}, "daily": {"rows": 0, "bytes": 0}, "stream": {"rows": 0, "bytes": 0}}
    families = {}
    dates = {}
    for item in inventory:
        path = Path(item["path"])
        delivery = "stream" if path.parts[0] == "stream" else path.parts[1]
        family = "CashTransaction" if delivery == "stream" else "FINWIRE" if path.name.startswith("FINWIRE") else path.stem.split("_", 1)[0]
        bucket = families.setdefault(family, {name: {"rows": 0, "bytes": 0} for name in totals})
        day = path.stem if delivery == "stream" else path.parts[2] if delivery == "daily" else "initial"
        dated = dates.setdefault(day, {name: {"rows": 0, "bytes": 0} for name in totals})
        for metric in ("rows", "bytes"):
            totals[delivery][metric] += item[metric]
            bucket[delivery][metric] += item[metric]
            dated[delivery][metric] += item[metric]
    return {"deliveries": totals, "families": families, "dates": dates}


def implementation_hashes(package: Path) -> dict[str, str]:
    """Hash executable inputs, including the static lists that set personal data."""
    return {
        str(path.relative_to(package)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(package.rglob("*"))
        if path.is_file() and path.suffix in (".py", ".yaml", ".csv", ".txt")
        and "tests" not in path.parts and "configs" not in path.parts
    }


def semantic_dataset_id(config: Config, format_version: str, implementation_files: dict[str, str], calendar_fingerprint: str) -> str:
    """Identify source semantics independently of worker, buffer, and resource settings."""
    semantic_config = {key: value for key, value in config.as_dict().items() if key not in RUNTIME_KEYS}
    definitions = {key: value for key, value in implementation_files.items() if key.startswith("definitions/")}
    identity = {"config": semantic_config, "format_version": format_version, "definitions": definitions,
                "calendar_fingerprint": calendar_fingerprint}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def worker(config: Config, output: Path):
    """Run the generation stages, export, independent validation, and local reports."""
    import importlib.metadata
    import locale
    import resource

    from .activity import generate_activity
    from .customers import generate_prospects
    from .generation import FORMAT_VERSION, Generation
    from .market import Market
    from .population import build_population
    from .reference import generate_reference
    from .validation import validate

    locale.setlocale(locale.LC_TIME, "C")
    output = Path(output)
    package = Path(__file__).parent
    implementation_files = implementation_hashes(package)
    generation = Generation(config, output)
    timings = {}

    def phase(name, function, *args):
        """Measure and announce one named stage without changing its return value."""
        print(f"Starting {name}.", flush=True)
        started = time.monotonic()
        result = function(*args)
        timings[name] = time.monotonic() - started
        print(f"Completed {name} in {timings[name]:.6f} seconds.", flush=True)
        return result

    phase("reference", generate_reference, generation)
    market = phase("market", Market.build, config, output, generation)
    population = phase("population", build_population, config, output)
    activity = phase("activity", generate_activity, generation, market, population)
    prospects = phase("prospects", generate_prospects, generation, population)
    inventory = phase("export", generation.finish)
    validation = phase("validation", validate, output, config, inventory)
    write_json(output / "validation.json", validation)
    versions = {name: importlib.metadata.version(name) for name in
                ("polars", "numpy", "PyYAML", "exchange-calendars", "psutil")}
    measured = inventory_summary(inventory)
    offline = sum(measured["deliveries"][name]["bytes"] for name in ("initial", "daily"))
    manifest = {
        "status": "validated", "implementation": FORMAT_VERSION, "config": config.as_dict(),
        "implementation_files": implementation_files,
        "dataset_id": semantic_dataset_id(config, FORMAT_VERSION, implementation_files, market.fingerprint),
        "runtime": {"python": sys.version, **versions}, "calendar": market.calendar_metadata,
        "timings_seconds": timings, "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "maximum_generation_frame_rows": generation.output.maximum_frame_rows,
        "maximum_export_buffer_rows": generation.output.maximum_buffer_rows,
        "maximum_merge_inputs": generation.output.maximum_merge_inputs,
        "maximum_retained_run_paths": generation.output.maximum_retained_run_paths,
        "source_bytes": offline, "stream_bytes": measured["deliveries"]["stream"]["bytes"],
        "source_records": sum(item["rows"] for item in inventory if item["path"].startswith("offline/")),
        "stream_records": measured["deliveries"]["stream"]["rows"], "files": inventory, "validation": validation,
    }
    summary = {"config": config.as_dict(), "files": measured, "activity": activity, "prospects": prospects,
               "data_errors": {**generation.recording_summary, "measurements": validation["source_recording"]["errors"]},
               "trade_recording": validation["source_recording"]["trade_schemas"],
               "calendar": market.calendar_metadata, "runtime": {"python": sys.version, "dependencies": versions,
               "timings_seconds": timings,
               "peak_rss_bytes": manifest["peak_rss_bytes"], "maximum_export_buffer_rows": generation.output.maximum_buffer_rows,
               "maximum_merge_inputs": generation.output.maximum_merge_inputs,
               "maximum_retained_run_paths": generation.output.maximum_retained_run_paths}}
    write_json(output / "summary.json", summary)
    write_json(output / "manifest.json", manifest)
    print(f"Validated {manifest['source_records']} offline records and {manifest['stream_records']} stream records.", flush=True)


def compare(first: Path, second: Path):
    """Rehash saved sources and require identical semantic configuration and bytes."""
    roots = [Path(first), Path(second)]
    manifests = [json.loads((path / "manifest.json").read_text()) for path in roots]
    if any(manifest.get("status") != "validated" for manifest in manifests):
        raise ValueError("Both runs must have a validated manifest.")
    if manifests[0]["dataset_id"] != manifests[1]["dataset_id"]:
        raise ValueError("The runs do not use the same semantic configuration.")
    inventories = [{item["path"]: (item["rows"], item["bytes"], item["sha256"]) for item in manifest["files"]} for manifest in manifests]
    if inventories[0] != inventories[1]:
        raise ValueError("Source file contents differ between runs.")
    for root, inventory in zip(roots, inventories):
        actual_files = {str(path.relative_to(root)) for directory in ("offline", "stream")
                        for path in (root / directory).rglob("*") if path.is_file()}
        if actual_files != set(inventory):
            raise ValueError("Source file coverage differs from the manifest.")
        for relative, (_, size, digest) in inventory.items():
            path = root / relative
            if path.is_symlink() or ".." in Path(relative).parts or Path(relative).is_absolute():
                raise ValueError("An inventory path must identify a file within the run.")
            with path.open("rb") as stream:
                actual_digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual_digest != digest or path.stat().st_size != size:
                raise ValueError(f"Source contents no longer match the manifest: {path}")
    return {"identical_files": len(inventories[0]), "dataset_id": manifests[0]["dataset_id"]}
