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

from .checkpoints import Checkpoints, atomic_json
from .config import Config


RUNTIME_KEYS = {"customer_group_size", "workers", "merge_fan_in", "export_rows", "memory_limit_mb",
                "disk_limit_mb", "timeout_seconds", "threads"}


def write_json(path, value):
    """Write stable, readable JSON and create its parent directory."""
    atomic_json(path, value)


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


def run(config: Config, output: Path | None = None, *, resume: Path | None = None, skip_validation=False):
    """Supervise new or matching saved work and retain evidence on every failure."""
    if resume is not None:
        if skip_validation:
            raise ValueError("A resumed run performs full validation; do not use --skip-validation.")
        partial = Path(resume).absolute()
        if partial.is_symlink() or not partial.is_dir() or not (partial / "_checkpoints/context.json").is_file():
            raise ValueError("Resume requires a retained run with checkpoint context.")
        destination = partial.with_name(partial.name.removesuffix(".partial"))
        if output is not None and Path(output).absolute() != destination:
            raise ValueError("The resume output must be the retained run's final path.")
        output = destination
        if output != partial and (output.exists() or output.is_symlink()):
            raise FileExistsError("The final output already exists.")
        if output == partial:
            manifest = json.loads((partial / "manifest.json").read_text())
            if manifest.get("status") != "generated-unvalidated":
                raise ValueError("A final resume path must contain a generated-unvalidated dataset.")
        Checkpoints(partial, config, implementation_hashes(Path(__file__).parent), RUNTIME_KEYS)
    else:
        if output is None:
            raise ValueError("A new run requires --output.")
        output = Path(output).absolute()
        partial = output.with_name(output.name + ".partial")
        if output.exists() or partial.exists() or output.is_symlink() or partial.is_symlink():
            raise FileExistsError("The output or partial directory already exists. Choose a new run directory.")
    if not 1 <= config.workers <= 8:
        raise ValueError("A run permits between one and eight business worker processes.")
    # One worker executes in the coordinator. Parallel runs keep a separate
    # coordinator plus at most `workers` pool processes in a parallel phase.
    process_limit = 1 if config.workers == 1 else config.workers + 1
    if resume is None:
        partial.mkdir(parents=True)
        Checkpoints(partial, config, implementation_hashes(Path(__file__).parent), RUNTIME_KEYS)
    config_path = partial / "config.json"
    write_json(config_path, config.as_dict())
    from .output import native_environment

    # Set private engine controls before the new process imports Polars. The
    # output module owns these version-specific names and conservative budgets.
    environment = {**os.environ, **native_environment(config, partial),
                   "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
    started = time.monotonic()
    maximum_rss = 0
    maximum_disk = 0
    maximum_temporary_disk = 0
    maximum_workers = 0
    next_disk_check = 0.0
    failure = None
    process = None
    try:
        with (partial / "worker.log").open("a" if resume is not None else "w", encoding="utf-8") as log:
            command = [sys.executable, "-m", "data.data_source.generator", "_worker", "--config", str(config_path), "--output", str(partial)]
            if skip_validation:
                command.append("--skip-validation")
            process = subprocess.Popen(
                command,
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
                elif maximum_workers > process_limit:
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
        expected_status = "generated-unvalidated" if skip_validation else "validated"
        if manifest.get("status") != expected_status:
            raise RuntimeError(f"The worker did not produce a {expected_status} manifest.")
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
        (partial / "failure.json").unlink(missing_ok=True)
        if partial != output:
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


def validate_saved(output: Path, config: Config | None = None):
    """Validate saved bytes and promote both reports only after every check passes."""
    from .validation import validate

    output = Path(output)
    manifest = json.loads((output / "manifest.json").read_text())
    summary = json.loads((output / "summary.json").read_text())
    result = validate(output, config or Config(**manifest["config"]), manifest["files"])
    write_json(output / "validation.json", result)
    summary["status"] = manifest["status"] = "validated"
    manifest["validation"] = result
    summary["data_errors"]["measurements"] = result["source_recording"]["errors"]
    summary["trade_recording"] = result["source_recording"]["trade_schemas"]
    write_json(output / "summary.json", summary)
    write_json(output / "manifest.json", manifest)
    return result


def worker(config: Config, output: Path, *, skip_validation=False):
    """Reuse matching completed steps, export sources, and validate by default."""
    import importlib.metadata
    import locale
    import resource

    import numpy as np

    from .activity import generate_activity
    from .customers import generate_prospects
    from .generation import FORMAT_VERSION, Generation
    from .market import Market
    from .population import Population, build_population
    from .reference import generate_reference

    locale.setlocale(locale.LC_TIME, "C")
    output = Path(output)
    implementation_files = implementation_hashes(Path(__file__).parent)
    checkpoints = Checkpoints(output, config, implementation_files, RUNTIME_KEYS)
    runtime = checkpoints.read("runtime")
    timings = runtime["result"]["timings_seconds"] if runtime else {}
    phase_cpu = runtime["result"]["phase_cpu_seconds"] if runtime else {}

    def cpu_times():
        """Count this process and reaped children, including native worker threads."""
        records = [resource.getrusage(kind) for kind in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN)]
        return {"user": sum(item.ru_utime for item in records),
                "system": sum(item.ru_stime for item in records)}

    def phase(name, function, *args):
        """Measure completed work in this attempt and keep earlier completed timings."""
        print(f"Starting {name}.", flush=True)
        before = cpu_times()
        started = time.monotonic()
        result = function(*args)
        timings[name] = time.monotonic() - started
        after = cpu_times()
        phase_cpu[name] = {key: after[key] - before[key] for key in before}
        checkpoints.save("runtime", {"timings_seconds": timings, "phase_cpu_seconds": phase_cpu})
        print(f"Completed {name} in {timings[name]:.6f} seconds.", flush=True)
        return result

    def producer(name, function):
        """Complete one independently sealed producer before reusing its result."""
        receipt = checkpoints.part(name)
        if receipt is not None:
            return receipt["result"]
        checkpoints.discard_part(name)
        generation = Generation(config, output, part_name=name)

        def generate_and_seal():
            """Include final buffer flushes and the completion seal in phase timing."""
            result = function(generation)
            generation.finish(completion=checkpoints.completion(result))
            return result

        return phase(name, generate_and_seal)

    exported = checkpoints.read("export")
    if exported is None:
        producer("reference", generate_reference)
        if checkpoints.part("market") is None:
            shutil.rmtree(output / "_work" / "market", ignore_errors=True)
        producer("market", lambda generation: Market.build(config, output, generation).calendar_metadata)
        market = Market(output, config)
        saved_population = checkpoints.read("population")
        if saved_population is None:
            def plan_population():
                """Save immutable planned rows before groups change actual lifetimes."""
                population = build_population(config, output)
                initial = output / "_work" / "population-initial"
                shutil.rmtree(initial, ignore_errors=True)
                shutil.copytree(population.path, initial)
                return population

            population = phase("population", plan_population)
            checkpoints.save("population", {"slot_stride": population.slot_stride})
        else:
            path = output / "_work" / "population"
            population = Population(np.load(path / "customers.npy", mmap_mode="r"),
                                    np.load(path / "accounts.npy", mmap_mode="r"), path,
                                    saved_population["result"]["slot_stride"])
        # The coordinator owns no earlier phase's buffered records. Its empty
        # seal still provides the final scheduled-group discovery and export.
        coordinator = checkpoints.part("coordinator")
        if coordinator is None:
            checkpoints.discard_part("coordinator")
        generation = Generation(config, output, reuse=coordinator is not None)
        activity = phase("activity", generate_activity, generation, market, population)
        prospects = producer("prospects", lambda producer_generation:
                             generate_prospects(producer_generation, population))
        # Export is one restartable step. Remove only its final source trees and
        # audit; sealed parts and hidden records stay intact for the new export.
        for name in ("offline", "stream"):
            shutil.rmtree(output / name, ignore_errors=True)
        (output / "summary" / "data_errors.jsonl").unlink(missing_ok=True)
        if not generation.output.closed:
            generation.output.seal(completion=checkpoints.completion())
        inventory = phase("export", generation.finish)
        versions = {name: importlib.metadata.version(name) for name in
                    ("polars", "numpy", "PyYAML", "exchange-calendars", "psutil")}
        measured = inventory_summary(inventory)
        offline = sum(measured["deliveries"][name]["bytes"] for name in ("initial", "daily"))
        export_metrics = {**generation.output.metrics, "backend": "polars",
                          "ignored_legacy_settings": {"merge_fan_in": config.merge_fan_in}}
        manifest = {
            "status": "generated-unvalidated", "implementation": FORMAT_VERSION, "config": config.as_dict(),
            "implementation_files": implementation_files,
            "dataset_id": semantic_dataset_id(config, FORMAT_VERSION, implementation_files, market.fingerprint),
            "runtime": {"python": sys.version, **versions}, "calendar": market.calendar_metadata,
            "timings_seconds": timings, "phase_cpu_seconds": phase_cpu,
            "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "export": export_metrics, "source_bytes": offline,
            "stream_bytes": measured["deliveries"]["stream"]["bytes"],
            "source_records": sum(item["rows"] for item in inventory if item["path"].startswith("offline/")),
            "stream_records": measured["deliveries"]["stream"]["rows"], "files": inventory, "validation": None,
        }
        summary = {"status": "generated-unvalidated", "config": config.as_dict(), "files": measured,
                   "activity": activity, "prospects": prospects,
                   "data_errors": {**generation.recording_summary, "measurements": None}, "trade_recording": None,
                   "calendar": market.calendar_metadata, "runtime": {"python": sys.version, "dependencies": versions,
                   "timings_seconds": timings, "phase_cpu_seconds": phase_cpu,
                   "peak_rss_bytes": manifest["peak_rss_bytes"], "export": export_metrics}}
        write_json(output / "summary.json", summary)
        write_json(output / "manifest.json", manifest)
        checkpoints.save("export")
    if not skip_validation and checkpoints.read("validation") is None:
        phase("validation", validate_saved, output, config)
        checkpoints.save("validation")
    manifest = json.loads((output / "manifest.json").read_text())
    summary = json.loads((output / "summary.json").read_text())
    manifest["config"] = summary["config"] = config.as_dict()
    manifest["timings_seconds"] = summary["runtime"]["timings_seconds"] = timings
    manifest["phase_cpu_seconds"] = summary["runtime"]["phase_cpu_seconds"] = phase_cpu
    manifest["peak_rss_bytes"] = max(manifest["peak_rss_bytes"], resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)
    summary["runtime"]["peak_rss_bytes"] = manifest["peak_rss_bytes"]
    write_json(output / "summary.json", summary)
    write_json(output / "manifest.json", manifest)
    print(f"{manifest['status']}: {manifest['source_records']} offline records and {manifest['stream_records']} stream records.", flush=True)


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
