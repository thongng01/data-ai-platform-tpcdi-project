import json
import os
from pathlib import Path
import subprocess
import sys
import time

import psutil

from .config import Config


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _disk_bytes(root):
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


def run(config: Config, output: Path):
    output = output.absolute()
    partial = output.with_name(output.name + ".partial")
    if output.exists() or partial.exists() or output.is_symlink() or partial.is_symlink():
        raise FileExistsError("The output or partial directory already exists. Choose a new run directory.")
    partial.mkdir(parents=True)
    config_path = partial / "config.json"
    write_json(config_path, config.as_dict())
    environment = {**os.environ, "POLARS_MAX_THREADS": str(config.threads), "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
    started = time.monotonic()
    maximum_rss = 0
    maximum_disk = 0
    next_disk_check = 0.0
    failure = None
    with (partial / "worker.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "data.data_source.generator", "_worker", "--config", str(config_path), "--output", str(partial)],
            env=environment, stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            monitored = psutil.Process(process.pid)
            while process.poll() is None:
                elapsed = time.monotonic() - started
                try:
                    maximum_rss = max(maximum_rss, monitored.memory_info().rss)
                except psutil.NoSuchProcess:
                    pass
                if elapsed >= next_disk_check:
                    maximum_disk = max(maximum_disk, _disk_bytes(partial))
                    next_disk_check = elapsed + 2
                if maximum_rss > config.memory_limit_mb * 1024**2:
                    failure = "The sampled RSS exceeded the memory budget."
                elif maximum_disk > config.disk_limit_mb * 1024**2:
                    failure = "The sampled disk usage exceeded the disk budget."
                elif elapsed > config.timeout_seconds:
                    failure = "The run exceeded its time budget."
                if failure:
                    process.terminate()
                    break
                time.sleep(0.1)
            try:
                returncode = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                returncode = process.wait()
            if returncode != 0 and failure is None:
                failure = f"The worker exited with status {returncode}. See worker.log."
        except BaseException:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            write_json(partial / "failure.json", {"reason": "The supervisor was interrupted."})
            raise
    supervision = {"elapsed_seconds": time.monotonic() - started, "sampled_peak_rss_bytes": maximum_rss,
                   "sampled_peak_disk_bytes": maximum_disk, "memory_sampling_seconds": 0.1, "disk_sampling_seconds": 2}
    if failure:
        write_json(partial / "failure.json", {**supervision, "reason": failure})
        raise RuntimeError(f"{failure} Retained files: {partial}")
    manifest = json.loads((partial / "manifest.json").read_text())
    if manifest.get("status") != "validated":
        raise RuntimeError(f"The worker did not produce a validated manifest: {partial}")
    if manifest["peak_rss_bytes"] > config.memory_limit_mb * 1024**2 or _disk_bytes(partial) > config.disk_limit_mb * 1024**2:
        write_json(partial / "failure.json", {**supervision, "reason": "The completed worker exceeded a resource budget."})
        raise RuntimeError(f"The completed worker exceeded a resource budget. Retained files: {partial}")
    write_json(partial / "supervision.json", supervision)
    partial.rename(output)
    return manifest


def worker(config: Config, output: Path):
    import hashlib
    import locale
    import resource

    import numpy as np
    import polars as pl
    import yaml

    locale.setlocale(locale.LC_TIME, "C")
    package = Path(__file__).parent
    implementation_files = {
        str(path.relative_to(package)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(package.rglob("*"))
        if path.is_file() and path.suffix in (".py", ".yaml") and "tests" not in path.parts and "configs" not in path.parts
    }
    from .activity import generate_activity
    from .customers import generate_customers
    from .generation import Generation
    from .reference import generate_finwire, generate_market, generate_reference
    from .validation import validate

    generation = Generation(config, output)
    timings = {}
    for function in (generate_reference, generate_finwire, generate_customers, generate_market, generate_activity):
        print(f"Starting {function.__name__}.", flush=True)
        started = time.monotonic()
        function(generation)
        timings[function.__name__] = time.monotonic() - started
    started = time.monotonic()
    inventory = generation.finish()
    timings["export"] = time.monotonic() - started
    print("Validating source files.", flush=True)
    started = time.monotonic()
    validation = validate(output, config, inventory)
    timings["validation"] = time.monotonic() - started
    write_json(output / "validation.json", validation)
    runtime_keys = {"customer_batch_size", "merge_fan_in", "memory_limit_mb", "disk_limit_mb", "timeout_seconds", "threads"}
    semantic_config = {key: value for key, value in config.as_dict().items() if key not in runtime_keys}
    manifest = {
        "status": "validated", "implementation": "polars-poc-0.1.0", "config": config.as_dict(),
        "implementation_files": implementation_files,
        "reference_commit": "2ac319db6e62927c5d4f772cf5654ad8981f9ec4",
        "dataset_id": hashlib.sha256(json.dumps(semantic_config, sort_keys=True).encode()).hexdigest(),
        "runtime": {"python": sys.version, "polars": pl.__version__, "numpy": np.__version__, "pyyaml": yaml.__version__},
        "timings_seconds": timings, "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "maximum_generation_frame_rows": generation.output.maximum_frame_rows,
        "maximum_merge_inputs": generation.output.maximum_merge_inputs,
        "maximum_retained_run_paths": generation.output.maximum_retained_run_paths,
        "source_bytes": sum(item["bytes"] for item in inventory), "source_records": sum(item["rows"] for item in inventory),
        "files": inventory, "validation": validation,
        "limitations": ["This POC does not reproduce DIGen bytes or distributions.",
                        "The closed-form account and trade schedules are simplified POC rules.",
                        "The POC does not implement injected faults or validate forecast signal.",
                        "The supervisor samples resource use; it does not impose a kernel memory limit."],
    }
    write_json(output / "manifest.json", manifest)
    print(f"Validated {manifest['source_records']} source records.", flush=True)


def compare(first: Path, second: Path):
    import hashlib

    manifests = [json.loads((path / "manifest.json").read_text()) for path in (first, second)]
    if manifests[0]["dataset_id"] != manifests[1]["dataset_id"]:
        raise ValueError("The runs do not use the same semantic configuration.")
    inventories = [{item["path"]: (item["rows"], item["bytes"], item["sha256"]) for item in manifest["files"]} for manifest in manifests]
    if inventories[0] != inventories[1]:
        raise ValueError("Source file contents differ between runs.")
    for root, inventory in zip((first, second), inventories):
        actual_files = {str(path.relative_to(root / "sources")) for path in (root / "sources").rglob("*") if path.is_file()}
        if actual_files != set(inventory):
            raise ValueError("Source file coverage differs from the manifest.")
        for relative, (_, size, digest) in inventory.items():
            path = root / "sources" / relative
            with path.open("rb") as stream:
                actual_digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual_digest != digest or path.stat().st_size != size:
                raise ValueError(f"Source contents no longer match the manifest: {path}")
    return {"identical_files": len(inventories[0]), "dataset_id": manifests[0]["dataset_id"]}
