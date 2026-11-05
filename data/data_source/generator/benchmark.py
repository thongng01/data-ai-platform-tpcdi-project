"""Measure sequential generator runs and compare runs with equal dataset identities."""

import argparse
from pathlib import Path

from .config import Config
from .runner import compare, run, write_json


def benchmark(configurations: list[Path], output: Path):
    """Run each configuration in a new directory and save measurements after each run.

    Runs with equal semantic dataset identities also undergo byte comparison.
    A failed run stays in the summary, then the exception stops the sequence.
    """
    output.mkdir(parents=True, exist_ok=False)
    results = []
    semantic_runs = {}
    for configuration in configurations:
        destination = output / configuration.stem
        print(f"Running {configuration.stem}.", flush=True)
        try:
            manifest = run(Config.load(configuration), destination)
            generation_seconds = sum(value for name, value in manifest["timings_seconds"].items() if name != "validation")
            result = {
                "name": configuration.stem, "status": "validated", "source_bytes": manifest["source_bytes"],
                "source_records": manifest["source_records"], "peak_rss_bytes": manifest["peak_rss_bytes"],
                "generation_seconds": generation_seconds, "validation_seconds": manifest["timings_seconds"]["validation"],
                "source_mib_per_generation_second": manifest["source_bytes"] / 1024**2 / generation_seconds,
                "scale_factor": manifest["config"]["scale_factor"], "customers_per_unit": manifest["config"]["customers_per_unit"],
                "customer_group_size": manifest["config"]["customer_group_size"], "workers": manifest["config"]["workers"],
                "stream_bytes": manifest["stream_bytes"], "stream_records": manifest["stream_records"],
                "timings_seconds": manifest["timings_seconds"], "supervision": manifest["supervision"],
            }
            identity = manifest["dataset_id"]
            if identity in semantic_runs:
                result["repeatability"] = compare(semantic_runs[identity], destination)
            semantic_runs[identity] = destination
            results.append(result)
            write_json(output / "summary.json", results)
            print(f"Validated {configuration.stem}: {result['source_bytes']} bytes; peak RSS {result['peak_rss_bytes']} bytes.", flush=True)
        except Exception as error:
            results.append({"name": configuration.stem, "status": "failed", "error": str(error)})
            write_json(output / "summary.json", results)
            raise
    return results


def main():
    """Read configuration paths and the output directory from command-line arguments."""
    parser = argparse.ArgumentParser(description="Run sequential scale and byte-repeatability measurements.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--configs", type=Path, nargs="+", required=True)
    arguments = parser.parse_args()
    benchmark(arguments.configs, arguments.output)


if __name__ == "__main__":
    main()
