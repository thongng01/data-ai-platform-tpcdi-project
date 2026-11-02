import argparse
from pathlib import Path

from .config import Config
from .runner import compare, run, write_json


def benchmark(configurations: list[Path], output: Path):
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
                "customers": manifest["config"]["customers"], "historical_days": manifest["config"]["historical_days"],
                "customer_batch_size": manifest["config"]["customer_batch_size"],
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
    parser = argparse.ArgumentParser(description="Run sequential POC scale and repeatability measurements.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--configs", type=Path, nargs="+", required=True)
    arguments = parser.parse_args()
    benchmark(arguments.configs, arguments.output)


if __name__ == "__main__":
    main()
