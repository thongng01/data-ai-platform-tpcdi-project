import argparse
import json
from pathlib import Path

from .config import Config
from .runner import compare, run, worker


def main():
    parser = argparse.ArgumentParser(description="Generate and validate bounded brokerage source datasets.")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "_worker"):
        subparser = commands.add_parser(command)
        subparser.add_argument("--config", type=Path, required=True)
        subparser.add_argument("--output", type=Path, required=True)
    validation = commands.add_parser("validate")
    validation.add_argument("output", type=Path)
    comparison = commands.add_parser("compare")
    comparison.add_argument("first", type=Path)
    comparison.add_argument("second", type=Path)
    arguments = parser.parse_args()
    if arguments.command == "_worker":
        worker(Config.load(arguments.config), arguments.output)
    elif arguments.command == "run":
        manifest = run(Config.load(arguments.config), arguments.output)
        print(json.dumps({"output": str(arguments.output), "source_bytes": manifest["source_bytes"],
                          "stream_bytes": manifest["stream_bytes"], "peak_rss_bytes": manifest["peak_rss_bytes"]}, indent=2))
    elif arguments.command == "validate":
        from .validation import validate
        manifest = json.loads((arguments.output / "manifest.json").read_text())
        result = validate(arguments.output, Config(**manifest["config"]), manifest["files"])
        from .runner import write_json
        write_json(arguments.output / "validation.json", result)
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(compare(arguments.first, arguments.second), indent=2))


if __name__ == "__main__":
    main()
