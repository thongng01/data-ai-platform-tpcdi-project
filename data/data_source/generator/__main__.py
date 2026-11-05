"""Expose bounded generation, validation, comparison, and sampled signal checks."""

import argparse
import json
from pathlib import Path

from .config import Config
from .runner import compare, run, worker


def main():
    """Parse one command and invoke its owning generation or verification stage."""
    parser = argparse.ArgumentParser(description="Generate and validate bounded brokerage source datasets.")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "_worker"):
        subparser = commands.add_parser(command)
        subparser.add_argument("--config", type=Path, required=True)
        subparser.add_argument("--output", type=Path, required=True)
    validation = commands.add_parser("validate")
    validation.add_argument("output", type=Path)
    signal = commands.add_parser("signal-check", help="Measure the attrition signal on an exported small run.")
    signal.add_argument("output", type=Path)
    signal.add_argument("--sample-accounts", type=int, default=512)
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
    elif arguments.command == "signal-check":
        from .signal_check import check_signal
        manifest = json.loads((arguments.output / "manifest.json").read_text())
        if manifest.get("status") != "validated":
            raise ValueError("A signal check requires a validated exported run.")
        result = check_signal(arguments.output, Config(**manifest["config"]), arguments.sample_accounts)
        print(json.dumps(result, indent=2))
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
