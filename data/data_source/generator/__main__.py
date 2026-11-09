"""Expose bounded generation, validation, comparison, and sampled signal checks."""

import argparse
import json
from pathlib import Path

from .config import Config
from .runner import compare, run, validate_saved, worker


def main():
    """Parse one command and invoke its owning generation or verification stage."""
    parser = argparse.ArgumentParser(description="Generate and validate bounded brokerage source datasets.")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "_worker"):
        subparser = commands.add_parser(command)
        subparser.add_argument("--config", type=Path, required=command == "_worker")
        subparser.add_argument("--output", type=Path, required=command == "_worker")
        subparser.add_argument("--skip-validation", action="store_true",
                               help="Export unchanged-code repeat runs without the full validator.")
        if command == "run":
            subparser.add_argument("--resume", type=Path,
                                   help="Reuse matching completed steps from a retained run; then validate.")
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
        worker(Config.load(arguments.config), arguments.output, skip_validation=arguments.skip_validation)
    elif arguments.command == "run":
        if arguments.config is None and arguments.resume is None:
            parser.error("A new run requires --config and --output.")
        config_path = arguments.config or arguments.resume / "config.json"
        manifest = run(Config.load(config_path), arguments.output, resume=arguments.resume,
                       skip_validation=arguments.skip_validation)
        output = arguments.output or arguments.resume.with_name(arguments.resume.name.removesuffix(".partial"))
        print(json.dumps({"output": str(output), "status": manifest["status"], "source_bytes": manifest["source_bytes"],
                          "stream_bytes": manifest["stream_bytes"], "peak_rss_bytes": manifest["peak_rss_bytes"]}, indent=2))
    elif arguments.command == "signal-check":
        from .signal_check import check_signal
        manifest = json.loads((arguments.output / "manifest.json").read_text())
        if manifest.get("status") != "validated":
            raise ValueError("A signal check requires a validated exported run.")
        result = check_signal(arguments.output, Config(**manifest["config"]), arguments.sample_accounts)
        print(json.dumps(result, indent=2))
    elif arguments.command == "validate":
        print(json.dumps(validate_saved(arguments.output), indent=2))
    else:
        print(json.dumps(compare(arguments.first, arguments.second), indent=2))


if __name__ == "__main__":
    main()
