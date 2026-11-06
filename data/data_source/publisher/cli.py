"""Expose source preflight and clean publication without displaying private settings."""

import argparse
import json
import os
from pathlib import Path
import sys

from .clock import ReplayClock
from .dataset import Dataset, read_json
from .delivery import publish
from .errors import PublisherError
from .journal import Journal
from .transports import KafkaTransport, S3Transport, Settings


class SafeArgumentParser(argparse.ArgumentParser):
    """Do not repeat unrecognized argument values that could contain credentials."""

    def error(self, message):
        """Return a fixed usage error instead of echoing the supplied value."""
        self.exit(2, "Invalid publisher command arguments. Use --help for accepted arguments.\n")


def parser():
    """Build component commands without importing optional transport SDKs."""
    root = SafeArgumentParser(description="Deliver saved source files and cash transactions without regeneration.")
    commands = root.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="Verify manifest, cadence, source hashes, and saved stream.")
    check.add_argument("dataset", type=Path)
    send = commands.add_parser("publish", help="Publish the clean profile with durable acknowledgment checkpoints.")
    send.add_argument("dataset", type=Path)
    send.add_argument("--state", type=Path, required=True, help="Private local state directory outside the dataset.")
    send.add_argument("--profile", choices=("clean",), default="clean")
    for command in (check, send):
        command.add_argument("--allow-unvalidated", action="store_true",
                             help="Accept a generated-unvalidated dataset and record this override; keep all preflight checks.")
    send.add_argument("--mode", choices=("bulk", "replay"), default="bulk", help="Bulk sends without clock waits.")
    send.add_argument("--speed", type=float, default=1.0, help="Simulated seconds per wall second in replay mode.")
    send.add_argument("--daily-release", choices=("start", "end"), default="end",
                      help="Release a daily extract at the start or end of its represented UTC date.")
    send.add_argument("--retry-uncertain", action="store_true",
                      help="Allow same-identity resends that can duplicate remote effects.")
    send.add_argument("--s3-endpoint", default=os.environ.get("PUBLISHER_S3_ENDPOINT"),
                      help="S3-compatible endpoint, or PUBLISHER_S3_ENDPOINT; never include credentials.")
    send.add_argument("--kafka-bootstrap", default=os.environ.get("PUBLISHER_KAFKA_BOOTSTRAP"),
                      help="Comma-separated host:port addresses, or PUBLISHER_KAFKA_BOOTSTRAP.")
    send.add_argument("--kafka-config", type=Path, default=os.environ.get("PUBLISHER_KAFKA_CONFIG"),
                      help="Private external JSON client settings for Kafka authentication and TLS.")
    return root


def main(argv=None):
    """Run a safe command, emit a report, and return a conventional exit status."""
    arguments = parser().parse_args(argv)
    try:
        dataset = Dataset(arguments.dataset, allow_unvalidated=arguments.allow_unvalidated)
        if arguments.command == "check":
            report = dataset.verify()
        else:
            clock = ReplayClock(arguments.mode, arguments.speed)
            config = read_json(Path(arguments.kafka_config), limit=64 * 1024) if arguments.kafka_config else {}
            settings = Settings(arguments.s3_endpoint, arguments.kafka_bootstrap, config,
                                os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))).validate()
            with Journal(arguments.state, dataset, settings.destination_id) as journal:
                report = publish(dataset, journal, lambda: S3Transport(settings, dataset.dataset_id),
                                 lambda: KafkaTransport(settings), clock, arguments.daily_release, arguments.retry_uncertain)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except PublisherError as error:
        print(str(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Publisher interrupted. Review the local report before retrying.", file=sys.stderr)
        return 130
    except Exception:
        # SDK exceptions can contain credentials, headers, or complete connection
        # strings. Never print their text or chain in a terminal or report.
        print("Publisher failed because a local operation or client setup failed. Check external settings and local storage.", file=sys.stderr)
        return 1
