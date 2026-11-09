"""Build saved source fixtures and in-memory remote adapters for publisher tests."""

from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest

from data.data_source.publisher.dataset import Dataset, expected_paths
from data.data_source.publisher.journal import Journal
from data.data_source.publisher.transports import Settings


@pytest.fixture
def saved_dataset(tmp_path):
    """Create a two-day saved dataset with an initial-to-stream cash sequence."""
    return make_dataset(tmp_path / "dataset")


def make_dataset(root, messages=3, config=None):
    """Write minimal offline bytes and valid ordered JSON with complete file cadence."""
    config = config or {"initial_start": "2018-12-30", "initial_end": "2018-12-31",
                        "daily_start": "2019-01-01", "daily_end": "2019-01-02"}
    paths, start, _ = expected_paths(config)
    inventory = []
    for relative in sorted(paths):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        content = b""
        rows = 0
        if relative == "offline/initial/CashTransaction.txt":
            content = b"1|1001|2018-12-31 08:00:00|100.00|Deposit\n"
            rows = 1
        elif relative == f"stream/cash_transactions/{start:%Y%m%d}.jsonl":
            lines = []
            for index in range(messages):
                stamp = datetime.combine(start, datetime.min.time(), timezone.utc) + timedelta(seconds=index)
                value = {"ct_id": index + 2, "ct_ca_id": 1001 + index % 2,
                         "ct_dts": stamp.strftime("%Y-%m-%dT%H:%M:%SZ"), "ct_amt": "-4.20",
                         "ct_name": "Withdrawal", "schema_version": 1}
                lines.append(json.dumps(value, separators=(",", ":")).encode() + b"\n")
            content = b"".join(lines)
            rows = messages
        path.write_bytes(content)
        inventory.append({"path": relative, "rows": rows, "bytes": len(content),
                          "sha256": hashlib.sha256(content).hexdigest()})
    manifest = {"status": "validated", "dataset_id": "a" * 64, "config": config, "files": inventory}
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


def refresh_file(root, relative, content, rows=None):
    """Update one file and its manifest so contract tests do not fail only on hash."""
    (root / relative).write_bytes(content)
    manifest = json.loads((root / "manifest.json").read_text())
    item = next(item for item in manifest["files"] if item["path"] == relative)
    item.update(bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
    if rows is not None:
        item["rows"] = rows
    (root / "manifest.json").write_text(json.dumps(manifest))


class MemoryS3:
    """Capture exact uploaded keys and bytes, including failure after acceptance."""

    def __init__(self, fail_key=None, during_upload=None):
        """Set an optional failing key and source-mutation callback."""
        self.deliveries = []
        self.fail_key = fail_key
        self.during_upload = during_upload
        self.closed = False

    def upload(self, item, source):
        """Accept bytes first so a later exception simulates a lost acknowledgment."""
        if self.during_upload:
            self.during_upload(item)
        self.deliveries.append((item.object_key, source.read()))
        if self.fail_key == item.object_key:
            raise RuntimeError("private-transport-secret")

    def close(self):
        """Record adapter cleanup without sending data."""
        self.closed = True


class MemoryKafka:
    """Capture bounded batches and simulate partial acceptance or missing callbacks."""

    def __init__(self, fail_batch=None, fail_id=None):
        """Choose the batch or accepted message whose acknowledgment is lost."""
        self.deliveries = []
        self.batches = []
        self.current = []
        self.fail_batch = fail_batch
        self.fail_id = fail_id
        self.closed = False

    def begin_batch(self):
        """Start one bounded in-memory batch."""
        self.current = []

    def enqueue(self, message):
        """Accept the original bytes before an optional simulated send error."""
        self.current.append(message)
        self.deliveries.append(message)
        if message.ct_id == self.fail_id:
            raise RuntimeError("private-kafka-secret")

    def finish_batch(self):
        """Fail after acceptance when this batch has an uncertain acknowledgment."""
        self.batches.append(self.current)
        if len(self.batches) == self.fail_batch:
            raise TimeoutError("private-kafka-secret")

    def close(self):
        """Record adapter cleanup without sending data."""
        self.closed = True


@pytest.fixture
def settings():
    """Return explicit nonprivate destinations for journal identity checks."""
    return Settings("http://object-store:8333", "kafka:9092")


@pytest.fixture
def journal(saved_dataset, tmp_path, settings):
    """Open and close one dataset-bound delivery journal."""
    with Journal(tmp_path / "state", Dataset(saved_dataset), settings.destination_id) as value:
        yield value
