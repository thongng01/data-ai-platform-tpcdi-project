"""Check exact delivery, bounded batching, resumable progress, and lost acknowledgments."""

from datetime import datetime, timezone
import json

import pytest

from data.data_source.publisher.clock import ReplayClock
from data.data_source.publisher.dataset import CashMessage, Dataset
from data.data_source.publisher.delivery import message_batches, publish
from data.data_source.publisher.errors import DeliveryUncertain, PublisherError
from data.data_source.publisher.journal import Journal
from .conftest import MemoryKafka, MemoryS3, make_dataset, refresh_file


class FakeTime:
    """Advance monotonic time only when the replay clock requests a wait."""

    def __init__(self):
        """Start a clock without real wall-clock sleeps."""
        self.now = 100.0
        self.waits = []

    def monotonic(self):
        """Return controlled elapsed time."""
        return self.now

    def sleep(self, duration):
        """Advance controlled elapsed time by the requested wait."""
        self.waits.append(duration)
        self.now += duration


def run_saved(root, state, settings, s3, kafka, **options):
    """Publish a saved fixture with isolated state and mock transport factories."""
    dataset = Dataset(root)
    with Journal(state, dataset, settings.destination_id) as journal:
        return publish(dataset, journal, lambda: s3, lambda: kafka, **options)


def test_clean_delivery_preserves_keys_payloads_and_generated_files(saved_dataset, tmp_path, settings):
    """Publish only offline files and exact saved JSON bytes, with decimal account keys."""
    dataset = Dataset(saved_dataset)
    before = {path: (saved_dataset / path).read_bytes() for path in dataset.files}
    s3, kafka = MemoryS3(), MemoryKafka()
    result = run_saved(saved_dataset, tmp_path / "state", settings, s3, kafka)
    expected_objects = {path.removeprefix("offline/"): data for path, data in before.items() if path.startswith("offline/")}
    assert dict(s3.deliveries) == expected_objects
    assert len(s3.deliveries) == len(expected_objects)
    expected_payloads = b"".join(data for path, data in sorted(before.items()) if path.startswith("stream/")).splitlines()
    assert [message.payload for message in kafka.deliveries] == expected_payloads
    assert [message.key for message in kafka.deliveries] == [b"1001", b"1002", b"1001"]
    assert s3.closed and kafka.closed
    assert result["outcomes"]["acknowledged_objects"] == len(expected_objects)
    assert result["outcomes"]["acknowledged_messages"] == 3
    assert result["outcomes"]["completed_stream_files"] == 2
    assert result["outcomes"]["uncertain_files"] == 0
    assert result["status"] == "complete"
    assert result["latest_clock_segment"]["mode"] == "bulk"
    assert all((saved_dataset / path).read_bytes() == content for path, content in before.items())


def test_successful_resume_sends_nothing_again(saved_dataset, tmp_path, settings):
    """A repeated command reuses acknowledged identities rather than resending them."""
    state = tmp_path / "state"
    run_saved(saved_dataset, state, settings, MemoryS3(), MemoryKafka())
    s3, kafka = MemoryS3(), MemoryKafka()
    report = run_saved(saved_dataset, state, settings, s3, kafka)
    assert not s3.deliveries and not kafka.deliveries
    assert report["outcomes"]["acknowledged_messages"] == 3
    assert report["clock_segments"] == 2


def test_object_lost_ack_blocks_default_resume_and_reuses_identity(saved_dataset, tmp_path, settings):
    """An uncertain PUT requires explicit retry and keeps the source key and bytes."""
    state = tmp_path / "state"
    original = MemoryS3(fail_key="initial/CashTransaction.txt")
    with pytest.raises(DeliveryUncertain):
        run_saved(saved_dataset, state, settings, original, MemoryKafka())
    report = json.loads((state / "report.json").read_text())
    assert report["status"] == "uncertain" and report["outcomes"]["uncertain_files"] == 1
    blocked = MemoryS3()
    with pytest.raises(DeliveryUncertain, match="retry-uncertain"):
        run_saved(saved_dataset, state, settings, blocked, MemoryKafka())
    assert not blocked.deliveries
    retried = MemoryS3()
    report = run_saved(saved_dataset, state, settings, retried, MemoryKafka(), retry_uncertain=True)
    assert original.deliveries[-1] == retried.deliveries[0]
    assert report["outcomes"]["uncertain_retries"] == 1
    assert "private-transport-secret" not in (state / "report.json").read_text()


def test_partial_batch_ack_retains_last_complete_cursor(tmp_path, settings):
    """Only complete acknowledged batches advance; retry repeats an uncertain batch."""
    root = make_dataset(tmp_path / "dataset", messages=600)
    state = tmp_path / "state"
    first = MemoryKafka(fail_batch=2)
    with pytest.raises(DeliveryUncertain):
        run_saved(root, state, settings, MemoryS3(), first)
    report = json.loads((state / "report.json").read_text())
    assert report["outcomes"]["acknowledged_messages"] == 256
    assert report["outcomes"]["uncertain_message_upper_bound"] == 256
    assert report["unresolved"][0]["first_ct_id"] == 258
    assert report["unresolved"][0]["end_ct_id"] == 513
    assert report["unresolved"][0]["acknowledged_source_offset"] > 0
    second = MemoryKafka()
    report = run_saved(root, state, settings, MemoryS3(), second, retry_uncertain=True)
    assert second.deliveries[0].ct_id == 258
    assert [len(batch) for batch in second.batches] == [256, 88]
    assert report["outcomes"]["acknowledged_messages"] == 600
    assert report["outcomes"]["possible_duplicate_messages"] == 256
    assert report["outcomes"]["uncertain_files"] == 0
    assert all(len(batch) <= 256 for batch in first.batches + second.batches)


def test_exception_after_one_remote_acceptance_marks_entire_batch_uncertain(saved_dataset, tmp_path, settings):
    """Some unsent members may share an uncertain range, so the report uses an upper bound."""
    state = tmp_path / "state"
    kafka = MemoryKafka(fail_id=2)
    with pytest.raises(DeliveryUncertain):
        run_saved(saved_dataset, state, settings, MemoryS3(), kafka)
    report = json.loads((state / "report.json").read_text())
    assert len(kafka.deliveries) == 1
    assert report["outcomes"]["acknowledged_messages"] == 0
    assert report["outcomes"]["uncertain_message_upper_bound"] == 3
    assert "private-kafka-secret" not in (state / "report.json").read_text()


def test_interrupted_send_is_uncertain(saved_dataset, tmp_path, settings):
    """Keyboard interruption does not turn a possibly accepted send into an unsent item."""
    class InterruptedKafka(MemoryKafka):
        """Simulate user interruption after one accepted message."""

        def enqueue(self, message):
            """Accept one message and interrupt the publisher."""
            super().enqueue(message)
            raise KeyboardInterrupt

    state = tmp_path / "state"
    with pytest.raises(KeyboardInterrupt):
        run_saved(saved_dataset, state, settings, MemoryS3(), InterruptedKafka())
    report = json.loads((state / "report.json").read_text())
    assert report["status"] == "interrupted"
    assert report["outcomes"]["uncertain_files"] == 1


def test_local_ack_commit_failure_does_not_claim_remote_failure(saved_dataset, journal):
    """A remote success followed by local storage failure still requires a reviewed retry."""
    def fail_commit(*arguments, **keywords):
        """Model storage failure immediately after a successful remote upload."""
        raise OSError("private-disk-details")

    journal.acknowledge = fail_commit
    s3 = MemoryS3()
    with pytest.raises(DeliveryUncertain):
        publish(Dataset(saved_dataset), journal, lambda: s3, MemoryKafka)
    assert len(s3.deliveries) == 1
    report = json.loads((journal.directory / "report.json").read_text())
    assert report["outcomes"]["acknowledged_objects"] == 0
    assert report["outcomes"]["uncertain_files"] == 1
    assert "private-disk-details" not in json.dumps(report)


def test_all_preflight_finishes_before_client_setup(saved_dataset, journal):
    """A damaged file late in the dataset prevents all early remote deliveries."""
    (saved_dataset / "stream/cash_transactions/20190102.jsonl").write_bytes(b"corrupt")
    opened = []

    def client():
        """Record client construction that must not happen on preflight failure."""
        opened.append(True)
        return MemoryS3()

    with pytest.raises(PublisherError, match="hash"):
        publish(Dataset(saved_dataset), journal, client, MemoryKafka)
    assert not opened
    assert json.loads((journal.directory / "report.json").read_text())["status"] == "failed"


def test_source_change_after_preflight_never_reaches_upload(saved_dataset, journal):
    """Snapshot hash verification catches edits made while the clients are initialized."""
    dataset = Dataset(saved_dataset)
    source = saved_dataset / "offline/initial/CashTransaction.txt"
    s3 = MemoryS3()

    def create_client():
        """Change the first source after preflight but before its snapshot is selected."""
        source.write_bytes(b"changed-after-verification")
        return s3

    with pytest.raises(PublisherError, match="changed|grew"):
        publish(dataset, journal, create_client, MemoryKafka)
    assert not s3.deliveries


def test_snapshot_is_stable_during_upload(saved_dataset, journal):
    """A live edit after snapshot selection cannot alter bytes sent for that object."""
    dataset = Dataset(saved_dataset)
    path = "offline/initial/CashTransaction.txt"
    original = (saved_dataset / path).read_bytes()

    def change_live_file(item):
        """Mutate only the already-selected source, not its private snapshot."""
        if item.path == path:
            (saved_dataset / path).write_bytes(b"live-source-edited")

    s3 = MemoryS3(during_upload=change_live_file)
    publish(dataset, journal, lambda: s3, MemoryKafka)
    assert dict(s3.deliveries)["initial/CashTransaction.txt"] == original


def test_second_client_setup_failure_closes_first(saved_dataset, journal):
    """Partial client setup does not leak an already-open object-store pool."""
    s3 = MemoryS3()

    def fail_kafka():
        """Fail before delivery with an exception that must not enter the report."""
        raise RuntimeError("private-client-settings")

    with pytest.raises(RuntimeError):
        publish(Dataset(saved_dataset), journal, lambda: s3, fail_kafka)
    assert s3.closed and not s3.deliveries
    assert "private-client-settings" not in (journal.directory / "report.json").read_text()


@pytest.mark.parametrize("release", ["start", "end"])
def test_one_replay_clock_orders_daily_release_and_stream(saved_dataset, tmp_path, settings, release):
    """Replay uses one simulated timeline, not separate daily and message timers."""
    fake = FakeTime()
    clock = ReplayClock("replay", speed=86400, monotonic=fake.monotonic, sleep=fake.sleep)
    report = run_saved(saved_dataset, tmp_path / "state", settings, MemoryS3(), MemoryKafka(),
                       clock=clock, daily_release=release)
    assert clock.origin == datetime(2019, 1, 1, tzinfo=timezone.utc)
    assert fake.now - 100 == pytest.approx(2.0 if release == "end" else 1.0)
    segment = report["latest_clock_segment"]
    assert segment["daily_release"] == release and segment["simulated_anchor"] == clock.origin.isoformat()
    assert segment["wall_anchor"] is not None


def test_restart_clock_anchor_skips_acknowledged_history(tmp_path, settings):
    """Resume begins at its next pending event without rewriting event timestamps."""
    root = make_dataset(tmp_path / "dataset", messages=600)
    state = tmp_path / "state"
    with pytest.raises(DeliveryUncertain):
        run_saved(root, state, settings, MemoryS3(), MemoryKafka(fail_batch=2))
    fake = FakeTime()
    clock = ReplayClock("replay", speed=86400, monotonic=fake.monotonic, sleep=fake.sleep)
    kafka = MemoryKafka()
    report = run_saved(root, state, settings, MemoryS3(), kafka, clock=clock, retry_uncertain=True)
    assert clock.origin == datetime(2019, 1, 1, 0, 4, 16, tzinfo=timezone.utc)
    assert kafka.deliveries[0].event_time == clock.origin
    assert report["clock_segments"] == 2


def test_bulk_has_no_clock_waits():
    """Bulk mode never calls the supplied sleep function."""
    fake = FakeTime()
    clock = ReplayClock("bulk", speed=1, monotonic=fake.monotonic, sleep=fake.sleep)
    clock.wait(datetime(2025, 1, 1, tzinfo=timezone.utc))
    assert not fake.waits and clock.origin is None


@pytest.mark.parametrize("speed", [0, -1, float("inf"), float("nan")])
def test_invalid_replay_speed_fails(speed):
    """Reject impossible replay speeds before opening clients."""
    with pytest.raises(PublisherError):
        ReplayClock("replay", speed)


def test_batch_memory_bounds_cover_message_count_and_payload_bytes():
    """The batch iterator limits both row count and aggregate payload allocation."""
    message = CashMessage(b"x", 2, b"1", datetime(2019, 1, 1, tzinfo=timezone.utc), 1)
    assert [len(batch) for batch in message_batches(message for _ in range(1000))] == [256, 256, 256, 232]
    large = CashMessage(b"x" * (3 * 1024 * 1024), 2, b"1", message.event_time, 1)
    assert [len(batch) for batch in message_batches(large for _ in range(5))] == [2, 2, 1]


def test_clean_delivery_does_not_repair_source_recording_errors(saved_dataset, tmp_path, settings):
    """Source-recording errors remain source bytes because clean describes delivery only."""
    relative = "offline/daily/20190101/Customer_20190101.txt"
    content = b"I|1|1001|tax-id|ACTV|Example|Customer|||9|1800-01-01\n"
    refresh_file(saved_dataset, relative, content, rows=1)
    s3 = MemoryS3()
    run_saved(saved_dataset, tmp_path / "state", settings, s3, MemoryKafka())
    assert dict(s3.deliveries)[relative.removeprefix("offline/")] == content
