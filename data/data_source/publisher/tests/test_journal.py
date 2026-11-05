"""Protect checkpoint ownership and preserve ambiguous delivery after process restart."""

import stat

import pytest

from data.data_source.publisher.dataset import Dataset
from data.data_source.publisher.errors import DeliveryUncertain, PublisherError
from data.data_source.publisher.journal import Journal
from data.data_source.publisher.transports import Settings
from .conftest import refresh_file


def test_checkpoint_binds_destination_and_source_bytes(saved_dataset, tmp_path, settings):
    """Do not reuse prior acknowledgments for a different destination or recorder output."""
    state = tmp_path / "state"
    dataset = Dataset(saved_dataset)
    with Journal(state, dataset, settings.destination_id):
        pass
    other = Settings("http://different-store:8333", "kafka:9092")
    with pytest.raises(PublisherError, match="identity"):
        Journal(state, dataset, other.destination_id)
    refresh_file(saved_dataset, "offline/initial/Trade.txt", b"different-recorded-bytes\n", rows=1)
    changed = Dataset(saved_dataset)
    assert changed.dataset_id == dataset.dataset_id and changed.fingerprint != dataset.fingerprint
    with pytest.raises(PublisherError, match="identity"):
        Journal(state, changed, settings.destination_id)


def test_only_one_publisher_can_hold_state(saved_dataset, journal, settings):
    """The process lock prevents two workers from sending the same pending cursor."""
    with pytest.raises(PublisherError, match="Another publisher"):
        Journal(journal.directory, Dataset(saved_dataset), settings.destination_id)


def test_state_stays_outside_dataset_and_private(saved_dataset, tmp_path, settings):
    """Local delivery evidence must not enter generated sources or permit broad reads."""
    dataset = Dataset(saved_dataset)
    with pytest.raises(PublisherError, match="outside"):
        Journal(saved_dataset / "publisher-state", dataset, settings.destination_id)
    with Journal(tmp_path / "state", dataset, settings.destination_id) as journal:
        journal.report("ready")
        for name in ("delivery.sqlite", "publisher.lock", "report.json"):
            mode = stat.S_IMODE((journal.directory / name).stat().st_mode)
            assert mode == 0o600
        assert stat.S_IMODE(journal.directory.stat().st_mode) == 0o700


@pytest.mark.parametrize("name", ["delivery.sqlite", "publisher.lock", "report.json.tmp"])
def test_state_file_symlinks_cannot_overwrite_external_files(saved_dataset, tmp_path, settings, name):
    """Never follow a state-file link to an unrelated private file."""
    state = tmp_path / "state"
    state.mkdir()
    target = tmp_path / "outside"
    target.write_text("keep-private-content")
    (state / name).symlink_to(target)
    with pytest.raises(OSError):
        with Journal(state, Dataset(saved_dataset), settings.destination_id) as journal:
            journal.report("ready")
    assert target.read_text() == "keep-private-content"


def test_sending_checkpoint_recovers_as_uncertain(saved_dataset, tmp_path, settings):
    """A process lost after begin cannot claim that the network send never happened."""
    dataset = Dataset(saved_dataset)
    state = tmp_path / "state"
    item = dataset.files["stream/cash_transactions/20190101.jsonl"]
    with Journal(state, dataset, settings.destination_id) as journal:
        journal.begin(item, 3, item.size - 3, first_id=2, last_id=4)
    with Journal(state, dataset, settings.destination_id) as journal:
        assert journal.entry(item)["status"] == "uncertain"
        with pytest.raises(DeliveryUncertain):
            journal.check_retry(item, False)
        report = journal.report("uncertain")
        assert report["outcomes"]["acknowledged_messages"] == 0
        assert report["unresolved"][0]["first_ct_id"] == 2


def test_checkpoint_journal_grows_by_files_not_individual_messages(saved_dataset, journal):
    """Retain one cursor per saved file rather than one history row per transaction."""
    dataset = Dataset(saved_dataset)
    item = dataset.files["stream/cash_transactions/20190101.jsonl"]
    for index in range(3):
        journal.begin(item, 1, 100, first_id=index + 2, last_id=index + 2)
        journal.acknowledge(item, offset=(index + 1) * 101, last_id=index + 2)
    assert journal.connection.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 1
    assert journal.entry(item)["confirmed_rows"] == 3
