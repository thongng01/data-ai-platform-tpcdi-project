"""Publish a real small generator result through controlled S3 and Kafka adapters."""

import json

from data.data_source.generator.config import Config
from data.data_source.generator.runner import run
from data.data_source.publisher.dataset import Dataset
from data.data_source.publisher.delivery import publish
from data.data_source.publisher.journal import Journal
from .conftest import MemoryKafka, MemoryS3


def test_real_saved_generator_output_publishes_without_regeneration(tmp_path, settings):
    """Generator and publisher agree on manifests, cash identity, and exact bytes."""
    config = Config(initial_start="2019-06-28", initial_end="2019-06-30",
                    daily_start="2019-07-01", daily_end="2019-07-02", scale_factor=.002,
                    securities=2, companies=1, workers=1, threads=1,
                    annual_trade_rates=(250, 250, 250, 250, 250),
                    annual_deposit_rates=(365, 365, 365, 365, 365))
    root = tmp_path / "generated"
    manifest = run(config, root)
    dataset = Dataset(root)
    verified = dataset.verify()
    assert verified["offline_bytes"] == manifest["source_bytes"]
    assert verified["stream_bytes"] == manifest["stream_bytes"] > 0
    kafka, s3 = MemoryKafka(), MemoryS3()
    with Journal(tmp_path / "state", dataset, settings.destination_id) as journal:
        report = publish(dataset, journal, lambda: s3, lambda: kafka)
    assert report["outcomes"]["acknowledged_messages"] == manifest["stream_records"]
    assert report["outcomes"]["acknowledged_offline_bytes"] == manifest["source_bytes"]
    assert all((root / "offline" / key).read_bytes() == content for key, content in s3.deliveries)
    saved_payloads = [line for path in sorted((root / "stream/cash_transactions").glob("*.jsonl"))
                      for line in path.read_bytes().splitlines()]
    assert [message.payload for message in kafka.deliveries] == saved_payloads
    assert all(message.key == str(json.loads(message.payload)["ct_ca_id"]).encode("ascii") for message in kafka.deliveries)
    assert Dataset(root).verify() == verified
