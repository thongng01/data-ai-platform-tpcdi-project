"""Check transport API contracts, credential redaction, and dependency-free CLI help."""

from datetime import datetime, timezone
import json
import subprocess
import sys
from types import ModuleType

import pytest

from data.data_source.publisher import cli, transports
from data.data_source.publisher.dataset import CashMessage, SourceFile
from data.data_source.publisher.errors import DeliveryUncertain, PublisherError
from data.data_source.publisher.transports import KafkaTransport, S3Transport, Settings, TOPIC


@pytest.mark.parametrize("endpoint,bootstrap", [
    ("http://private:password@store:8333", "kafka:9092"),
    ("http://store:8333?token=private", "kafka:9092"),
    ("http://store:8333", "private:password@kafka:9092"),
    ("http://store:8333", "kafka:9092?token=private"),
    ("http://store:8333", "kafka:9092#private"),
    ("http://store:8333", "kafka"),
    (None, "kafka:9092"),
])
def test_destination_addresses_reject_credentials_and_bad_forms(endpoint, bootstrap):
    """Connection address validation must not repeat rejected private values."""
    with pytest.raises(PublisherError) as error:
        Settings(endpoint, bootstrap).validate()
    assert "private" not in str(error.value)


def test_settings_repr_and_destination_identity_exclude_credentials(settings):
    """Credential changes do not appear in repr or alter a stable destination ID."""
    private = Settings(settings.s3_endpoint, settings.kafka_bootstrap, {"sasl.password": "private-password"})
    assert "private-password" not in repr(private)
    assert private.destination_id == settings.destination_id


def test_offline_only_settings_need_no_kafka_and_have_separate_identity(settings):
    """Offline-only settings accept a missing Kafka address and never share full-delivery state."""
    offline = Settings(settings.s3_endpoint, None, offline_only=True).validate()
    ignored = Settings(settings.s3_endpoint, settings.kafka_bootstrap, offline_only=True)
    assert offline.destination_id == ignored.destination_id
    assert offline.destination_id != settings.destination_id
    with pytest.raises(PublisherError):
        Settings(settings.s3_endpoint, None).validate()


class FakeProducer:
    """Capture the actual confluent producer API calls without a running broker."""

    def __init__(self, config, logger=None):
        """Keep effective settings and pending callbacks for adapter assertions."""
        self.config = config
        self.logger = logger
        self.records = []
        self.callbacks = []
        self.callback_error = None
        self.unconfirmed = 0
        self.purged = False

    def produce(self, topic, **kwargs):
        """Store exact record arguments and the corresponding delivery callback."""
        self.records.append((topic, kwargs))
        self.callbacks.append(kwargs["on_delivery"])

    def poll(self, timeout):
        """Leave callbacks pending so finish_batch must explicitly flush them."""
        return None

    def flush(self, timeout):
        """Complete queued callbacks with a configured acknowledgment result."""
        for callback in self.callbacks:
            callback(self.callback_error, None)
        self.callbacks = []
        return self.unconfirmed

    def purge(self):
        """Record a shutdown that does not silently deliver pending messages."""
        self.purged = True


@pytest.fixture
def fake_kafka_module(monkeypatch):
    """Supply only the external producer class that the adapter uses."""
    module = ModuleType("confluent_kafka")
    module.Producer = FakeProducer
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    return module


def test_kafka_adapter_uses_raw_topic_decimal_key_and_delivery_time(settings, fake_kafka_module, monkeypatch):
    """Do not replace event time or reserialize the saved message value."""
    monkeypatch.setattr(transports.time, "time", lambda: 1000.123)
    kafka = KafkaTransport(settings)
    payload = b'{"ct_id":2,"ct_ca_id":1001,"ct_dts":"2019-01-01T00:00:00Z","ct_amt":"-4.20","ct_name":"Withdrawal","schema_version":1}'
    message = CashMessage(payload, 2, b"1001", datetime(2019, 1, 1, tzinfo=timezone.utc), len(payload) + 1)
    kafka.begin_batch()
    kafka.enqueue(message)
    kafka.finish_batch()
    topic, record = kafka.producer.records[0]
    assert topic == TOPIC == "brokerage.cash_transaction.raw"
    assert record["key"] == b"1001" and record["value"] is payload
    assert record["timestamp"] == 1000123
    assert kafka.producer.config["enable.idempotence"] is True
    assert kafka.producer.config["max.in.flight.requests.per.connection"] == 1
    kafka.close()
    assert kafka.producer.purged


@pytest.mark.parametrize("result", ["error", "timeout", "missing_callback"])
def test_kafka_callback_failure_is_not_a_success(settings, fake_kafka_module, result):
    """A partial queue flush or failed callback must not acknowledge a batch."""
    kafka = KafkaTransport(settings)
    kafka.begin_batch()
    message = CashMessage(b"{}", 2, b"1001", datetime(2019, 1, 1, tzinfo=timezone.utc), 3)
    kafka.enqueue(message)
    if result == "error":
        kafka.producer.callback_error = RuntimeError("private-broker-details")
    elif result == "timeout":
        kafka.producer.unconfirmed = 1
    else:
        kafka.producer.callbacks.clear()
    with pytest.raises(DeliveryUncertain) as error:
        kafka.finish_batch()
    assert "private-broker-details" not in str(error.value)


def test_kafka_protected_settings_cannot_disable_safety(settings, fake_kafka_module):
    """Private authentication settings cannot turn off acknowledgments or expand queues."""
    settings = Settings(settings.s3_endpoint, settings.kafka_bootstrap,
                        {"acks": "0", "enable.idempotence": False, "queue.buffering.max.messages": 10000000,
                         "log_level": 7, "sasl.password": "private-password"})
    kafka = KafkaTransport(settings)
    assert kafka.producer.config["acks"] == "all"
    assert kafka.producer.config["queue.buffering.max.messages"] == 256
    assert kafka.producer.config["log_level"] == 0
    assert kafka.producer.config["sasl.password"] == "private-password"


def test_kafka_setup_exception_is_safe(settings, fake_kafka_module):
    """Client constructor errors must not display the external connection settings."""
    def fail(*args, **kwargs):
        """Return a deliberately unsafe native client error."""
        raise RuntimeError("private-authentication-details")

    fake_kafka_module.Producer = fail
    with pytest.raises(PublisherError) as error:
        KafkaTransport(settings)
    assert "private-authentication-details" not in str(error.value)


def test_s3_adapter_uploads_only_exact_source_key(settings, monkeypatch, tmp_path):
    """Verify boto3 upload parameters and bounded, single-worker multipart settings."""
    captured = {}

    class ObjectClient:
        """Capture one object upload and pool cleanup."""

        def upload_fileobj(self, source, bucket, key, **options):
            """Read provided bytes and capture upload identity and metadata."""
            captured.update(bucket=bucket, key=key, content=source.read(), options=options)

        def close(self):
            """Record that the connection pool closed."""
            captured["closed"] = True

    def client(*args, **kwargs):
        """Capture connection and retry settings without using external credentials."""
        captured["client"] = kwargs
        return ObjectClient()

    def options(**kwargs):
        """Represent SDK configuration constructors as plain dictionaries."""
        return kwargs

    modules = {name: ModuleType(name) for name in ("boto3", "boto3.s3", "boto3.s3.transfer", "botocore", "botocore.config")}
    modules["boto3"].client = client
    modules["boto3.s3.transfer"].TransferConfig = options
    modules["botocore.config"].Config = options
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    s3 = S3Transport(settings, "a" * 64)
    path = tmp_path / "content"
    path.write_bytes(b"unchanged-offline-file\n")
    item = SourceFile("offline/daily/20190101/Trade_20190101.txt", 1, path.stat().st_size, "b" * 64)
    with path.open("rb") as source:
        s3.upload(item, source)
    assert captured["bucket"] == "upstream"
    assert captured["key"] == "daily/20190101/Trade_20190101.txt"
    assert captured["content"] == path.read_bytes()
    assert captured["options"]["Config"]["max_concurrency"] == 1
    assert captured["options"]["Config"]["use_threads"] is False
    assert captured["client"]["config"]["retries"]["total_max_attempts"] == 1
    assert captured["options"]["ExtraArgs"]["Metadata"]["publisher-source-sha256"] == item.sha256
    s3.close()
    assert captured["closed"]


@pytest.mark.parametrize("command", [[], ["check"], ["publish"]])
def test_cli_help_without_optional_sdks(command):
    """Every public help page works before transport dependencies are installed."""
    result = subprocess.run([sys.executable, "-m", "data.data_source.publisher", *command, "--help"],
                            text=True, capture_output=True)
    assert result.returncode == 0
    assert "usage:" in result.stdout and not result.stderr
    if command == ["publish"]:
        assert "--retry-uncertain" in result.stdout and "--daily-release" in result.stdout


def test_cli_check_saved_dataset(saved_dataset, capsys):
    """Preflight needs no destination settings, credentials, or live services."""
    assert cli.main(["check", str(saved_dataset)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "verified"


def test_cli_setup_error_redacts_private_values(saved_dataset, tmp_path, capsys, monkeypatch):
    """An unexpected SDK failure stays out of terminal output and delivery reports."""
    secret = "credential-that-must-never-be-reported"

    def fail(*args, **kwargs):
        """Simulate a client that includes credentials in its exception text."""
        raise RuntimeError(secret)

    monkeypatch.setattr(cli, "S3Transport", fail)
    state = tmp_path / "state"
    result = cli.main(["publish", str(saved_dataset), "--state", str(state),
                       "--s3-endpoint", "http://store:8333", "--kafka-bootstrap", "kafka:9092"])
    output = capsys.readouterr()
    assert result == 1 and secret not in output.out + output.err
    report = (state / "report.json").read_text()
    assert secret not in report and json.loads(report)["status"] == "failed"


def test_cli_environment_settings_are_external(saved_dataset, tmp_path, capsys, monkeypatch):
    """Use environment addresses and private Kafka settings without recording their values."""
    from .conftest import MemoryKafka, MemoryS3

    private = tmp_path / "private-settings.json"
    private.write_text(json.dumps({"sasl.password": "private-password"}))
    monkeypatch.setenv("PUBLISHER_S3_ENDPOINT", "http://store:8333")
    monkeypatch.setenv("PUBLISHER_KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("PUBLISHER_KAFKA_CONFIG", str(private))
    monkeypatch.setattr(cli, "S3Transport", lambda *arguments: MemoryS3())
    monkeypatch.setattr(cli, "KafkaTransport", lambda *arguments: MemoryKafka())
    state = tmp_path / "state"
    assert cli.main(["publish", str(saved_dataset), "--state", str(state)]) == 0
    output = capsys.readouterr()
    assert "private-password" not in output.out + output.err + (state / "report.json").read_text()
    assert "http://store:8333" not in output.out


def test_cli_offline_only_publishes_without_kafka_settings(saved_dataset, tmp_path, capsys, monkeypatch):
    """The offline-only flag needs only an S3 endpoint and reports that the stream was skipped."""
    from .conftest import MemoryS3

    def kafka_transport(*arguments):
        """Fail the test if the command builds a Kafka client."""
        raise AssertionError("offline-only command opened Kafka")

    monkeypatch.delenv("PUBLISHER_KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.setattr(cli, "S3Transport", lambda *arguments: MemoryS3())
    monkeypatch.setattr(cli, "KafkaTransport", kafka_transport)
    state = tmp_path / "state"
    assert cli.main(["publish", str(saved_dataset), "--state", str(state),
                     "--s3-endpoint", "http://store:8333", "--offline-only"]) == 0
    report = json.loads((state / "report.json").read_text())
    assert report["offline_only"] is True
    assert report["outcomes"]["acknowledged_messages"] == 0


def test_cli_through_date_records_a_partial_delivery(saved_dataset, tmp_path, monkeypatch):
    """The --through flag reaches the publisher and the report records the date."""
    from .conftest import MemoryS3

    monkeypatch.setattr(cli, "S3Transport", lambda *arguments: MemoryS3())
    state = tmp_path / "state"
    assert cli.main(["publish", str(saved_dataset), "--state", str(state), "--s3-endpoint", "http://store:8333",
                     "--offline-only", "--through", "2019-01-01"]) == 0
    report = json.loads((state / "report.json").read_text())
    assert report["status"] == "partial"
    assert report["through_date"] == "2019-01-01"


def test_cli_argument_errors_do_not_echo_unknown_private_values(capsys):
    """Even parser failures use fixed text rather than repeating user input."""
    with pytest.raises(SystemExit) as error:
        cli.main(["publish", "example", "--private-password-value"])
    assert error.value.code == 2
    output = capsys.readouterr()
    assert "private-password-value" not in output.err
