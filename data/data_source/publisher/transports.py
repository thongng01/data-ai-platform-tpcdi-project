"""Adapt S3-compatible storage and Kafka with external credentials and safe errors."""

from dataclasses import dataclass, field
import hashlib
import json
import logging
import time
from urllib.parse import urlsplit

from .errors import DeliveryUncertain, PublisherError

BUCKET = "upstream"
TOPIC = "brokerage.cash_transaction.raw"
MAX_BATCH_MESSAGES = 256
MAX_BATCH_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, repr=False)
class Settings:
    """Keep destination settings in memory and credentials out of reports and repr."""

    s3_endpoint: str
    kafka_bootstrap: str
    kafka_config: dict = field(default_factory=dict, repr=False)
    region: str = "us-east-1"

    def validate(self):
        """Reject credentials embedded in destination addresses before opening clients."""
        try:
            parsed = urlsplit(self.s3_endpoint)
            if (parsed.scheme not in ("http", "https") or not parsed.hostname or
                    parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment):
                raise ValueError
            parsed.port
            for address in self.kafka_bootstrap.split(","):
                target = urlsplit("//" + address.strip())
                if (not target.hostname or target.port is None or target.username is not None or
                        target.password is not None or target.path or target.query or target.fragment):
                    raise ValueError
            if not isinstance(self.kafka_config, dict):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise PublisherError("Use explicit S3 and Kafka addresses without embedded credentials.") from None
        return self

    @property
    def destination_id(self):
        """Bind checkpoints to opaque destination identity, never credential values."""
        self.validate()
        target = {"s3_endpoint": self.s3_endpoint.rstrip("/"), "bucket": BUCKET,
                  "kafka_bootstrap": self.kafka_bootstrap, "topic": TOPIC}
        return hashlib.sha256(json.dumps(target, sort_keys=True).encode()).hexdigest()


class S3Transport:
    """Upload one verified file snapshot with at most one multipart worker."""

    def __init__(self, settings, dataset_id):
        """Load the optional SDK and use its external AWS credential chain."""
        settings.validate()
        try:
            import boto3
            from boto3.s3.transfer import TransferConfig
            from botocore.config import Config
        except ImportError:
            raise PublisherError("Install the publisher requirements in a separate environment to use S3.") from None
        try:
            self.client = boto3.client(
                "s3", endpoint_url=settings.s3_endpoint, region_name=settings.region,
                config=Config(connect_timeout=10, read_timeout=60, max_pool_connections=2,
                              retries={"total_max_attempts": 1}, s3={"addressing_style": "path"}),
            )
            self.transfer = TransferConfig(
                multipart_threshold=8 * 1024 * 1024, multipart_chunksize=8 * 1024 * 1024,
                max_concurrency=1, use_threads=False,
            )
        except Exception:
            raise PublisherError("Cannot initialize the S3 client. Check external settings and credentials.") from None
        self.dataset_id = dataset_id

    def upload(self, item, source):
        """Upload exact bytes to the source key; never treat an ETag as a content hash."""
        try:
            self.client.upload_fileobj(
                source, BUCKET, item.object_key, Config=self.transfer,
                ExtraArgs={"Metadata": {"publisher-source-sha256": item.sha256, "publisher-dataset-id": self.dataset_id}},
            )
        except Exception:
            raise DeliveryUncertain("S3 acknowledgment is uncertain. Check the local delivery report.") from None

    def close(self):
        """Close the client's connection pool without making additional deliveries."""
        self.client.close()


class KafkaTransport:
    """Enqueue one bounded batch and require a successful callback for every message."""

    def __init__(self, settings):
        """Load the optional producer and enforce bounded queues and ordered sends."""
        settings.validate()
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise PublisherError("Install the publisher requirements in a separate environment to use Kafka.") from None
        config = dict(settings.kafka_config)
        config.update({
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True, "acks": "all", "max.in.flight.requests.per.connection": 1,
            "queue.buffering.max.messages": MAX_BATCH_MESSAGES,
            "queue.buffering.max.kbytes": MAX_BATCH_BYTES // 1024,
            "delivery.timeout.ms": 30000, "socket.timeout.ms": 10000,
            "log_level": 0, "error_cb": self._ignore_background_error,
        })
        # Native client logs can include addresses and authentication details. Keep
        # them out of shared reports, even when the application's root logger runs.
        logger = logging.Logger("publisher.kafka.private")
        logger.addHandler(logging.NullHandler())
        logger.propagate = False
        try:
            self.producer = Producer(config, logger=logger)
        except Exception:
            raise PublisherError("Cannot initialize the Kafka client. Check external settings and credentials.") from None
        self.begin_batch()

    @staticmethod
    def _ignore_background_error(error):
        """Keep native background error text private; delivery callbacks decide success."""
        return None

    def begin_batch(self):
        """Reset the bounded acknowledgment counters before enqueueing a new batch."""
        self.sent = 0
        self.received = 0
        self.failed = False

    def _acknowledge(self, error, message):
        """Count callbacks without retaining message payloads or raw error text."""
        self.received += 1
        self.failed = self.failed or error is not None

    def enqueue(self, message):
        """Keep source key and payload bytes, and stamp actual delivery wall time."""
        try:
            self.producer.produce(TOPIC, key=message.key, value=message.payload,
                                  timestamp=int(time.time() * 1000), on_delivery=self._acknowledge)
            self.sent += 1
            self.producer.poll(0)
        except Exception:
            raise DeliveryUncertain("Kafka send acknowledgment is uncertain. Check the local delivery report.") from None

    def finish_batch(self):
        """Require all callbacks before advancing the durable source-file cursor."""
        try:
            remaining = self.producer.flush(35)
            if remaining or self.received != self.sent or self.failed:
                raise DeliveryUncertain("Kafka batch acknowledgment is uncertain. Check the local delivery report.")
        except DeliveryUncertain:
            raise
        except Exception:
            raise DeliveryUncertain("Kafka batch acknowledgment is uncertain. Check the local delivery report.") from None

    def close(self):
        """Purge pending client work instead of quietly sending more during shutdown."""
        self.producer.purge()
        self.producer.poll(0)
