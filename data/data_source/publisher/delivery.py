"""Deliver verified files and saved messages with bounded acknowledgment batches."""

from contextlib import ExitStack
from datetime import timedelta

from .clock import ReplayClock
from .dataset import cash_messages
from .errors import DeliveryUncertain, PublisherError
from .transports import MAX_BATCH_BYTES, MAX_BATCH_MESSAGES


def message_batches(messages):
    """Hold at most 256 messages and 8 MiB of payload before checkpointing."""
    batch = []
    size = 0
    for message in messages:
        if batch and (len(batch) >= MAX_BATCH_MESSAGES or size + len(message.payload) > MAX_BATCH_BYTES):
            yield batch
            batch = []
            size = 0
        batch.append(message)
        size += len(message.payload)
    if batch:
        yield batch


def _close_transport(transport):
    """Attempt local cleanup without hiding an earlier delivery outcome."""
    try:
        transport.close()
    except Exception:
        # Closing a connection does not undo acknowledged sends. Any in-flight
        # sends already have an uncertain checkpoint before cleanup starts.
        pass


def deliver_object(dataset, journal, transport, item, retry_uncertain):
    """Upload one immutable verified snapshot and commit its acknowledgment."""
    row = journal.check_retry(item, retry_uncertain)
    if row["status"] == "acked":
        return
    with dataset.snapshot(item, journal.directory) as snapshot:
        journal.begin(item, item.rows, item.size)
        try:
            transport.upload(item, snapshot)
            journal.acknowledge(item)
        except BaseException as error:
            journal.uncertain(item)
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            raise DeliveryUncertain("Object acknowledgment is uncertain. Review report.json before retrying.") from None


def deliver_stream(dataset, journal, transport, item, day, clock, retry_uncertain):
    """Resume one saved daily stream at its last fully acknowledged batch offset."""
    row = journal.check_retry(item, retry_uncertain)
    if row["status"] == "acked":
        return
    if not 0 <= row["offset"] <= item.size or not 0 <= row["confirmed_rows"] <= item.rows:
        raise PublisherError("Stream checkpoint cursor is outside its source file.")
    with dataset.snapshot(item, journal.directory) as snapshot:
        messages = cash_messages(snapshot, day, row["offset"])
        for batch in message_batches(messages):
            transport.begin_batch()
            journal.begin(item, len(batch), sum(len(message.payload) for message in batch), batch[0].ct_id, batch[-1].ct_id)
            try:
                for message in batch:
                    clock.wait(message.event_time)
                    transport.enqueue(message)
                transport.finish_batch()
                journal.acknowledge(item, batch[-1].end_offset, batch[-1].ct_id)
            except BaseException as error:
                # Some callbacks can succeed before another fails. Without a whole
                # acknowledged batch, retry can repeat up to this batch's size.
                journal.uncertain(item)
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                raise DeliveryUncertain("Stream batch acknowledgment is uncertain. Review report.json before retrying.") from None
    journal.complete_stream(item)


def _deliver_daily_objects(dataset, journal, s3, day, clock, daily_release, retry_uncertain):
    """Schedule pending complete daily extracts with the same clock as the stream."""
    for item in dataset.daily_files(day):
        if journal.entry(item)["status"] != "acked":
            clock.wait(dataset.daily_time(day, daily_release))
            deliver_object(dataset, journal, s3, item, retry_uncertain)


def publish(dataset, journal, s3_factory, kafka_factory, clock=None, daily_release="end", retry_uncertain=False,
            offline_only=False, through=None):
    """Preflight all sources, then publish initial files and each date in order.

    With ``offline_only``, only offline files go to S3. The Kafka client never
    opens and saved cash transaction messages stay unsent, so a platform that
    has no streaming services yet can still receive every source file.

    With ``through`` (a date), delivery stops after that daily date and the
    report status is ``partial``. The day before the first daily date delivers
    the initial files only. A later run with the same state and a later date
    skips acknowledged files and continues, so the platform can receive its
    history a few days at a time.
    """
    clock = clock or ReplayClock()
    if daily_release not in ("start", "end"):
        raise PublisherError("Daily release must be start or end.")
    last_day = dataset.daily_end if through is None else through
    if not dataset.daily_start - timedelta(days=1) <= last_day <= dataset.daily_end:
        raise PublisherError("The through date must fall between the initial load's last date and the last daily date.")
    segment = journal.start_segment(clock, daily_release, offline_only, through)
    status = "failed"
    error_code = None
    try:
        dataset.verify()
        unresolved = journal.connection.execute(
            "SELECT 1 FROM deliveries WHERE status IN ('sending', 'uncertain') LIMIT 1"
        ).fetchone()
        if unresolved and not retry_uncertain:
            raise DeliveryUncertain("Remote delivery is uncertain. Review report.json, then use --retry-uncertain if resends are acceptable.")
        with ExitStack() as resources:
            # Factories delay dependency loading and network setup until every file
            # has passed preflight. Opening Kafka must not strand an S3 client.
            s3 = s3_factory()
            resources.callback(_close_transport, s3)
            kafka = None if offline_only else kafka_factory()
            if kafka is not None:
                resources.callback(_close_transport, kafka)
            for item in dataset.initial_files():
                deliver_object(dataset, journal, s3, item, retry_uncertain)
            day = dataset.daily_start
            while day <= last_day:
                if daily_release == "start":
                    _deliver_daily_objects(dataset, journal, s3, day, clock, daily_release, retry_uncertain)
                if not offline_only:
                    stream = dataset.files[f"stream/cash_transactions/{day:%Y%m%d}.jsonl"]
                    deliver_stream(dataset, journal, kafka, stream, day, clock, retry_uncertain)
                if daily_release == "end":
                    _deliver_daily_objects(dataset, journal, s3, day, clock, daily_release, retry_uncertain)
                day += timedelta(days=1)
        status = "complete" if last_day == dataset.daily_end else "partial"
    except DeliveryUncertain:
        status = "uncertain"
        error_code = "remote_acknowledgment_uncertain"
        raise
    except (KeyboardInterrupt, SystemExit):
        status = "interrupted"
        error_code = "interrupted"
        raise
    except Exception:
        error_code = "local_or_setup_failure"
        raise
    finally:
        journal.finish_segment(segment, clock, status)
        report = journal.report(status, error_code)
    return report
