"""DP1 entry point: run the whole ingest for one batch.

    docker compose --profile compute run --rm spark-jobs \
        /opt/pipelines/dp1/main.py --batch-id Batch1

Three steps, in order, each reporting what it did:

  1. publish   copy the generator's local output into the landing bucket
  2. ingest    landing -> bronze, unmodified
  3. transform bronze -> silver, typed and deduplicated

The job writes a run report as JSON to `misc/` so the docs can quote measured
numbers rather than configured ones.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time

from pyspark.sql import SparkSession

if __package__ in (None, ""):  # pragma: no cover - spark-submit entry path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dp1.bronze_to_silver import bronze_to_silver
from dp1.config import config_from_args
from dp1.landing_to_bronze import landing_to_bronze
from dp1.publish_landing import upload_batch

LOG = logging.getLogger("dp1.main")

# Where the generator writes. Only used by the publish step, and overridden with
# GENERATOR_ROOT. Inside a container, misc/ is mounted at /opt/misc.
DEFAULT_GENERATOR_ROOT = "/opt/misc/sample_run"


def build_session(app_name: str) -> SparkSession:
    """Create the SparkSession DP1 runs on.

    The session relies on infra/spark/spark-defaults.conf for the metastore URI,
    the warehouse location and the S3 credentials, all mounted into every
    container that runs a job. Nothing about the platform is set here, so there
    is one place to change it.

    Args:
        app_name: The Spark application name, which shows in the Spark UI.

    Returns:
        The active SparkSession.
    """
    session = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    session.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return session


STOP_TIMEOUT_SECONDS = 90


def stop_session(session, timeout: int = STOP_TIMEOUT_SECONDS) -> bool:
    """Stop a SparkSession without letting the process hang on it.

    Closing the event log uploads it to object storage, and that close can block
    for minutes. It did here, on every run: the job had written all ten silver
    tables and reported nothing, and Airflow would have seen a task that never
    finished rather than a task that succeeded. So the stop runs on its own
    thread and the process leaves on a deadline whether or not it finished.

    Args:
        session: The SparkSession to stop.
        timeout: Seconds to wait for a clean stop.

    Returns:
        True when the session stopped cleanly inside the timeout.
    """
    if session is None:
        return True

    finished = threading.Event()

    def _stop():
        try:
            session.stop()
        finally:
            finished.set()

    worker = threading.Thread(target=_stop, name="spark-stop", daemon=True)
    worker.start()
    worker.join(timeout)

    if finished.is_set():
        return True

    LOG.warning(
        "SparkSession.stop() did not return within %ds. The work is committed; "
        "leaving anyway so the run is not reported as hung.",
        timeout,
    )
    return False


def main(argv=None) -> int:
    """Run DP1 for one batch.

    Args:
        argv: Argument list, or None to read sys.argv.

    Returns:
        A process exit code: 0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    config = config_from_args(argv)

    started = time.time()
    report = {"batch_id": config.batch_id, "batch_seq": config.batch_seq}
    session = None

    try:
        if not _flag_set(argv, "--skip-upload"):
            root = os.environ.get("GENERATOR_ROOT", DEFAULT_GENERATOR_ROOT)
            # Publish before the Spark session exists: it is a plain S3 copy and
            # has no business holding a cluster slot while it runs.
            report["publish"] = upload_batch(root, config.batch_id, config)

        session = build_session(f"dp1-{config.batch_id}")
        report["bronze"] = landing_to_bronze(session, config)
        report["silver"] = bronze_to_silver(session, config)
        report["seconds"] = round(time.time() - started, 1)
        report["ok"] = True
    except Exception as exc:  # noqa: BLE001 - the report is the point
        LOG.exception("DP1 failed for %s", config.batch_id)
        report["ok"] = False
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["seconds"] = round(time.time() - started, 1)
    finally:
        report["stopped_cleanly"] = stop_session(session)

    # The run report is written where the caller asked, falling back to /tmp.
    # A job container runs as uid 185, so a bind mount owned by the host user is
    # not writable and the report would be lost along with the container.
    destination = os.environ.get("DP1_REPORT_PATH", "/tmp/dp1_report.json")
    try:
        with open(destination, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str)
        LOG.info("report written to %s", destination)
    except OSError as exc:
        LOG.warning("could not write the report to %s: %s", destination, exc)

    print(json.dumps(report, indent=2, default=str))
    sys.stdout.flush()

    exit_code = 0 if report.get("ok") else 1

    # Leave hard if the session did not stop, so a hung close cannot turn a
    # completed run into a timed-out task.
    #
    # The JVM has to go too. spark-submit runs this script as a child of the
    # JVM and the JVM is the container's PID 1, so exiting Python on its own
    # leaves the container running, holding its executors on the worker. That
    # is the same starvation as finding 8.2, arrived at from the other
    # direction. `pipelines/_exit_check.py` is the one-line check of this
    # mechanism.
    if report.get("stopped_cleanly"):
        return exit_code

    try:
        os.kill(os.getppid(), signal.SIGTERM)
        time.sleep(1)
    except (OSError, ProcessLookupError) as exc:
        LOG.warning("could not signal the JVM: %s", exc)
    os._exit(exit_code)


def _flag_set(argv, name: str) -> bool:
    """Return whether a boolean flag appears in an argument list.

    spark-submit passes the script's own arguments after the script path, and
    they do not arrive as a parsed namespace, so the check is textual.

    Args:
        argv: The argument list.
        name: The flag to look for.

    Returns:
        True when the flag is present.
    """
    return argv is not None and name in argv


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))