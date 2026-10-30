"""Check that the exit path kills the JVM and not just the Python driver.

spark-submit runs this script as a child of the JVM, so `os.getppid()` is the
JVM. If only Python exits, the container's PID 1 is still the JVM and the
container never goes away, holding its executors and starving the next run.

Run with:
    docker compose --profile compute run --rm spark-jobs /opt/pipelines/_exit_check.py
"""

import os
import signal
import sys
import time


def main() -> int:
    """Report the parent PID, kill it, and leave immediately."""
    parent = os.getppid()
    print("python pid:", os.getpid())
    print("parent pid (the JVM):", parent)
    print("killing the JVM and leaving")
    sys.stdout.flush()

    os.kill(parent, signal.SIGTERM)
    time.sleep(0.5)
    os._exit(7)


if __name__ == "__main__":
    sys.exit(main())
