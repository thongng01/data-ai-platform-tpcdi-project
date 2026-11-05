"""Pace every delivery against one monotonic simulated replay clock."""

from datetime import datetime
import math
import time

from .errors import PublisherError


class ReplayClock:
    """Map elapsed simulated seconds to wall seconds; bulk mode never sleeps."""

    def __init__(self, mode="bulk", speed=1.0, monotonic=time.monotonic, sleep=time.sleep):
        """Set the speed and injectable time functions without starting the clock."""
        if mode not in ("bulk", "replay") or not math.isfinite(speed) or speed <= 0:
            raise PublisherError("Use bulk or replay mode and a finite positive speed.")
        self.mode = mode
        self.speed = speed
        self.monotonic = monotonic
        self.sleep = sleep
        self.origin = None
        self.started = None
        self.wall_anchor = None

    def wait(self, simulated: datetime):
        """Wait until this simulated time is due, starting at the first pending item."""
        if self.mode == "bulk":
            return
        if self.origin is None:
            self.origin = simulated
            self.started = self.monotonic()
            self.wall_anchor = time.time()
        target = self.started + (simulated - self.origin).total_seconds() / self.speed
        # Short waits make keyboard interruption responsive during a slow replay.
        while (remaining := target - self.monotonic()) > 0:
            self.sleep(min(remaining, 1.0))
