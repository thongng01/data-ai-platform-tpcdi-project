"""Keep bounded-memory delivery checkpoints and conservative acknowledgment state."""

import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time

from .errors import DeliveryUncertain, PublisherError


class Journal:
    """Persist per-file cursors, not an in-memory history of every cash transaction."""

    def __init__(self, directory, dataset, destination_id):
        """Lock a private state directory and bind it to source and destination identity."""
        raw = Path(directory).absolute()
        if raw.is_symlink():
            raise PublisherError("The publisher state directory cannot be a symbolic link.")
        self.directory = raw.resolve()
        self.generator_status = dataset.generator_status
        self.allow_unvalidated = dataset.allow_unvalidated
        if self.directory.is_relative_to(dataset.root):
            raise PublisherError("Publisher state must stay outside the generator dataset.")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = None
        self.connection = None
        try:
            self.lock = self._private_file("publisher.lock")
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PublisherError("Another publisher holds this state directory.") from None
            # Create with owner-only permissions before SQLite opens the file.
            self._private_file("delivery.sqlite").close()
            self.connection = sqlite3.connect(self.directory / "delivery.sqlite")
            self.connection.row_factory = sqlite3.Row
            self.connection.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                PRAGMA cache_size=-4096;
                PRAGMA temp_store=FILE;
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS segments (
                    id INTEGER PRIMARY KEY, mode TEXT NOT NULL, speed REAL NOT NULL,
                    daily_release TEXT NOT NULL, started REAL NOT NULL, ended REAL,
                    simulated_anchor TEXT, wall_anchor REAL, status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    path TEXT PRIMARY KEY, kind TEXT NOT NULL, sha256 TEXT NOT NULL,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    confirmed_rows INTEGER NOT NULL DEFAULT 0, confirmed_bytes INTEGER NOT NULL DEFAULT 0,
                    offset INTEGER NOT NULL DEFAULT 0, last_ct_id INTEGER,
                    inflight_rows INTEGER NOT NULL DEFAULT 0, inflight_bytes INTEGER NOT NULL DEFAULT 0,
                    first_ct_id INTEGER, end_ct_id INTEGER,
                    uncertain_retries INTEGER NOT NULL DEFAULT 0,
                    possible_duplicate_rows INTEGER NOT NULL DEFAULT 0, updated REAL NOT NULL
                );
            """)
            # Add policy evidence without changing an existing delivery identity.
            # Older journals remain usable, and earlier override use stays visible.
            columns = {row[1] for row in self.connection.execute("PRAGMA table_info(segments)")}
            for name, definition in (("generator_status", "TEXT"), ("allow_unvalidated", "INTEGER NOT NULL DEFAULT 0"),
                                     ("offline_only", "INTEGER NOT NULL DEFAULT 0"), ("through_date", "TEXT")):
                if name not in columns:
                    self.connection.execute(f"ALTER TABLE segments ADD COLUMN {name} {definition}")
            identity = {"schema": "1", "dataset_id": dataset.dataset_id,
                        "source_fingerprint": dataset.fingerprint, "destination_id": destination_id}
            saved = dict(self.connection.execute("SELECT key, value FROM metadata"))
            if saved and saved != identity:
                raise PublisherError("Checkpoint source or destination identity does not match this run.")
            with self.connection:
                if not saved:
                    self.connection.executemany("INSERT INTO metadata VALUES (?, ?)", identity.items())
                # An interrupted process might have received a remote acknowledgment
                # but failed before its local commit. Never label that work unsent.
                self.connection.execute("UPDATE deliveries SET status='uncertain' WHERE status='sending'")
            self.identity = identity
        except BaseException:
            self.close()
            raise

    def _private_file(self, name):
        """Open one state file without following a final symbolic link."""
        path = self.directory / name
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        os.fchmod(descriptor, 0o600)
        return os.fdopen(descriptor, "r+b")

    def close(self):
        """Close SQLite and release the process lock, including after failed setup."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.lock is not None:
            self.lock.close()
            self.lock = None

    def __enter__(self):
        """Return the locked journal for a context-managed publication."""
        return self

    def __exit__(self, exception_type, exception, traceback):
        """Release resources without hiding a delivery or local-storage failure."""
        self.close()

    def entry(self, item):
        """Get a file checkpoint, adding one idle row if this is its first visit."""
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO deliveries(path, kind, sha256, status, updated) VALUES (?, ?, ?, 'ready', ?)",
                (item.path, "stream" if item.path.startswith("stream/") else "object", item.sha256, time.time()),
            )
        row = self.connection.execute("SELECT * FROM deliveries WHERE path=?", (item.path,)).fetchone()
        if row["sha256"] != item.sha256:
            raise PublisherError("Checkpoint file hash does not match the saved source.")
        return row

    def check_retry(self, item, retry_uncertain):
        """Block uncertain delivery unless the caller accepts possible duplicate effects."""
        row = self.entry(item)
        if row["status"] in ("sending", "uncertain") and not retry_uncertain:
            raise DeliveryUncertain("Remote delivery is uncertain. Review report.json before an explicit same-identity retry.")
        return row

    def begin(self, item, rows, size, first_id=None, last_id=None):
        """Durably identify a complete bounded batch before any network call starts."""
        previous = self.entry(item)
        retry = previous["status"] in ("sending", "uncertain")
        with self.connection:
            self.connection.execute("""
                UPDATE deliveries SET status='sending', attempts=attempts+1,
                    uncertain_retries=uncertain_retries+?,
                    possible_duplicate_rows=possible_duplicate_rows+?,
                    inflight_rows=?, inflight_bytes=?, first_ct_id=?, end_ct_id=?, updated=? WHERE path=?
            """, (int(retry), previous["inflight_rows"] if retry else 0,
                  rows, size, first_id, last_id, time.time(), item.path))

    def acknowledge(self, item, offset=None, last_id=None):
        """Commit the whole batch only after every remote acknowledgment succeeds."""
        row = self.entry(item)
        with self.connection:
            self.connection.execute("""
                UPDATE deliveries SET status=?, confirmed_rows=confirmed_rows+inflight_rows,
                    confirmed_bytes=confirmed_bytes+inflight_bytes, offset=?, last_ct_id=?,
                    inflight_rows=0, inflight_bytes=0, first_ct_id=NULL, end_ct_id=NULL, updated=? WHERE path=?
            """, ("acked" if row["kind"] == "object" else "ready",
                  offset or 0, last_id, time.time(), item.path))

    def complete_stream(self, item):
        """Mark a fully acknowledged stream file complete, including an empty file."""
        row = self.entry(item)
        if row["confirmed_rows"] != item.rows or row["offset"] != item.size:
            raise PublisherError("Stream checkpoint does not cover the complete saved file.")
        with self.connection:
            self.connection.execute("UPDATE deliveries SET status='acked', updated=? WHERE path=?", (time.time(), item.path))

    def uncertain(self, item):
        """Keep an unsuccessful or interrupted send eligible only for explicit retry."""
        with self.connection:
            self.connection.execute("UPDATE deliveries SET status='uncertain', updated=? WHERE path=?", (time.time(), item.path))

    def start_segment(self, clock, daily_release, offline_only=False, through=None):
        """Record the new replay segment, whether it skips the stream, and its last daily date."""
        with self.connection:
            cursor = self.connection.execute(
                "INSERT INTO segments(mode, speed, daily_release, started, status, generator_status, allow_unvalidated, "
                "offline_only, through_date) VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?)",
                (clock.mode, clock.speed, daily_release, time.time(), self.generator_status, self.allow_unvalidated,
                 int(offline_only), through.isoformat() if through else None),
            )
        return cursor.lastrowid

    def finish_segment(self, segment, clock, status):
        """Save the restart clock anchor so delays do not imply uninterrupted replay."""
        with self.connection:
            self.connection.execute(
                "UPDATE segments SET ended=?, simulated_anchor=?, wall_anchor=?, status=? WHERE id=?",
                (time.time(), clock.origin.isoformat() if clock.origin else None, clock.wall_anchor, status, segment),
            )

    def report(self, status, error_code=None):
        """Write safe aggregate outcomes and at most 100 unresolved retry identities."""
        totals = self.connection.execute("""
            SELECT COUNT(*) AS visited_files,
                COALESCE(SUM(status='acked' AND kind='object'), 0) AS acknowledged_objects,
                COALESCE(SUM(status='acked' AND kind='stream'), 0) AS completed_stream_files,
                COALESCE(SUM(CASE WHEN kind='object' THEN confirmed_bytes ELSE 0 END), 0) AS acknowledged_offline_bytes,
                COALESCE(SUM(CASE WHEN kind='stream' THEN confirmed_rows ELSE 0 END), 0) AS acknowledged_messages,
                COALESCE(SUM(CASE WHEN kind='stream' THEN confirmed_bytes ELSE 0 END), 0) AS acknowledged_payload_bytes,
                COALESCE(SUM(status IN ('sending', 'uncertain')), 0) AS uncertain_files,
                COALESCE(SUM(CASE WHEN kind='stream' THEN inflight_rows ELSE 0 END), 0) AS uncertain_message_upper_bound,
                COALESCE(SUM(uncertain_retries), 0) AS uncertain_retries,
                COALESCE(SUM(CASE WHEN kind='stream' THEN possible_duplicate_rows ELSE 0 END), 0) AS possible_duplicate_messages
            FROM deliveries
        """).fetchone()
        unresolved = [dict(row) for row in self.connection.execute("""
            SELECT path, kind, sha256, offset AS acknowledged_source_offset,
                first_ct_id, end_ct_id, inflight_rows, attempts FROM deliveries
            WHERE status IN ('sending', 'uncertain') ORDER BY path LIMIT 100
        """)]
        latest = self.connection.execute("SELECT * FROM segments ORDER BY id DESC LIMIT 1").fetchone()
        segments = self.connection.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
        overrides = self.connection.execute(
            "SELECT COUNT(*) FROM segments WHERE generator_status='generated-unvalidated' AND allow_unvalidated=1"
        ).fetchone()[0]
        # The destination identity separates offline-only and full deliveries, so
        # the latest segment states the mode for the whole journal.
        offline_only = bool(latest["offline_only"]) if latest else False
        result = {"status": status, "profile": "clean", **self.identity,
                  "generator_status": self.generator_status, "allow_unvalidated": self.allow_unvalidated,
                  "offline_only": offline_only,
                  # The last daily date the latest run was asked to deliver; null means the whole dataset.
                  "through_date": latest["through_date"] if latest else None,
                  "unvalidated_delivery_segments": overrides,
                  "clock_segments": segments, "latest_clock_segment": dict(latest) if latest else None,
                  "outcomes": dict(totals), "unresolved": unresolved,
                  "unresolved_truncated": totals["uncertain_files"] > len(unresolved),
                  "delivery_semantics": "Acknowledged checkpoints only; uncertain retries can duplicate remote effects."}
        if error_code:
            result["error_code"] = error_code
        self._write_report(result)
        return result

    def _write_report(self, result):
        """Replace the local JSON report atomically without including secret settings."""
        with self._private_file("report.json.tmp") as output:
            output.truncate(0)
            output.write((json.dumps(result, indent=2, sort_keys=True) + "\n").encode())
            output.flush()
            os.fsync(output.fileno())
        os.replace(self.directory / "report.json.tmp", self.directory / "report.json")
