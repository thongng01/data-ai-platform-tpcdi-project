"""Bounded record buffers and external merge with a disk-backed run catalog."""

import hashlib
import heapq
import itertools
import json
import sqlite3
from contextlib import ExitStack
from pathlib import Path

import polars as pl

from .formats import Source


class SortedOutput:
    def __init__(self, root: Path, fan_in: int, export_rows: int = 8192):
        if fan_in < 2 or export_rows < 1:
            raise ValueError("Merge fan-in must be at least two and export_rows must be positive.")
        self.root = Path(root)
        self.fan_in = fan_in
        self.export_rows = export_rows
        self.work = self.root / "_work"
        self.work.mkdir(parents=True, exist_ok=True)
        self.catalog = sqlite3.connect(self.work / "export.sqlite")
        self.catalog.execute("CREATE TABLE IF NOT EXISTS runs (relative TEXT, level INTEGER, path TEXT PRIMARY KEY)")
        self.catalog.execute("CREATE INDEX IF NOT EXISTS output_level ON runs(relative, level)")
        self.sequence = 0
        self.buffer = []
        self.maximum_frame_rows = 0
        self.maximum_buffer_rows = 0
        self.maximum_merge_inputs = 0
        self.maximum_retained_run_paths = 0

    def _new_run(self):
        # O_EXCL also protects retained evidence if this object is opened again.
        while True:
            run = self.work / "runs" / f"{self.sequence:012d}.txt"
            self.sequence += 1
            run.parent.mkdir(parents=True, exist_ok=True)
            try:
                run.touch(exist_ok=False)
                return run
            except FileExistsError:
                continue

    def _compact(self, paths):
        merged = self._new_run()
        with merged.open("w", encoding="ascii", newline="\n") as stream:
            stream.writelines(self._merge(paths))
        return merged

    def _retain(self, relative, run, level=0):
        while True:
            self.catalog.execute("INSERT INTO runs VALUES (?, ?, ?)", (relative, level, str(run)))
            paths = [Path(row[0]) for row in self.catalog.execute(
                "SELECT path FROM runs WHERE relative=? AND level=? ORDER BY path LIMIT ?",
                (relative, level, self.fan_in),
            )]
            self.maximum_retained_run_paths = max(self.maximum_retained_run_paths, len(paths))
            if len(paths) < self.fan_in:
                break
            run = self._compact(paths)
            self.catalog.executemany("DELETE FROM runs WHERE path=?", [(str(path),) for path in paths])
            for path in paths:
                path.unlink()
            level += 1

    def add(self, relative: str, frame, source: Source | None = None, cdc=False):
        if isinstance(frame, pl.DataFrame):
            rows = frame.iter_rows(named=True)
        else:
            rows = iter(frame)
        for row in rows:
            key = str(row["_key"])
            if not key or not key.isascii() or any(c in key for c in "\t\r\n"):
                raise ValueError(f"Invalid sort key in {relative}.")
            if source:
                values = ["" if row.get(field.name) is None else str(row[field.name]) for field in source.fields]
                source.check(values)
                payload = source.separator.join(values)
                if cdc:
                    flag = row.get("_flag", "I")
                    if flag not in ("I", "U"):
                        raise ValueError(f"Invalid CDC flag in {relative}.")
                    payload = flag + "|" + payload
            else:
                payload = str(row["_payload"])
            if not payload.isascii() or any(c in payload for c in "\r\n\t"):
                raise ValueError(f"Invalid record separator or encoding in {relative}.")
            self.buffer.append((relative, key, payload))
            self.maximum_buffer_rows = max(self.maximum_buffer_rows, len(self.buffer))
            if len(self.buffer) == self.export_rows:
                self.flush()

    def flush(self):
        if not self.buffer:
            self.catalog.commit()
            return
        self.maximum_frame_rows = max(self.maximum_frame_rows, len(self.buffer))
        self.buffer.sort()
        for relative, rows in itertools.groupby(self.buffer, key=lambda row: row[0]):
            run = self._new_run()
            previous = None
            with run.open("w", encoding="ascii", newline="\n") as stream:
                for _, key, payload in rows:
                    if key == previous:
                        raise ValueError(f"Duplicate sort key in {relative}: {key}")
                    previous = key
                    stream.write(key + "\t" + payload + "\n")
            self._retain(relative, run)
        self.buffer.clear()
        self.catalog.commit()

    def _merge(self, paths):
        self.maximum_merge_inputs = max(self.maximum_merge_inputs, len(paths))
        self.maximum_retained_run_paths = max(self.maximum_retained_run_paths, len(paths))
        if len(paths) > self.fan_in:
            raise ValueError("A merge exceeded its open-file bound.")
        with ExitStack() as stack:
            streams = [stack.enter_context(path.open(encoding="ascii", newline="")) for path in paths]
            yield from heapq.merge(*streams)

    def adopt(self, root):
        self.flush()
        root = Path(root)
        if root.resolve() == self.root.resolve():
            raise ValueError("An export cannot adopt its own runs.")
        metrics_path = root / "_work" / "export-metrics.json"
        if metrics_path.exists():
            for name, value in json.loads(metrics_path.read_text()).items():
                setattr(self, name, max(getattr(self, name), value))
        other = sqlite3.connect(root / "_work" / "export.sqlite")
        try:
            for relative, level, path in other.execute("SELECT relative, level, path FROM runs ORDER BY relative, level, path"):
                target = self._new_run()
                Path(path).replace(target)
                self._retain(relative, target, level)
            self.catalog.commit()
            other.execute("DELETE FROM runs")
            other.commit()
        finally:
            other.close()

    def _final_paths(self, relative):
        # Reduce on disk: no Python list grows with run or delivery count.
        while True:
            paths = [Path(row[0]) for row in self.catalog.execute(
                "SELECT path FROM runs WHERE relative=? ORDER BY level, path LIMIT ?", (relative, self.fan_in)
            )]
            count = self.catalog.execute("SELECT COUNT(*) FROM runs WHERE relative=?", (relative,)).fetchone()[0]
            if count <= self.fan_in:
                return paths
            merged = self._compact(paths)
            self.catalog.executemany("DELETE FROM runs WHERE path=?", [(str(path),) for path in paths])
            self.catalog.execute("INSERT INTO runs VALUES (?, -1, ?)", (relative, str(merged)))
            for path in paths:
                path.unlink()

    def write(self, relative, *, cdc=False, xml=False, transform=None, check=None):
        self.flush()
        paths = self._final_paths(relative)
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        count = 0
        previous = None
        with target.open("wb") as stream:
            def emit(text):
                encoded = text.encode("ascii")
                stream.write(encoded)
                digest.update(encoded)

            if xml:
                emit('<?xml version="1.0" encoding="UTF-8"?>\n<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">\n')
            for line in self._merge(paths):
                key, payload = line.removesuffix("\n").split("\t", 1)
                if previous is not None and key <= previous:
                    raise ValueError(f"Nonunique or unordered key in {relative}: {key}")
                previous = key
                count += 1
                if cdc:
                    if count >= 10**12:
                        raise ValueError("CDC_DSN exceeds NUM(12).")
                    flag, payload = payload.split("|", 1)
                    if flag not in ("I", "U"):
                        raise ValueError(f"Invalid CDC flag in {relative}.")
                    if check:
                        check(payload)
                    payload = f"{flag}|{count}|{payload}"
                elif check:
                    check(payload)
                if transform:
                    payload = transform(key, payload)
                emit(payload + "\n")
            if xml:
                emit("</TPCDI:Actions>\n")
        for path in paths:
            path.unlink()
        self.catalog.execute("DELETE FROM runs WHERE relative=?", (relative,))
        self.catalog.commit()
        return {"path": relative, "rows": count, "bytes": target.stat().st_size, "sha256": digest.hexdigest()}

    def close(self):
        self.flush()
        metrics = {name: getattr(self, name) for name in ("maximum_frame_rows", "maximum_buffer_rows",
                   "maximum_merge_inputs", "maximum_retained_run_paths")}
        (self.work / "export-metrics.json").write_text(json.dumps(metrics) + "\n", encoding="ascii")
        self.catalog.close()
