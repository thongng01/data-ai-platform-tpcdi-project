import hashlib
import heapq
from contextlib import ExitStack
from pathlib import Path

import polars as pl

from .formats import Source


class SortedOutput:
    def __init__(self, root: Path, fan_in: int):
        self.root = root
        self.fan_in = fan_in
        self.runs = {}
        self.sequence = 0
        self.maximum_frame_rows = 0
        self.maximum_merge_inputs = 0
        self.maximum_retained_run_paths = 0

    def _new_run(self):
        run = self.root / "runs" / f"{self.sequence:012d}.txt"
        self.sequence += 1
        run.parent.mkdir(parents=True, exist_ok=True)
        return run

    def _compact(self, paths):
        merged = self._new_run()
        with merged.open("w", encoding="utf-8", newline="\n") as stream:
            stream.writelines(self._merge(paths))
        for path in paths:
            path.unlink()
        return merged

    def _retain(self, relative, run):
        levels = self.runs.setdefault(relative, [])
        level = 0
        while True:
            if level == len(levels):
                levels.append([])
            levels[level].append(run)
            if len(levels[level]) < self.fan_in:
                break
            run = self._compact(levels[level])
            levels[level] = []
            level += 1
        self.maximum_retained_run_paths = max(
            self.maximum_retained_run_paths,
            sum(len(paths) for source_levels in self.runs.values() for paths in source_levels),
        )

    def add(self, relative: str, frame: pl.DataFrame, source: Source | None = None, cdc=False):
        if frame.is_empty():
            return
        self.maximum_frame_rows = max(self.maximum_frame_rows, frame.height)
        if source:
            columns = (["_flag"] if cdc else []) + source.columns
            expressions = [pl.col(column).cast(pl.String).fill_null("") for column in columns]
            frame = frame.select("_key", pl.concat_str(expressions, separator=source.separator).alias("_payload"))
        frame = frame.select("_key", "_payload").sort("_key")
        if frame["_key"].n_unique() != frame.height:
            raise ValueError(f"Duplicate sort key in {relative}.")
        if frame["_payload"].str.contains(r"[\r\n\t]").any():
            raise ValueError(f"Invalid record separator in {relative}.")
        run = self._new_run()
        frame.write_csv(run, separator="\t", include_header=False, quote_style="never")
        self._retain(relative, run)

    def _merge(self, paths):
        self.maximum_merge_inputs = max(self.maximum_merge_inputs, len(paths))
        if len(paths) > self.fan_in:
            raise ValueError("A merge exceeded its open-file bound.")
        with ExitStack() as stack:
            streams = [stack.enter_context(path.open(encoding="utf-8")) for path in paths]
            yield from heapq.merge(*streams)

    def finish(self, specifications: dict[str, tuple[bool, bool]]):
        inventory = []
        for relative, (cdc, xml) in sorted(specifications.items()):
            paths = [path for level in self.runs.get(relative, []) for path in level]
            while len(paths) > self.fan_in:
                next_paths = []
                for offset in range(0, len(paths), self.fan_in):
                    group = paths[offset:offset + self.fan_in]
                    next_paths.append(self._compact(group))
                paths = next_paths
            target = self.root / "sources" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            count = 0
            previous = None
            with target.open("wb") as stream:
                def emit(text):
                    encoded = text.encode("utf-8")
                    stream.write(encoded)
                    digest.update(encoded)

                if xml:
                    emit('<?xml version="1.0" encoding="UTF-8"?>\n<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">\n')
                for line in self._merge(paths):
                    key, payload = line.rstrip("\n").split("\t", 1)
                    if previous is not None and key <= previous:
                        raise ValueError(f"Nonunique or unordered key in {relative}: {key}")
                    previous = key
                    count += 1
                    if cdc:
                        flag, payload = payload.split("|", 1)
                        payload = f"{flag}|{count}|{payload}"
                    emit(payload + "\n")
                if xml:
                    emit("</TPCDI:Actions>\n")
            for path in paths:
                path.unlink()
            inventory.append({"path": relative, "rows": count, "bytes": target.stat().st_size, "sha256": digest.hexdigest()})
        return inventory
