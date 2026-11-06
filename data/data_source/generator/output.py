"""Seal bounded typed source parts and publish ordered, checked source files."""

import hashlib
import json
import time
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import polars as pl

from .output_formats import (ORDER_COLUMNS, RAW_FAMILIES, input_fields,
                             input_schema, order_key)
from .output_policy import family_policies


XML_OPEN = '<?xml version="1.0" encoding="UTF-8"?>\n<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">\n'
XML_CLOSE = "</TPCDI:Actions>\n"


def native_environment(config, root: Path) -> dict[str, str]:
    """Return pinned-engine resource settings for a fresh supervised child process."""
    # Private OOC settings trigger spill; they do not cap RSS. Give the engine
    # one eighth of the memory allowance, capped at 256 MB, to leave room for
    # account state, Python, scan buffers, and encoding. Convert the disk limit
    # from MiB to decimal MB and reserve half for non-spill run files.
    memory_mb = max(1, min(256, config.memory_limit_mb // 8))
    disk_mb = max(1, int(config.disk_limit_mb * 1024**2 / 2 / 1_000_000))
    return {"POLARS_MAX_THREADS": str(config.threads),
            "POLARS_OOC_MEMORY_BUDGET_MB": str(memory_mb),
            "POLARS_OOC_DISK_BUDGET_MB": str(disk_mb),
            "POLARS_OOC_SPILL_DIR": str(Path(root).absolute() / "_work" / "polars-spill")}


def native_batches(query: pl.LazyFrame, rows: int, consume) -> None:
    """Consume native batches synchronously and stop on the original writer error."""
    # The pinned 2.0.0 collect_batches iterator has no close/stop operation,
    # despite its docstring. The synchronous sink's explicit stop result keeps
    # query lifetime local and avoids leaving a producer running after failure.
    failures = []

    def accept(frame):
        """Tell the engine to stop before propagating a consumer failure."""
        try:
            consume(frame)
        except BaseException as error:
            failures.append(error)
            return True
        return False

    query.sink_batches(accept, chunk_size=rows, maintain_order=True, engine="streaming")
    if failures:
        raise failures[0]


class BinaryWriter:
    """Hash exactly the native writer's bytes and close one source file at a time."""

    def __init__(self, root: Path, relative: str, *, xml=False):
        """Create a new output file without overwriting retained partial evidence."""
        self.relative = relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        self.stream = target.open("xb")
        self.digest = hashlib.sha256()
        self.size = self.rows = 0
        self.xml = xml
        if xml:
            self.write(XML_OPEN.encode("ascii"))

    def write(self, data):
        """Support the native binary sink and include only bytes actually written."""
        written = self.stream.write(data)
        self.digest.update(data[:written])
        self.size += written
        return written

    def close(self):
        """Finish exact XML framing and return path, row, byte, and hash metadata."""
        if self.xml:
            self.write(XML_CLOSE.encode("ascii"))
        self.stream.close()
        return {"path": self.relative, "rows": self.rows, "bytes": self.size,
                "sha256": self.digest.hexdigest()}

    def abort(self):
        """Close a failed source file without publishing a successful inventory entry."""
        self.stream.close()


class RecordMetadata(NamedTuple):
    """Name the fixed columns shared by scalar records and borrowed column batches."""

    record_day: int
    flag: str
    profile: int
    delivery_rank: int
    relative: str


class Publication(NamedTuple):
    """Return source inventory and the separate audit receipt to the coordinator."""

    inventory: list[dict]
    audit: dict


class PartWriter:
    """Buffer correct tuples, record source-only changes, and seal one producer part."""

    def __init__(self, config, root, sources, route, apply_recording, record_fault,
                 group_id: int | None, *, part_name=None, reuse=False):
        """Accept explicit recording dependencies or reopen a completed producer seal."""
        self.config, self.root = config, Path(root)
        self.route, self.apply_recording, self.record_fault = route, apply_recording, record_fault
        self.policies = family_policies(config, sources)
        if group_id is not None and (type(group_id) is not int or group_id < 1):
            raise ValueError("A group ID must be a positive first-customer ordinal.")
        self.group_id = group_id
        self.parts = self.root / "_work" / "source-parts"
        if part_name not in (None, "reference", "market", "prospects") or (part_name and group_id is not None):
            raise ValueError("Invalid named source producer.")
        name = part_name or ("coordinator" if group_id is None else f"group-{group_id:011d}")
        self.part = self.parts / name
        if not reuse:
            self.part.mkdir(parents=True, exist_ok=False)
        self.schemas = {family: input_schema(family, sources)
                        for family in (*sources, "CustomerMgmt", "FINWIRE", "__audit")}
        self.fields = {family: input_fields(family, sources) for family in self.schemas}
        self.buffers = {}
        self.buffer_rows = self.sequence = 0
        self.closed = False
        self.families = set()
        self.metrics = {"maximum_buffer_rows": 0, "maximum_frame_rows": 0,
                        "maximum_stream_batch_rows": 0, "part_fragments": 0,
                        "sealed_bytes": 0, "spool_seconds": 0.0, "seal_seconds": 0.0,
                        "sort_write_seconds": 0.0, "native_batches": 0}
        if reuse:
            marker = json.loads((self.part / "complete.json").read_text(encoding="ascii"))
            self.metrics.update(marker["metrics"])
            self.closed = True

    def _require_open(self):
        """Reject every append after this part has been sealed."""
        if self.closed:
            raise ValueError("A sealed source part cannot accept more records.")

    def _metadata(self, family, day, flag, profile):
        """Name fixed metadata in one tuple, with no extra per-record mapping."""
        if family not in self.schemas:
            raise ValueError(f"Unknown source family: {family}")
        if type(day) is not int:
            raise ValueError("A recording day must be an integer offset from config.start.")
        if flag not in ("I", "U"):
            raise ValueError("A source CDC flag must be I or U.")
        day_value = self.config.start + timedelta(days=day)
        return RecordMetadata(day, flag, 1 if profile is None else profile,
                              0 if family == "__audit" else int(day_value > self.config.cutover),
                              self.route(family, day_value))

    def record(self, family, day, values, *, order=(), flag="I", profile=None):
        """Buffer one typed tuple until the shared row allowance triggers a flush."""
        self._require_open()
        if family not in self.fields or not isinstance(values, tuple) or len(values) != len(self.fields[family]):
            raise ValueError(f"Incorrect typed source tuple in {family}.")
        metadata = self._metadata(family, day, flag, profile)
        order = self.policies[family].complete_order(order)
        names = ORDER_COLUMNS.get(family, ())
        if len(order) != len(names):
            raise ValueError(f"Incorrect ordering metadata in {family}: expected {names}.")
        self.buffers.setdefault(family, []).append((*values, *order, *metadata))
        self.buffer_rows += 1
        self.metrics["maximum_buffer_rows"] = max(self.metrics["maximum_buffer_rows"], self.buffer_rows)
        if self.buffer_rows >= self.config.export_rows:
            self.flush()

    @staticmethod
    def _array_length(value):
        """Distinguish scalar constants from aligned input arrays without materializing them."""
        return None if isinstance(value, (str, bytes)) or value is None or not hasattr(value, "__len__") else len(value)

    @staticmethod
    def _series(name, value, dtype, count):
        """Reject lossy array casts before constructing an explicitly typed column."""
        source_dtype = getattr(value, "dtype", None)
        if dtype == pl.Int64 and source_dtype is not None:
            # Polars accepts Float64 NumPy arrays as Int64 by truncating even
            # with strict=True. Validate their physical kind before that cast.
            if isinstance(value, pl.Series):
                valid = source_dtype.is_integer() or source_dtype == pl.Null
            else:
                valid = getattr(source_dtype, "kind", None) in ("i", "u")
            if not valid:
                raise ValueError(f"{name} requires integral source values.")
        if PartWriter._array_length(value) is None:
            value = [value] * count
        return pl.Series(name, value, dtype=dtype, strict=True)

    def record_batch(self, family, day, columns, *, order=None, flag="I"):
        """Consume bounded borrowed columns synchronously, before the producer reuses them."""
        self._require_open()
        if family not in self.fields or set(columns) != set(self.fields[family]):
            raise ValueError(f"Incorrect typed source columns in {family}.")
        metadata = dict(order or {})
        profile = metadata.pop("_profile", 1)
        names = ORDER_COLUMNS.get(family, ())
        for name, value in self.policies[family].order_defaults.items():
            metadata.setdefault(name, value)
        if set(metadata) != set(names):
            raise ValueError(f"Incorrect ordering columns in {family}: expected {names}.")
        lengths = {length for value in (*columns.values(), *metadata.values(), profile)
                   if (length := self._array_length(value)) is not None}
        if len(lengths) > 1:
            raise ValueError("Source arrays must have identical lengths.")
        count = next(iter(lengths), 1)
        if not 0 <= count <= self.config.export_rows:
            raise ValueError("A source batch exceeds the shared export row allowance.")
        # Pending scalar tuples and an independent input frame never share the
        # active row allowance. Borrowed arrays are not retained after this call.
        self.flush()
        context = self._metadata(family, day, flag, 1)
        data = dict(columns)
        data.update(metadata)
        data.update(_record_day=context.record_day, _flag=context.flag, _profile=profile,
                    _delivery_rank=context.delivery_rank, _relative=context.relative)
        if not count:
            return
        series = {name: self._series(name, data[name], dtype, count)
                  for name, dtype in self.schemas[family].items()}
        self._spool(family, pl.DataFrame(series))

    def flush(self):
        """Convert the aggregate pending tuples into bounded typed family frames."""
        pending, self.buffers = self.buffers, {}
        self.buffer_rows = 0
        for family, rows in pending.items():
            frame = pl.DataFrame(rows, schema=self.schemas[family], orient="row", strict=True)
            self._spool(family, frame)

    def _spool(self, family, frame):
        """Apply source-only recording and persist typed columns without source payload joins."""
        if not frame.height:
            return
        started = time.monotonic()
        self.metrics["maximum_frame_rows"] = max(self.metrics["maximum_frame_rows"], frame.height)
        if family not in RAW_FAMILIES:
            frame, faults = self.apply_recording(family, frame)
            for entry in faults:
                self.record_fault(entry)
        frame = self.policies[family].prepare(frame)
        frame = frame.with_columns(order_key(family).alias("_key"))
        bad_key = (pl.col("_key").is_null() | (pl.col("_key") == "") |
                   pl.col("_key").str.contains(r"[^\x00-\x7F]|[\r\n\t]"))
        if frame.select(bad_key.any()).item():
            raise ValueError(f"Invalid sort key in {family}.")
        directory = self.part / "fragments" / family
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{self.sequence:012d}.parquet"
        self.sequence += 1
        frame.write_parquet(target, compression="uncompressed", statistics=True,
                            row_group_size=self.config.export_rows)
        self.families.add(family)
        self.metrics["part_fragments"] += 1
        self.metrics["spool_seconds"] += time.monotonic() - started

    def seal(self, *, completion=None):
        """Seal families and commit context and business results in the last marker."""
        self._require_open()
        self.flush()
        # Recording a flushed batch can append bounded audit records. Drain those
        # before sealing so no selected error remains outside its completed part.
        while self.buffer_rows:
            self.flush()
        started = time.monotonic()
        files = {}
        for family in sorted(self.families):
            directory = self.part / "fragments" / family
            target = self.part / f"{family}.parquet"
            temporary = target.with_suffix(".parquet.partial")
            pl.scan_parquet(str(directory / "*.parquet")).sink_parquet(
                temporary, compression="uncompressed", statistics=True,
                row_group_size=self.config.export_rows, maintain_order=True, engine="streaming")
            temporary.replace(target)
            files[family] = target.name
            self.metrics["sealed_bytes"] += target.stat().st_size
            for fragment in directory.iterdir():
                fragment.unlink()
            directory.rmdir()
        self.metrics["seal_seconds"] += time.monotonic() - started
        marker = {"group_id": self.group_id, "families": files, "metrics": dict(self.metrics),
                  **(completion or {})}
        temporary = self.part / "complete.json.partial"
        temporary.write_text(json.dumps(marker, sort_keys=True) + "\n", encoding="ascii")
        temporary.replace(self.part / "complete.json")
        self.closed = True
        return marker


class SourcePublisher:
    """Discover sealed producers, order their records, and write final source files."""

    def __init__(self, config, root, sources, coordinator_part, metrics):
        """Accept sealed input paths and counters without a live producer or recorder."""
        self.config, self.root = config, Path(root)
        self.part = coordinator_part
        self.parts = self.part.parent
        self.families = set(sources) | RAW_FAMILIES
        self.policies = family_policies(config, sources)
        self.metrics = metrics

    def discover(self):
        """Require exactly the scheduled completed group namespaces before final export."""
        schedule = self.parts / "scheduled-groups.json"
        directories = {path.name: path for path in self.parts.iterdir() if path.is_dir() and path.name.startswith("group-")}
        if schedule.exists():
            ids = json.loads(schedule.read_text(encoding="ascii"))
            if not isinstance(ids, list) or any(type(value) is not int or value < 1 for value in ids) or len(set(ids)) != len(ids):
                raise ValueError("Invalid scheduled source group IDs.")
        elif directories:
            raise ValueError("Source groups exist without their scheduled group list.")
        else:
            ids = []
        expected = {f"group-{value:011d}" for value in ids}
        if set(directories) != expected:
            raise ValueError("The completed source groups differ from their schedule.")
        parts = [self.part]
        # Each named phase has its own seal, so a resumed run need not append to
        # an earlier phase's buffers. Legacy single-coordinator runs use none.
        for name in ("reference", "market", "prospects"):
            part = self.parts / name
            if part.exists():
                marker_path = part / "complete.json"
                if not marker_path.exists():
                    raise ValueError(f"Unfinished source producer: {name}")
                marker = json.loads(marker_path.read_text(encoding="ascii"))
                self._aggregate(marker["metrics"])
                parts.append(part)
        for name in sorted(expected):
            part = directories[name]
            marker_path = part / "complete.json"
            if not marker_path.exists():
                raise ValueError(f"Unfinished source group: {name}")
            marker = json.loads(marker_path.read_text(encoding="ascii"))
            if marker.get("group_id") != int(name.removeprefix("group-")):
                raise ValueError(f"Mismatched source group marker: {name}")
            self._aggregate(marker["metrics"])
            parts.append(part)
        return parts

    def _aggregate(self, metrics):
        """Sum sealed worker counters and retain the largest worker maxima."""
        for name, value in metrics.items():
            self.metrics[name] = max(self.metrics.get(name, 0), value) if name.startswith("maximum_") else self.metrics.get(name, 0) + value

    @staticmethod
    def _check_raw(family, frame):
        """Keep raw-line encoding and FINWIRE final structural checks exact."""
        payload = pl.col("_payload")
        invalid = payload.is_null() | payload.str.contains(r"[^\x00-\x7F]|[\r\n\t]")
        if family == "FINWIRE":
            kind = payload.str.slice(15, 3)
            width = payload.str.len_bytes()
            valid_width = (((kind == "CMP") & (width == 543)) |
                           ((kind == "SEC") & width.is_in((170, 220))) |
                           ((kind == "FIN") & width.is_in((196, 246))))
            invalid = invalid | ~valid_width | ~payload.str.slice(0, 15).str.contains(r"^[0-9]{8}-[0-9]{6}$")
        if frame.select(invalid.fill_null(True).any()).item():
            raise ValueError(f"Invalid encoded source record in {family}.")

    def _contract_chunks(self, family, frame):
        """Split only at initial, daily CDC, or dated Trade schema changes."""
        policy = self.policies[family]
        if not policy.cdc:
            yield 0, frame
            return
        dated_start = policy.dated_start

        def kind(index):
            """Resolve one endpoint without building another native plan for the usual case."""
            rank = int(frame["_delivery_rank"][index])
            return 2 if dated_start is not None and rank and int(frame["_record_day"][index]) >= dated_start else rank

        first, last = kind(0), kind(-1)
        if first == last:
            yield first, frame
            return
        # Dated YYYYMMDD paths sort chronologically, so initial, daily v1, and
        # daily v2 Trade ranges stay contiguous. Endpoint checks are sufficient
        # because a later path cannot return to an earlier contract version.
        expression = pl.col("_delivery_rank")
        if dated_start is not None:
            expression = pl.when((expression > 0) & (pl.col("_record_day") >= dated_start)).then(2).otherwise(expression)
        chunks = frame.with_columns(expression.alias("_write_kind")).partition_by(
            "_write_kind", include_key=False, as_dict=True, maintain_order=True)
        for (write_kind,), chunk in chunks.items():
            yield int(write_kind), chunk

    def _encode_batch(self, family, batch, write_kind, writer):
        """Format and check a bounded contract batch before splitting source files."""
        if family in RAW_FAMILIES:
            self._check_raw(family, batch)
            return batch.select("_payload", "_relative"), "|"
        return self.policies[family].encode(batch, write_kind, writer)

    def write_family(self, family, paths, expected):
        """Sort one family with native spill and write contiguous source-file segments."""
        started = time.monotonic()
        # Initial files precede daily YYYYMMDD paths, which sort chronologically.
        # Global CT_ID assignment and dated Trade chunks rely on this path order.
        query = pl.scan_parquet(paths).sort(["_delivery_rank", "_relative", "_key"])
        writer = None
        inventory = []
        previous = None
        def consume(frame):
            """Check one bounded frame and write its contiguous source-file segments."""
            nonlocal previous, writer
            self.metrics["native_batches"] += 1
            self.metrics["maximum_stream_batch_rows"] = max(self.metrics["maximum_stream_batch_rows"], frame.height)
            if not frame.height:
                return
            keys = frame.select("_delivery_rank", "_relative", "_key")
            duplicate = keys.select(pl.struct(pl.all()).eq(pl.struct(pl.all()).shift(1)).fill_null(False).any()).item()
            # Paths and keys contain ASCII only. Python tuple comparison thus
            # matches the native byte order, including across batch boundaries.
            first, last = tuple(keys.row(0)), tuple(keys.row(-1))
            if duplicate or (previous is not None and first <= previous):
                raise ValueError(f"Nonunique or unordered source key in {family}.")
            previous = last
            # Formatting and all contract checks precede per-file dispatch.
            # Native partitioning drops the path column, leaving final writer
            # frames without a select/check plan for each small source file.
            for write_kind, chunk in self._contract_chunks(family, frame):
                encoded, separator = self._encode_batch(family, chunk, write_kind, writer)
                source_files = encoded.partition_by("_relative", include_key=False,
                                                  as_dict=True, maintain_order=True)
                for (relative,), segment in source_files.items():
                    if relative not in expected:
                        raise ValueError(f"Unexpected source output: {relative}")
                    if writer is None or writer.relative != relative:
                        if writer is not None:
                            inventory.append(writer.close())
                        writer = BinaryWriter(self.root, relative, xml=family == "CustomerMgmt")
                    segment.write_csv(writer, separator=separator, include_header=False, include_bom=False,
                                      quote_style="never", line_terminator="\n", batch_size=self.config.export_rows)
                    writer.rows += segment.height

        try:
            native_batches(query, self.config.export_rows, consume)
            if writer is not None:
                inventory.append(writer.close())
                writer = None
        finally:
            if writer is not None:
                writer.abort()
            self.metrics["sort_write_seconds"] += time.monotonic() - started
        return inventory

    def publish(self, expected_paths, audit_path):
        """Write every expected source and the separate audit from sealed typed parts."""
        parts = self.discover()
        family_paths = {}
        for part in parts:
            marker = json.loads((part / "complete.json").read_text(encoding="ascii"))
            for family, name in marker["families"].items():
                if family not in self.families or Path(name).name != name or not (part / name).is_file():
                    raise ValueError("Invalid sealed source file in completion marker.")
                family_paths.setdefault(family, []).append(str(part / name))
        expected = set(expected_paths)
        inventory = []
        audit = self.write_family("__audit", family_paths.pop("__audit"), {audit_path}) if "__audit" in family_paths else []
        audit_receipt = audit[0] if audit else BinaryWriter(self.root, audit_path).close()
        for family, paths in sorted(family_paths.items()):
            inventory.extend(self.write_family(family, paths, expected))
        written = {item["path"] for item in inventory}
        for relative in sorted(expected - written):
            inventory.append(BinaryWriter(self.root, relative, xml=relative.endswith("CustomerMgmt.xml")).close())
        return Publication(sorted(inventory, key=lambda item: item["path"]), audit_receipt)
