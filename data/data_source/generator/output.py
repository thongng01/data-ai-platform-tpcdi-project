"""Own bounded typed spools, native ordering, exact source writers, and group seals."""

import hashlib
import json
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from .output_formats import (ORDER_COLUMNS, RAW_FAMILIES, cash_name, input_fields,
                             input_schema, native_contract, order_key)


XML_OPEN = '<?xml version="1.0" encoding="UTF-8"?>\n<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">\n'
XML_CLOSE = "</TPCDI:Actions>\n"
CASH_NAME_PATTERN = r"^(?:Deposit|Withdrawal|Trade settlement [1-9][0-9]{0,14}|Dividend [A-Za-z0-9._-]{1,15})$"


def native_environment(config, root: Path) -> dict[str, str]:
    """Return pinned-engine resource settings for a fresh supervised child process."""
    # Private OOC settings trigger spill; they do not cap RSS. Leave most of the
    # process-tree allowance for account state, Python, scan buffers, and encoding.
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
    """Hash exactly the native writer's bytes and close one final delivery at a time."""

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
        """Finish exact XML framing and return the existing inventory entry shape."""
        if self.xml:
            self.write(XML_CLOSE.encode("ascii"))
        self.stream.close()
        return {"path": self.relative, "rows": self.rows, "bytes": self.size,
                "sha256": self.digest.hexdigest()}

    def abort(self):
        """Close a failed delivery without publishing a successful inventory entry."""
        self.stream.close()


class TypedOutput:
    """Hide buffering, part lifecycle, spill sorting, and final encoding from producers."""

    def __init__(self, owner, group_id: int | None):
        """Allocate one shared scalar row allowance for one coordinator or group."""
        self.owner, self.config, self.root = owner, owner.config, owner.root
        if group_id is not None and (type(group_id) is not int or group_id < 1):
            raise ValueError("A group ID must be a positive first-customer ordinal.")
        self.group_id = group_id
        self.parts = self.root / "_work" / "source-parts"
        name = "coordinator" if group_id is None else f"group-{group_id:011d}"
        self.part = self.parts / name
        self.part.mkdir(parents=True, exist_ok=False)
        self.schemas = {family: input_schema(family, owner.sources)
                        for family in (*owner.sources, "CustomerMgmt", "FINWIRE", "__audit")}
        self.fields = {family: input_fields(family, owner.sources) for family in self.schemas}
        self.buffers = {}
        self.buffer_rows = self.sequence = 0
        self.closed = False
        self.families = set()
        self.cash_sequence = 0
        self.metrics = {"maximum_buffer_rows": 0, "maximum_frame_rows": 0,
                        "maximum_stream_batch_rows": 0, "part_fragments": 0,
                        "sealed_bytes": 0, "spool_seconds": 0.0, "seal_seconds": 0.0,
                        "sort_write_seconds": 0.0, "native_batches": 0}

    def _require_open(self):
        """Reject every append after this part has been sealed."""
        if self.closed:
            raise ValueError("A sealed source part cannot accept more records.")

    def _metadata(self, family, day, order, flag, profile):
        """Build fixed metadata without formatting a key or copying a row dictionary."""
        if family not in self.schemas:
            raise ValueError(f"Unknown source family: {family}")
        if type(day) is not int:
            raise ValueError("A recording day must be an integer offset from config.start.")
        if flag not in ("I", "U"):
            raise ValueError("A source CDC flag must be I or U.")
        names = ORDER_COLUMNS.get(family, ())
        if family == "CashTransaction" and len(order) == 1:
            order = (*order, 0)
        if len(order) != len(names):
            raise ValueError(f"Incorrect ordering metadata in {family}: expected {names}.")
        day_value = self.config.start + timedelta(days=day)
        relative = self.owner._path(family, day_value)
        return (*order, day, flag, 1 if profile is None else profile,
                0 if family == "__audit" else int(day_value > self.config.cutover), relative)

    def record(self, family, day, values, *, order=(), flag="I", profile=None):
        """Append one typed tuple without creating a tiny DataFrame per business event."""
        self._require_open()
        if family not in self.fields or not isinstance(values, tuple) or len(values) != len(self.fields[family]):
            raise ValueError(f"Incorrect typed source tuple in {family}.")
        metadata = self._metadata(family, day, order, flag, profile)
        self.buffers.setdefault(family, []).append((*values, *metadata))
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
        if TypedOutput._array_length(value) is None:
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
        if family == "CashTransaction":
            metadata.setdefault("_reference", 0)
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
        meta_values = self._metadata(family, day, tuple(0 for _ in names), flag, 1)
        data = dict(columns)
        data.update(metadata)
        data.update(zip(("_record_day", "_flag", "_profile", "_delivery_rank", "_relative"), meta_values[len(names):]))
        data["_profile"] = profile
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
            frame, faults = self.owner.recorder.apply(family, frame)
            for entry in faults:
                self.owner._record_fault(entry)
        if family == "Trade" and "T_CHANNEL" not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias("T_CHANNEL"))
        if family == "CashTransaction":
            frame = frame.with_columns(cash_name().alias("CT_NAME"))
            first_epoch_day = (self.config.start - date(1970, 1, 1)).days
            invalid = ((pl.col("CT_CA_ID") <= 0) | (pl.col("CT_CA_ID") >= 10**11) |
                       (pl.col("_ordinal") < 0) | (pl.col("_ordinal") >= 10**15) |
                       (pl.col("_record_day") < 0) | (pl.col("_record_day") >= self.config.total_days) |
                       (pl.col("CT_DTS") // 86400 != first_epoch_day + pl.col("_record_day")))
            if frame.select(invalid.fill_null(True).any()).item():
                raise ValueError("A cash timestamp or identity exceeds the recording contract.")
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

    def seal(self):
        """Seal each family natively and publish the completion marker last."""
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
        marker = {"group_id": self.group_id, "families": files, "metrics": dict(self.metrics)}
        temporary = self.part / "complete.json.partial"
        temporary.write_text(json.dumps(marker, sort_keys=True) + "\n", encoding="ascii")
        temporary.replace(self.part / "complete.json")
        self.closed = True
        return marker

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
        """Combine sealed worker counters and maxima without inventing merge metrics."""
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
        if family in RAW_FAMILIES or (family != "CashTransaction" and not self.owner.sources[family].cdc):
            yield 0, frame
            return
        channel_day = (date.fromisoformat(self.config.trade_channel_start) - self.config.start).days

        def kind(index):
            """Resolve one endpoint without building another native plan for the usual case."""
            rank = int(frame["_delivery_rank"][index])
            return 2 if family == "Trade" and rank and int(frame["_record_day"][index]) >= channel_day else rank

        first, last = kind(0), kind(-1)
        if first == last:
            yield first, frame
            return
        # Delivery-major ordering makes these ranges contiguous. Trade has at
        # most three kinds: initial, daily v1, and daily v2. Most batches need no
        # split, even when they contain many separate delivery files.
        expression = pl.col("_delivery_rank")
        if family == "Trade":
            expression = pl.when((expression > 0) & (pl.col("_record_day") >= channel_day)).then(2).otherwise(expression)
        chunks = frame.with_columns(expression.alias("_write_kind")).partition_by(
            "_write_kind", include_key=False, as_dict=True, maintain_order=True)
        for (write_kind,), chunk in chunks.items():
            yield int(write_kind), chunk

    def _encode_batch(self, family, batch, write_kind, writer):
        """Format and check a bounded contract batch before splitting delivery files."""
        if family in RAW_FAMILIES:
            self._check_raw(family, batch)
            return batch.select("_payload", "_relative"), "|"
        source = self.owner.sources[family]
        if family == "CashTransaction":
            if batch.select((~pl.col("CT_NAME").str.contains(CASH_NAME_PATTERN)).fill_null(True).any()).item():
                raise ValueError("Invalid CT_NAME.")
            batch = batch.with_columns(pl.int_range(
                self.cash_sequence + 1, self.cash_sequence + batch.height + 1).alias("CT_ID"))
            self.cash_sequence += batch.height
        elif family == "Trade" and write_kind == 2:
            source = source.daily_version
        frame = native_contract(source).format(batch).with_columns(batch["_relative"])
        if family == "CashTransaction" and write_kind:
            fields = [pl.col("CT_ID").cast(pl.Int64).alias("ct_id"),
                      pl.col("CT_CA_ID").cast(pl.Int64).alias("ct_ca_id"),
                      (pl.col("CT_DTS").str.replace(" ", "T", literal=True) + "Z").alias("ct_dts"),
                      pl.col("CT_AMT").alias("ct_amt"), pl.col("CT_NAME").alias("ct_name"),
                      pl.lit(1).alias("schema_version")]
            # CPython ensure_ascii escapes ASCII DEL, while serde leaves it
            # literal. Keep that rule even though legal cash names exclude DEL.
            frame = frame.select(pl.struct(fields).struct.json_encode().str.replace_all(
                "\x7f", "\\u007f", literal=True).alias("_payload"), "_relative")
        elif source.cdc and write_kind:
            # Only the first delivery can continue the previous batch's open
            # file. All later groups start at one. This window is bounded by the
            # admitted batch, never by a complete family or delivery history.
            carried = (pl.when(pl.col("_relative") == writer.relative).then(writer.rows).otherwise(0)
                       if writer is not None else pl.lit(0))
            cdc = batch.select(pl.col("_flag").alias("CDC_FLAG"),
                (pl.col("_relative").cum_count().over("_relative").cast(pl.Int64) + carried).alias("CDC_DSN"))
            if cdc["CDC_DSN"].max() >= 10**12:
                raise ValueError("CDC_DSN exceeds NUM(12).")
            frame = frame.with_columns(cdc["CDC_FLAG"], cdc["CDC_DSN"]).select(
                "CDC_FLAG", "CDC_DSN", *source.columns, "_relative")
        return frame, source.separator

    def write_family(self, family, paths, expected):
        """Sort one family with native spill and write contiguous delivery segments."""
        started = time.monotonic()
        query = pl.scan_parquet(paths).sort(["_delivery_rank", "_relative", "_key"])
        writer = None
        inventory = []
        previous = None
        def consume(frame):
            """Check one bounded frame and write its contiguous delivery segments."""
            nonlocal previous, writer
            self.metrics["native_batches"] += 1
            self.metrics["maximum_stream_batch_rows"] = max(self.metrics["maximum_stream_batch_rows"], frame.height)
            if not frame.height:
                return
            keys = frame.select("_delivery_rank", "_relative", "_key")
            duplicate = keys.select(pl.struct(pl.all()).eq(pl.struct(pl.all()).shift(1)).fill_null(False).any()).item()
            first, last = tuple(keys.row(0)), tuple(keys.row(-1))
            if duplicate or (previous is not None and first <= previous):
                raise ValueError(f"Nonunique or unordered source key in {family}.")
            previous = last
            # Formatting and all contract checks precede per-file dispatch.
            # Native partitioning drops the path column, leaving final writer
            # frames without a select/check plan for each small delivery.
            for write_kind, chunk in self._contract_chunks(family, frame):
                encoded, separator = self._encode_batch(family, chunk, write_kind, writer)
                deliveries = encoded.partition_by("_relative", include_key=False,
                                                  as_dict=True, maintain_order=True)
                for (relative,), segment in deliveries.items():
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
                if family not in self.schemas or Path(name).name != name or not (part / name).is_file():
                    raise ValueError("Invalid sealed source file in completion marker.")
                family_paths.setdefault(family, []).append(str(part / name))
        expected = set(expected_paths)
        inventory = []
        audit = self.write_family("__audit", family_paths.pop("__audit"), {audit_path}) if "__audit" in family_paths else []
        self.owner.recording_summary = audit[0] if audit else BinaryWriter(self.root, audit_path).close()
        for family, paths in sorted(family_paths.items()):
            inventory.extend(self.write_family(family, paths, expected))
        written = {item["path"] for item in inventory}
        for relative in sorted(expected - written):
            inventory.append(BinaryWriter(self.root, relative, xml=relative.endswith("CustomerMgmt.xml")).close())
        return sorted(inventory, key=lambda item: item["path"])
