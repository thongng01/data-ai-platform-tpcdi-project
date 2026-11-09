"""Check saved dataset identity, file cadence, hashes, and cash transaction bytes."""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from .errors import PublisherError

METADATA_LIMIT = 32 * 1024 * 1024
COPY_BYTES = 1024 * 1024
MESSAGE_LIMIT = 64 * 1024
CASH_FIELDS = ("ct_id", "ct_ca_id", "ct_dts", "ct_amt", "ct_name", "schema_version")
INITIAL_FILES = (
    "CustomerMgmt.xml", "Trade.txt", "TradeHistory.txt", "CashTransaction.txt",
    "HoldingHistory.txt", "WatchHistory.txt", "DailyMarket.txt", "HR.csv", "Prospect.csv",
    "Date.txt", "Time.txt", "Industry.txt", "StatusType.txt", "TaxRate.txt", "TradeType.txt",
)
DAILY_SOURCES = ("Customer", "Account", "Trade", "HoldingHistory", "WatchHistory", "DailyMarket")


@dataclass(frozen=True)
class SourceFile:
    """Describe one manifest-identified saved source file."""

    path: str
    rows: int
    size: int
    sha256: str

    @property
    def object_key(self):
        """Remove only the offline prefix, never add a dataset prefix."""
        return self.path.removeprefix("offline/")


@dataclass(frozen=True)
class CashMessage:
    """Keep the original JSON bytes beside parsed routing and scheduling fields."""

    payload: bytes
    ct_id: int
    key: bytes
    event_time: datetime
    end_offset: int


def read_json(path, limit=METADATA_LIMIT):
    """Read bounded JSON metadata and reject duplicate keys and nonfinite numbers."""
    def unique_object(pairs):
        """Reject duplicate keys instead of silently keeping the last value."""
        result = {}
        for key, value in pairs:
            if key in result:
                raise PublisherError("JSON metadata contains a duplicate key.")
            result[key] = value
        return result

    def invalid_constant(value):
        """Reject the nonstandard NaN and Infinity JSON extensions."""
        raise PublisherError("JSON metadata contains a nonfinite number.")

    try:
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise PublisherError("JSON metadata exceeds its size limit.")
        return json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    except (OSError, ValueError, UnicodeError):
        raise PublisherError("Cannot read valid JSON metadata.") from None


def expected_paths(config):
    """Derive complete source cadence from dates, independently of generator code."""
    try:
        start, cutover, daily_start, end = (
            date.fromisoformat(config[name])
            for name in ("initial_start", "initial_end", "daily_start", "daily_end")
        )
        if not start <= cutover < daily_start <= end or daily_start != cutover + timedelta(days=1):
            raise ValueError
        if start.year < 1902 or end.year > 2100:
            raise ValueError
    except (ValueError, TypeError, KeyError):
        raise PublisherError("Manifest dates must be adjacent inclusive periods within 1902 to 2100.") from None
    paths = {f"offline/initial/{name}" for name in INITIAL_FILES}
    first_quarter = start.year * 4 + (start.month - 1) // 3 - 5
    last_quarter = end.year * 4 + (end.month - 1) // 3
    first_year, first_number = divmod(first_quarter, 4)
    paths.add(f"offline/initial/FINWIRE{first_year}Q{first_number + 1}")
    for quarter in range(first_quarter, last_quarter + 1):
        next_year, next_number = divmod(quarter + 1, 4)
        released = date(next_year, next_number * 3 + 2, 1) - timedelta(days=1)
        if released > end:
            continue
        folder = "offline/initial" if released <= cutover else f"offline/daily/{released:%Y%m%d}"
        paths.add(f"{folder}/FINWIRE{released.year}Q{(released.month - 1) // 3 + 1}")
    current = daily_start
    while current <= end:
        stamp = current.strftime("%Y%m%d")
        paths.update(f"offline/daily/{stamp}/{source}_{stamp}.txt" for source in DAILY_SOURCES)
        if current.day == 1:
            paths.add(f"offline/daily/{stamp}/Prospect_{stamp}.csv")
        paths.add(f"stream/cash_transactions/{stamp}.jsonl")
        current += timedelta(days=1)
    return paths, daily_start, end


def message_from_line(line, day, offset):
    """Check compact ordered JSON, then keep its bytes unchanged as the Kafka value."""
    try:
        pairs = json.loads(line, object_pairs_hook=tuple)
        if not isinstance(pairs, tuple) or tuple(key for key, _ in pairs) != CASH_FIELDS:
            raise ValueError
        value = dict(pairs)
        for field in ("ct_id", "ct_ca_id", "schema_version"):
            if type(value[field]) is not int or value[field] <= 0:
                raise ValueError
        if value["schema_version"] != 1 or value["ct_ca_id"] >= 10**11:
            raise ValueError
        if not isinstance(value["ct_dts"], str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value["ct_dts"]):
            raise ValueError
        event_time = datetime.strptime(value["ct_dts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if event_time.date() != day:
            raise ValueError
        if not isinstance(value["ct_amt"], str) or not re.fullmatch(r"-?\d{1,8}\.\d{2}", value["ct_amt"]):
            raise ValueError
        if not isinstance(value["ct_name"], str) or not re.fullmatch(
            r"Deposit|Withdrawal|Trade settlement \d{1,15}|Dividend [A-Za-z0-9._-]{1,15}", value["ct_name"]
        ):
            raise ValueError
        if value["ct_name"].startswith("Trade settlement ") and int(value["ct_name"].split()[-1]) <= 0:
            raise ValueError
        canonical = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        if line != canonical:
            raise ValueError
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise PublisherError("Saved cash transaction violates the ordered JSON contract.") from None
    return CashMessage(line[:-1], value["ct_id"], str(value["ct_ca_id"]).encode("ascii"), event_time, offset)


def cash_messages(stream, day, start_offset=0):
    """Read one bounded JSON line at a time, including a durable next-byte offset."""
    stream.seek(start_offset)
    previous = None
    while line := stream.readline(MESSAGE_LIMIT + 1):
        if len(line) > MESSAGE_LIMIT:
            raise PublisherError("Saved cash transaction exceeds the message size limit.")
        message = message_from_line(line, day, stream.tell())
        order = (message.event_time, message.ct_id)
        if previous is not None and order <= previous:
            raise PublisherError("Saved cash transactions are not in strict event order.")
        previous = order
        yield message


class Dataset:
    """Check validation policy, then verify only the public source-file trees."""

    def __init__(self, root, *, allow_unvalidated=False):
        """Check metadata and exact cadence before any remote client is opened."""
        self.root = Path(root).resolve()
        if self.root.name.endswith(".partial"):
            raise PublisherError("A partial generator run cannot be published.")
        self._regular_path("manifest.json")
        manifest = read_json(self.root / "manifest.json")
        self.generator_status = manifest.get("status") if isinstance(manifest, dict) else None
        self.allow_unvalidated = allow_unvalidated
        accepted = ("validated", "generated-unvalidated") if allow_unvalidated else ("validated",)
        if self.generator_status not in accepted:
            raise PublisherError("The generator manifest must have validated status; use --allow-unvalidated only for a generated-unvalidated dataset.")
        self.dataset_id = manifest.get("dataset_id")
        if not isinstance(self.dataset_id, str) or not re.fullmatch(r"[0-9a-f]{64}", self.dataset_id):
            raise PublisherError("The manifest must contain a SHA-256 dataset identity.")
        expected, self.daily_start, self.daily_end = expected_paths(manifest.get("config", {}))
        self.files = self._inventory(manifest.get("files"), expected)
        self._check_actual_paths(expected)
        # Bind resume to content as well as semantic identity. A recorder version can
        # change source bytes without changing the generator's business dataset ID.
        evidence = [(item.path, item.rows, item.size, item.sha256) for item in self.files.values()]
        self.fingerprint = hashlib.sha256(json.dumps(sorted(evidence), separators=(",", ":")).encode()).hexdigest()

    def _regular_path(self, relative):
        """Reject links, directories, and special files beneath the dataset root."""
        path = self.root
        try:
            parts = Path(relative).parts
            for index, part in enumerate(parts):
                path = path / part
                mode = path.lstat().st_mode
                expected_type = stat.S_ISREG if index == len(parts) - 1 else stat.S_ISDIR
                if not expected_type(mode):
                    raise PublisherError("Source paths must use regular files and directories, not links.")
        except OSError:
            raise PublisherError("An expected source file is missing or unreadable.") from None
        return path

    @contextmanager
    def _open_source(self, relative):
        """Open each path component without following links, even during replacement."""
        directory = None
        source = None
        try:
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            parts = Path(relative).parts
            for part in parts[:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                os.close(directory)
                directory = child
            descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            source = os.fdopen(descriptor, "rb")
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise PublisherError("Source paths must be regular files.")
            yield source
        except OSError:
            raise PublisherError("Cannot open a regular source path without following links.") from None
        finally:
            if source is not None:
                source.close()
            if directory is not None:
                os.close(directory)

    def _inventory(self, inventory, expected):
        """Require one valid hash and nonnegative row and byte counts per expected path."""
        if not isinstance(inventory, list):
            raise PublisherError("Manifest files must be a list.")
        files = {}
        for entry in inventory:
            if not isinstance(entry, dict):
                raise PublisherError("Manifest contains an invalid file entry.")
            path = entry.get("path")
            if not isinstance(path, str) or path not in expected or path in files:
                raise PublisherError("Manifest file coverage does not match source cadence.")
            if any(type(entry.get(field)) is not int or entry[field] < 0 for field in ("rows", "bytes")):
                raise PublisherError("Manifest file counts must be nonnegative integers.")
            digest = entry.get("sha256")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise PublisherError("Manifest file hashes must use SHA-256 hex text.")
            files[path] = SourceFile(path, entry["rows"], entry["bytes"], digest)
        if files.keys() != expected:
            raise PublisherError("Manifest file coverage does not match source cadence.")
        return files

    def _check_actual_paths(self, expected):
        """Reject extra files and links instead of silently publishing a partial tree."""
        actual = set()
        for folder in ("offline", "stream"):
            base = self.root / folder
            if not base.is_dir() or base.is_symlink():
                raise PublisherError("Source trees must be regular directories.")
            for directory, directories, filenames in os.walk(base):
                for name in directories:
                    if (Path(directory) / name).is_symlink():
                        raise PublisherError("Source trees cannot contain symbolic links.")
                for name in filenames:
                    relative = (Path(directory) / name).relative_to(self.root).as_posix()
                    self._regular_path(relative)
                    actual.add(relative)
        if actual != expected:
            raise PublisherError("Actual source-file coverage does not match source cadence.")

    def _initial_cash_id(self, stream, item):
        """Check the initial cash sequence so the saved stream continues at the next ID."""
        stream.seek(0)
        count = 0
        while line := stream.readline(MESSAGE_LIMIT + 1):
            identity = line.partition(b"|")[0]
            if len(line) > MESSAGE_LIMIT or not line.endswith(b"\n") or not identity.isdigit():
                raise PublisherError("Initial cash transaction identity is invalid.")
            count += 1
            if int(identity) != count:
                raise PublisherError("Initial cash transaction identities must be contiguous from one.")
        if count != item.rows:
            raise PublisherError("Initial cash transaction row count does not match its manifest.")
        return count

    def verify(self):
        """Hash every file and validate every saved stream before the first delivery."""
        last_id = 0
        for item in sorted(self.files.values(), key=lambda value: value.path):
            with self._open_source(item.path) as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if os.fstat(stream.fileno()).st_size != item.size or digest != item.sha256:
                    raise PublisherError("Source bytes do not match their manifest hash and size.")
                if item.path == "offline/initial/CashTransaction.txt":
                    last_id = self._initial_cash_id(stream, item)
                if item.path.startswith("stream/"):
                    day = date.fromisoformat(Path(item.path).stem)
                    count = 0
                    for message in cash_messages(stream, day):
                        if message.ct_id != last_id + 1:
                            raise PublisherError("Saved stream cash transaction identities must continue the contiguous initial sequence.")
                        last_id = message.ct_id
                        count += 1
                    if count != item.rows:
                        raise PublisherError("Saved stream row count does not match its manifest.")
        return {"status": "verified", "dataset_id": self.dataset_id, "files": len(self.files),
                "generator_status": self.generator_status, "allow_unvalidated": self.allow_unvalidated,
                "offline_bytes": sum(item.size for item in self.files.values() if item.path.startswith("offline/")),
                "stream_bytes": sum(item.size for item in self.files.values() if item.path.startswith("stream/"))}

    @contextmanager
    def snapshot(self, item, directory):
        """Copy one file to a private bounded-memory snapshot and recheck its hash."""
        # Uploads read the verified private copy. An in-place source edit during a
        # remote send therefore cannot change the bytes already selected for it.
        with tempfile.TemporaryFile(dir=directory) as copy:
            digest = hashlib.sha256()
            size = 0
            with self._open_source(item.path) as source:
                while block := source.read(COPY_BYTES):
                    size += len(block)
                    if size > item.size:
                        raise PublisherError("Source file grew after preflight verification.")
                    digest.update(block)
                    copy.write(block)
            if size != item.size or digest.hexdigest() != item.sha256:
                raise PublisherError("Source file changed after preflight verification.")
            copy.seek(0)
            yield copy

    def initial_files(self):
        """Return initial objects in stable file-name order."""
        return sorted((item for item in self.files.values() if item.path.startswith("offline/initial/")), key=lambda item: item.path)

    def daily_files(self, day):
        """Return one date's objects with FINWIRE before dependent daily extracts."""
        prefix = f"offline/daily/{day:%Y%m%d}/"
        return sorted((item for item in self.files.values() if item.path.startswith(prefix)),
                      key=lambda item: (not Path(item.path).name.startswith("FINWIRE"), item.path))

    def daily_time(self, day, release):
        """Map the explicit daily-release setting to one UTC replay time."""
        if release not in ("start", "end"):
            raise PublisherError("Daily release must be start or end.")
        delivery_day = day if release == "start" else day + timedelta(days=1)
        return datetime.combine(delivery_day, time(), timezone.utc)
