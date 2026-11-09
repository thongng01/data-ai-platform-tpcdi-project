"""Verify each configured source error, then restore correct values for replay."""

from datetime import date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path

from .random_values import uniform


class FaultAudit:
    """Check sparse read-only evidence with separate consumption and measurement state."""

    def __init__(self, config, require, entries):
        """Attach immutable evidence and start this reader's one-time-use checks."""
        self.config = config
        self.require = require
        # Readers never change entries. Spawned workers receive their own copy;
        # only the coordinator combines their consumed keys and measurements.
        self.entries = entries
        self.used = set()
        self.count = len(entries)
        self.rates = {"C_TIER": config.customer_tier_error_rate, "C_DOB": config.customer_dob_error_rate,
                      "SPrating": config.company_rating_error_rate, "T_CHRG": config.trade_charge_error_rate,
                      "T_COMM": config.trade_commission_error_rate}
        self.measurements = {field: {delivery: {"eligible": 0, "errors": 0} for delivery in ("initial", "daily")}
                             for field in ("C_TIER", "C_DOB", "SPrating", "T_CHRG", "T_COMM")}

    @classmethod
    def load(cls, root: Path, config, require):
        """Load exact audit records and reject duplicate or unsupported evidence."""
        entries = {}
        path = root / "summary/data_errors.jsonl"
        if path.exists():
            require(not path.is_symlink(), "Fault audit must not be a symbolic link.")
            with path.open(encoding="ascii") as stream:
                for line in stream:
                    entry = json.loads(line)
                    require(set(entry) == {"source", "day", "identity", "field", "correct", "recorded"}, "Unexpected fault audit fields.")
                    allowed = {"Customer": {"C_TIER", "C_DOB"}, "CustomerMgmt": {"C_TIER", "C_DOB"},
                               "FINWIRE:CMP": {"SPrating"}, "Trade": {"T_CHRG", "T_COMM"}}
                    require(entry["field"] in allowed.get(entry["source"], set()), "Unsupported fault audit source or field.")
                    require(isinstance(entry["identity"], dict) and all(type(value) is str for value in entry["identity"].values()), "Invalid fault audit identity.")
                    require(type(entry["correct"]) is str and type(entry["recorded"]) is str and entry["correct"] != entry["recorded"], "Fault audit must contain distinct text values.")
                    key = cls._key(entry["source"], entry["day"], entry["identity"]), entry["field"]
                    require(key not in entries, "Duplicate fault audit identity and field.")
                    entries[key] = entry["correct"], entry["recorded"]
        return cls(config, require, entries)

    def reader(self, entries=None):
        """Share selected evidence without sharing a file's mutable consumption state."""
        return FaultAudit(self.config, self.require, self.entries if entries is None else entries)

    def slices(self, expected):
        """Route evidence to expected source files without discarding unmatched entries."""
        result = {}
        for key, evidence in self.entries.items():
            source, day_text, identity = json.loads(key[0])
            try:
                day = date.fromisoformat(day_text)
                if source == "CustomerMgmt":
                    relative = "offline/initial/CustomerMgmt.xml" if self.config.start <= day <= self.config.cutover else None
                elif source == "FINWIRE:CMP":
                    posted = datetime.strptime(identity["PTS"], "%Y%m%d-%H%M%S")
                    folder = "offline/initial" if day <= self.config.cutover else f"offline/daily/{day:%Y%m%d}"
                    relative = f"{folder}/FINWIRE{posted.year}Q{(posted.month - 1) // 3 + 1}" if posted.date() == day else None
                elif source in {"Customer", "Trade"}:
                    if source == "Trade" and self.config.start <= day <= self.config.cutover:
                        relative = "offline/initial/Trade.txt"
                    else:
                        relative = f"offline/daily/{day:%Y%m%d}/{source}_{day:%Y%m%d}.txt"
                else:
                    relative = None
            except (KeyError, TypeError, ValueError):
                relative = None
            if relative in expected:
                result.setdefault(relative, {})[key] = evidence
        return result

    def release(self):
        """Save final measurements and completeness, then release parse-only proof graphs."""
        result = self.count, self.report(), len(self.used) == self.count
        self.entries.clear()
        self.used.clear()
        return result

    def accept(self, used, measurements):
        """Combine one file's verified keys and exact counts in source-file order."""
        self.require(not self.used.intersection(used), "A fault audit entry was consumed more than once.")
        self.require(used <= self.entries.keys(), "A worker consumed unknown fault audit evidence.")
        self.used.update(used)
        for field, deliveries in measurements.items():
            for delivery, bucket in deliveries.items():
                for name, count in bucket.items():
                    self.measurements[field][delivery][name] += count

    @staticmethod
    def _key(source, day, identity):
        """Use canonical source identity rather than trusting the export sort key."""
        return json.dumps([source, str(day), identity], sort_keys=True, separators=(",", ":"))

    def restore(self, source, day, fields, *, kind="", timestamp="", account_id=None):
        """Require exact configured faults; return a separate correct-value row."""
        row = dict(fields)
        if source == "CustomerMgmt":
            identity = {"C_ID": str(row["C_ID"]), "ActionType": kind, "ActionTS": timestamp}
            if kind in {"ADDACCT", "UPDACCT", "CLOSEACCT"}:
                identity["CA_ID"] = str(account_id)
            candidates = ("C_TIER", "C_DOB")
        elif source == "Customer":
            identity = {name: str(row[name]) for name in ("C_ID", "C_ST_ID")}
            identity["CDC_FLAG"] = row["_flag"]
            candidates = ("C_TIER", "C_DOB")
        elif source == "FINWIRE:CMP":
            identity = {"CIK": str(int(row["CIK"])), "PTS": row["PTS"]}
            candidates = ("SPrating",)
        elif source == "Trade" and row["T_ST_ID"] == "CMPT":
            identity = {name: str(row[name]) for name in ("T_ID", "T_DTS", "T_ST_ID")}
            candidates = ("T_CHRG", "T_COMM")
        else:
            return row
        identifier = int(identity.get("C_ID", identity.get("T_ID", identity.get("CIK", 0))))
        discriminator = identity.get("ActionType", identity.get("C_ST_ID", identity.get("T_ST_ID", "")))
        identity_key = None
        for field in candidates:
            if field not in row or row[field] in (None, ""):
                continue
            # Recompute selection here, independently of the recorder. Evidence
            # cannot authorize a fault on a row that the configured draw excludes.
            chosen = uniform(identifier, self.config.seed, f"source-error/{source}/{field}/{day}/{discriminator}") < self.rates[field]
            delivery = "initial" if str(day) <= self.config.initial_end else "daily"
            bucket = self.measurements[field][delivery]
            bucket["eligible"] += 1
            bucket["errors"] += int(chosen)
            if not chosen:
                continue
            if identity_key is None:
                identity_key = self._key(source, day, identity)
            key = identity_key, field
            evidence = self.entries.get(key)
            self.require(evidence is not None, f"Missing fault audit for {source}.{field}.")
            correct, recorded = evidence
            self.require(key not in self.used, "A fault audit entry was consumed more than once.")
            self.require(str(row[field]) == recorded, f"Recorded fault differs from audit: {source}.{field}.")
            if field == "C_TIER":
                expected = "9"
                self.require(correct in {"1", "2", "3"}, "Fault audit has an invalid correct customer tier.")
            elif field == "C_DOB":
                expected = (date.fromisoformat(str(day)) + timedelta(days=1)).isoformat()
                born = date.fromisoformat(correct)
                current = date.fromisoformat(str(day))
                self.require(born <= current and current.year - born.year <= 120, "Fault audit has an invalid correct birth date.")
            elif field == "SPrating":
                expected = "BAD"
                self.require(correct == "AAA", "Fault audit has an invalid correct company rating.")
            else:
                value = Decimal(str(row["T_TRADE_PRICE"])) * int(row["T_QTY"])
                expected = f"{value + Decimal('0.01'):.2f}"
                price_cents = Decimal(str(row["T_TRADE_PRICE"])) * 100
                fee_cents = self.config.charge_cents if field == "T_CHRG" else (int(row["T_QTY"]) * int(price_cents) * self.config.commission_basis_points + 5000) // 10000
                self.require(Decimal(correct) * 100 == fee_cents, "Fault audit correct fee differs from the business rule.")
            self.require(recorded == expected, f"Fault audit does not describe the configured mutation: {source}.{field}.")
            row[field] = correct
            self.used.add(key)
        return row

    def report(self):
        """Report eligible rows, measured errors, and rates for each error type."""
        result = {}
        for field, deliveries in sorted(self.measurements.items()):
            totals = {name: sum(bucket[name] for bucket in deliveries.values()) for name in ("eligible", "errors")}
            result[field] = {**totals, "configured_rate": self.rates[field], "rate": totals["errors"] / totals["eligible"] if totals["eligible"] else None,
                             "deliveries": {delivery: {**bucket, "rate": bucket["errors"] / bucket["eligible"] if bucket["eligible"] else None}
                                            for delivery, bucket in deliveries.items()}}
        return result

    def finish(self):
        """Reject extra audit entries that no parsed source row consumed."""
        self.require(len(self.used) == self.count, "Unused fault audit evidence has no configured source fault.")
