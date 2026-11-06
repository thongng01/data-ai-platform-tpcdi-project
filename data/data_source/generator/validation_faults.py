"""Verify each configured source error, then restore correct values for replay."""

from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path
import sqlite3

from .random_values import uniform


class FaultAudit:
    """Index fault evidence on disk so memory does not grow with run history."""

    def __init__(self, root: Path, temporary: Path, config, require):
        """Load exact audit records and reject duplicate or unsupported evidence."""
        self.config = config
        self.require = require
        self.database = sqlite3.connect(temporary / "faults.sqlite")
        self.database.execute("CREATE TABLE faults (identity TEXT, field TEXT, correct TEXT, recorded TEXT, used INTEGER DEFAULT 0, PRIMARY KEY(identity, field))")
        self.count = 0
        self.measurements = {field: {delivery: {"eligible": 0, "errors": 0} for delivery in ("initial", "daily")}
                             for field in ("C_TIER", "C_DOB", "SPrating", "T_CHRG", "T_COMM")}
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
                    key = self._key(entry["source"], entry["day"], entry["identity"])
                    try:
                        self.database.execute("INSERT INTO faults(identity, field, correct, recorded) VALUES (?, ?, ?, ?)",
                                              (key, entry["field"], entry["correct"], entry["recorded"]))
                    except sqlite3.IntegrityError:
                        require(False, "Duplicate fault audit identity and field.")
                    self.count += 1
        self.database.commit()

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
        rates = {"C_TIER": self.config.customer_tier_error_rate, "C_DOB": self.config.customer_dob_error_rate,
                 "SPrating": self.config.company_rating_error_rate, "T_CHRG": self.config.trade_charge_error_rate,
                 "T_COMM": self.config.trade_commission_error_rate}
        identifier = int(identity.get("C_ID", identity.get("T_ID", identity.get("CIK", 0))))
        discriminator = identity.get("ActionType", identity.get("C_ST_ID", identity.get("T_ST_ID", "")))
        key = self._key(source, day, identity)
        for field in candidates:
            if field not in row or row[field] in (None, ""):
                continue
            # Recompute selection here, independently of the recorder. Evidence
            # cannot authorize a fault on a row that the configured draw excludes.
            chosen = uniform(identifier, self.config.seed, f"source-error/{source}/{field}/{day}/{discriminator}") < rates[field]
            delivery = "initial" if str(day) <= self.config.initial_end else "daily"
            bucket = self.measurements[field][delivery]
            bucket["eligible"] += 1
            bucket["errors"] += int(chosen)
            if not chosen:
                continue
            evidence = self.database.execute("SELECT correct, recorded, used FROM faults WHERE identity=? AND field=?", (key, field)).fetchone()
            self.require(evidence is not None, f"Missing fault audit for {source}.{field}.")
            correct, recorded, used = evidence
            self.require(not used, "A fault audit entry was consumed more than once.")
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
            self.database.execute("UPDATE faults SET used=1 WHERE identity=? AND field=?", (key, field))
        return row

    def report(self):
        """Report eligible rows, measured errors, and rates for each error type."""
        result = {}
        rates = {"C_TIER": self.config.customer_tier_error_rate, "C_DOB": self.config.customer_dob_error_rate,
                 "SPrating": self.config.company_rating_error_rate, "T_CHRG": self.config.trade_charge_error_rate,
                 "T_COMM": self.config.trade_commission_error_rate}
        for field, deliveries in sorted(self.measurements.items()):
            totals = {name: sum(bucket[name] for bucket in deliveries.values()) for name in ("eligible", "errors")}
            result[field] = {**totals, "configured_rate": rates[field], "rate": totals["errors"] / totals["eligible"] if totals["eligible"] else None,
                             "deliveries": {delivery: {**bucket, "rate": bucket["errors"] / bucket["eligible"] if bucket["eligible"] else None}
                                            for delivery, bucket in deliveries.items()}}
        return result

    def finish(self):
        """Reject extra audit entries that no parsed source row consumed."""
        remaining = self.database.execute("SELECT COUNT(*) FROM faults WHERE used=0").fetchone()[0]
        self.require(remaining == 0, "Unused fault audit evidence has no configured source fault.")

    def close(self):
        """Release the temporary disk index before its directory is removed."""
        self.database.close()
