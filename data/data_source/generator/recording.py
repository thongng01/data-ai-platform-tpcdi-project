"""Record source errors without changing the correct business fields or ledger."""

from datetime import date, datetime, timedelta
from types import MappingProxyType

import numpy as np
import polars as pl

from .formats import trade_channel_day
from .money import cents_text
from .random_values import uniform, uniforms


AUDIT_PATH = "summary/data_errors.jsonl"
CHANNELS = ("WEB", "MOBILE", "BROKER")
# Retirees contact brokers more often; frequent traders prefer direct channels.
CHANNEL_SHARES = ((.45, .25, .30), (.50, .35, .15), (.20, .10, .70), (.55, .40, .05), (.45, .53, .02))
ERROR_RATES = {"C_TIER": "customer_tier_error_rate", "C_DOB": "customer_dob_error_rate",
               "SPrating": "company_rating_error_rate", "T_CHRG": "trade_charge_error_rate",
               "T_COMM": "trade_commission_error_rate"}
EPOCH_DATE = date(1970, 1, 1)
EPOCH_DATETIME = datetime(1970, 1, 1)
MAX_FEE_CENTS = 9_999_999_999


def _day(config, day) -> date:
    """Normalize the simulation's offset and public date forms."""
    return config.start + timedelta(days=day) if type(day) is int else date.fromisoformat(day) if isinstance(day, str) else day


def _identity(source, fields, kind="", timestamp="", account_id=None) -> dict[str, str]:
    """Identify a source record with unchanged fields visible in its saved delivery."""
    if source == "CustomerMgmt":
        identity = {"C_ID": str(fields["C_ID"]), "ActionType": kind, "ActionTS": timestamp}
        if kind in {"ADDACCT", "UPDACCT", "CLOSEACCT"}:
            identity["CA_ID"] = str(account_id)
        return identity
    if source == "Customer":
        return {"C_ID": str(fields["C_ID"]), "C_ST_ID": str(fields["C_ST_ID"]),
                "CDC_FLAG": str(fields["_flag"])}
    return {name: str(fields[name]) for name in ("CIK", "PTS")}


def _selected(config, source, day, identity, field) -> bool:
    """Select a scalar CMS or FINWIRE error from its entity and action identity."""
    identifier = int(identity.get("C_ID", identity.get("CIK", 0)))
    kind = identity.get("ActionType", "")
    purpose = f"source-error/{source}/{field}/{day}/{kind}"
    return uniform(identifier, config.seed, purpose) < getattr(config, ERROR_RATES[field])


def _incorrect(field, day) -> str:
    """Return a format-valid value that violates the selected business rule."""
    if field == "C_TIER":
        return "9"
    if field == "C_DOB":
        return (day + timedelta(days=1)).isoformat()
    if field == "SPrating":
        return "BAD"
    raise ValueError(f"Unsupported scalar source error field: {field}")


class SourceRecorder:
    """Apply source-only changes to immutable business values and spool their audit."""

    def __init__(self, generation, transform=None):
        """Attach one batch recorder; an optional transform is a bounded test hook."""
        self.generation = generation
        self.transform = transform

    def _selection(self, source, frame, field):
        """Select errors in arrays while preserving each named scalar draw exactly."""
        config = self.generation.config
        eligible = frame[field].is_not_null().to_numpy().copy()
        if source == "Trade":
            eligible &= frame["T_ST_ID"].to_numpy() == "CMPT"
        selected = np.zeros(frame.height, dtype=np.bool_)
        rate = getattr(config, ERROR_RATES[field])
        if not rate or not np.any(eligible):
            return selected
        identifiers = frame["T_ID" if source == "Trade" else "C_ID"].to_numpy()
        days = frame["_record_day"].to_numpy()
        kinds = frame["T_ST_ID" if source == "Trade" else "C_ST_ID"].to_numpy()
        # A buffer can cover several dates and statuses. Group only the random
        # purpose, not the draw stream: every entity keeps its original key.
        for offset in np.unique(days[eligible]):
            on_day = eligible & (days == offset)
            for kind in np.unique(kinds[on_day]):
                positions = np.flatnonzero(on_day & (kinds == kind))
                day = config.start + timedelta(days=int(offset))
                purpose = f"source-error/{source}/{field}/{day}/{kind}"
                selected[positions] = uniforms(identifiers[positions], config.seed, purpose) < rate
        return selected

    @staticmethod
    def _audit_value(field, value):
        """Render selected audit values in the exact source units, not storage units."""
        if field == "C_DOB":
            return (EPOCH_DATE + timedelta(days=int(value))).isoformat()
        if field in ("T_CHRG", "T_COMM"):
            return cents_text(value)
        return str(value)

    def _batch_audit(self, source, correct, field, selected, replacement):
        """Build dictionaries only for selected errors, never for ordinary rows."""
        columns = ["_record_day", field, "T_ID", "T_DTS", "T_ST_ID"] if source == "Trade" else [
            "_record_day", field, "C_ID", "C_ST_ID", "_flag"]
        evidence = correct.select(columns).with_columns(replacement.alias("_recorded")).filter(pl.Series(selected))
        entries = []
        for row in evidence.iter_rows(named=True):
            if source == "Trade":
                stamp = EPOCH_DATETIME + timedelta(seconds=int(row["T_DTS"]))
                identity = {"T_ID": str(row["T_ID"]), "T_DTS": stamp.isoformat(sep=" "),
                            "T_ST_ID": row["T_ST_ID"]}
            else:
                identity = _identity(source, row)
            day = self.generation.config.start + timedelta(days=int(row["_record_day"]))
            entries.append({"source": source, "day": str(day), "identity": identity, "field": field,
                            "correct": self._audit_value(field, row[field]),
                            "recorded": self._audit_value(field, row["_recorded"])})
        return entries

    def _transform_batch(self, source, frame):
        """Expose immutable typed rows only when a test explicitly supplies a hook."""
        fields = [name for name in frame.columns if not name.startswith("_")]
        changed = []
        days = frame["_record_day"].to_list()
        for offset, row in zip(days, frame.select(fields).iter_rows(named=True), strict=True):
            day = self.generation.config.start + timedelta(days=int(offset))
            changed.append(dict(self.transform(source, day, MappingProxyType(row))))
        return frame.with_columns(
            pl.Series(name, [row.get(name) for row in changed], dtype=frame.schema[name]) for name in fields)

    def apply(self, source: str, frame: pl.DataFrame) -> tuple[pl.DataFrame, list[dict]]:
        """Record one admitted typed batch without changing its correct values or metadata."""
        recorded = frame.clone()
        audit = []
        candidates = ("C_TIER", "C_DOB") if source == "Customer" else ("T_CHRG", "T_COMM") if source == "Trade" else ()
        for field in candidates:
            if field not in frame.columns:
                continue
            selected = self._selection(source, frame, field)
            if not np.any(selected):
                continue
            mask = pl.Series(selected)
            if field == "C_TIER":
                incorrect = pl.lit(9)
            elif field == "C_DOB":
                incorrect = pl.col("_record_day") + (self.generation.config.start - EPOCH_DATE).days + 1
            else:
                # The product can exceed Int64 before the VALUE_T bound check.
                # Int128 keeps the full trade value until that check rejects it.
                incorrect = pl.col("T_TRADE_PRICE").cast(pl.Int128) * pl.col("T_QTY").cast(pl.Int128) + 1
                if frame.filter(mask).select((incorrect > MAX_FEE_CENTS).any()).item():
                    raise ValueError("A fee error above this trade value cannot fit VALUE_T; reduce the configured trade wealth or disable this fee error.")
            replacement = frame.select(
                pl.when(mask).then(incorrect).otherwise(pl.col(field)).cast(frame.schema[field]).alias(field)
            )[field]
            audit.extend(self._batch_audit(source, frame, field, selected, replacement))
            recorded = recorded.with_columns(replacement)
        if self.transform is not None and frame.height:
            recorded = self._transform_batch(source, recorded)
        if source == "Trade" and frame.height:
            config = self.generation.config
            days = frame["_record_day"].to_numpy()
            first_channel_day = (trade_channel_day(config) - config.start).days
            eligible = days >= first_channel_day
            profiles = frame["_profile"].to_numpy() if "_profile" in frame.columns else np.ones(frame.height, dtype=np.int64)
            if np.any(eligible & ((profiles < 0) | (profiles >= len(CHANNEL_SHARES)))):
                raise ValueError("A trade channel requires a known behavior profile.")
            channels = np.full(frame.height, None, dtype=object)
            if np.any(eligible):
                shares = np.asarray(CHANNEL_SHARES)[profiles[eligible]]
                draws = uniforms(frame["T_ID"].to_numpy()[eligible], config.seed, "source/trade-channel")
                channels[eligible] = np.asarray(CHANNELS)[np.where(
                    draws < shares[:, 0], 0, np.where(draws < shares[:, 0] + shares[:, 1], 1, 2))]
            recorded = recorded.with_columns(pl.Series("T_CHANNEL", channels.tolist(), dtype=pl.String))
        return recorded, audit

    def _recorded(self, source, day, fields, *, identity=None):
        """Copy correct fields, apply independent faults, and save exact originals."""
        correct = dict(fields)
        recorded = dict(correct)
        config = getattr(self.generation, "config", None)
        if config is not None:
            day = _day(config, day)
            candidates = ("C_TIER", "C_DOB") if source == "CustomerMgmt" else ("SPrating",) if source == "FINWIRE:CMP" else ()
            if candidates:
                identity = identity or _identity(source, correct)
                for field in candidates:
                    if field in correct and correct[field] not in (None, "") and _selected(config, source, day, identity, field):
                        recorded[field] = _incorrect(field, day)
                        self.generation._record_fault({"source": source, "day": str(day), "identity": identity,
                                                       "field": field, "correct": str(correct[field]), "recorded": recorded[field]})
        if self.transform is not None:
            # A custom transform sees no mutable business mapping. It remains free
            # to return test corruption, which the independent validator rejects.
            recorded = dict(self.transform(source, day, MappingProxyType(recorded)))
        return recorded

    def xml_action(self, kind, timestamp, customer_id, customer_fields, account_fields, *, ordinal=0):
        """Record a CMS action through the same error seam as daily customers."""
        from .customers import xml_action

        correct = {**(customer_fields or {}), "C_ID": customer_id}
        day = date.fromisoformat(str(timestamp)[:10])
        account_fields = list(account_fields or ())
        account_id = int(account_fields[0]["CA_ID"]) if len(account_fields) == 1 else None
        if kind in {"ADDACCT", "UPDACCT", "CLOSEACCT"} and account_id is None:
            raise ValueError("An account-specific CMS action requires exactly one account.")
        identity = _identity("CustomerMgmt", correct, kind, str(timestamp), account_id)
        recorded = self._recorded("CustomerMgmt", day, correct, identity=identity)
        accounts = [self._recorded("Account", day, fields) for fields in account_fields or ()]
        # A single-account action uses its CA_ID only to break an existing
        # customer-action tie. Actions with zero or multiple accounts use zero.
        account_id = int(accounts[0]["CA_ID"]) if len(accounts) == 1 else 0
        stamp = datetime.fromisoformat(str(timestamp))
        epoch_seconds = (stamp - EPOCH_DATETIME).days * 86400 + stamp.hour * 3600 + stamp.minute * 60 + stamp.second
        self.generation.record("CustomerMgmt", (day - self.generation.config.start).days,
                               (xml_action(kind, timestamp, customer_id, recorded, accounts),),
                               order=(epoch_seconds, int(customer_id), int(ordinal), kind, account_id))

    def finwire(self, kind, day, fields, *, layout):
        """Record released company, security, and financial fields before padding."""
        from .market import _fixed

        recorded = self._recorded(f"FINWIRE:{kind}", day, fields)
        natural_id = int(fields["CIK"]) if kind == "CMP" else int(str(fields["Symbol"])[1:]) if kind == "SEC" else int(fields["CoNameOrCIK"])
        offset = (_day(self.generation.config, day) - self.generation.config.start).days
        self.generation.record("FINWIRE", offset, (_fixed(layout, recorded),),
                               order=(fields["PTS"], ("CMP", "SEC", "FIN").index(kind), natural_id))
