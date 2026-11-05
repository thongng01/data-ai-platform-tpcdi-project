"""Record source errors without changing the correct business fields or ledger."""

from datetime import date, timedelta
from decimal import Decimal
import json
from types import MappingProxyType

from .random_values import uniform


AUDIT_PATH = "summary/data_errors.jsonl"
CHANNELS = ("WEB", "MOBILE", "BROKER")
# Retirees contact brokers more often; frequent traders prefer direct channels.
CHANNEL_SHARES = ((.45, .25, .30), (.50, .35, .15), (.20, .10, .70), (.55, .40, .05), (.45, .53, .02))
ERROR_RATES = {"C_TIER": "customer_tier_error_rate", "C_DOB": "customer_dob_error_rate",
               "SPrating": "company_rating_error_rate", "T_CHRG": "trade_charge_error_rate",
               "T_COMM": "trade_commission_error_rate"}


def _day(config, day) -> date:
    """Normalize the simulation's offset and public date forms."""
    return config.start + timedelta(days=day) if type(day) is int else date.fromisoformat(day) if isinstance(day, str) else day


def _identity(source, fields, kind="", timestamp="") -> dict[str, str]:
    """Build an identity from fields that recording errors never change."""
    if source == "CustomerMgmt":
        return {"C_ID": str(fields["C_ID"]), "ActionType": kind, "ActionTS": timestamp}
    names = {"Customer": ("C_ID", "C_ST_ID"), "Trade": ("T_ID", "T_DTS", "T_ST_ID"),
             "FINWIRE:CMP": ("CIK", "PTS")}[source]
    return {name: str(fields[name]) for name in names}


def _selected(config, source, day, identity, field) -> bool:
    """Select each error separately from stable entity and source-version identity."""
    identifier = int(identity.get("C_ID", identity.get("T_ID", identity.get("CIK", 0))))
    kind = identity.get("ActionType", identity.get("C_ST_ID", identity.get("T_ST_ID", "")))
    purpose = f"source-error/{source}/{field}/{day}/{kind}"
    return uniform(identifier, config.seed, purpose) < getattr(config, ERROR_RATES[field])


def _incorrect(field, correct_fields, day) -> str:
    """Return a format-valid value that violates the selected business rule."""
    if field == "C_TIER":
        return "9"
    if field == "C_DOB":
        return (day + timedelta(days=1)).isoformat()
    if field == "SPrating":
        return "BAD"
    value = Decimal(str(correct_fields["T_TRADE_PRICE"])) * int(correct_fields["T_QTY"])
    incorrect = value + Decimal("0.01")
    if incorrect > Decimal("99999999.99"):
        raise ValueError("A fee error above this trade value cannot fit VALUE_T; reduce the configured trade wealth or disable this fee error.")
    return f"{incorrect:.2f}"


class SourceRecorder:
    """Apply source-only changes to immutable business values and spool their audit."""

    def __init__(self, generation, transform=None):
        """Keep the legacy three-argument custom transform for test recorders."""
        self.generation = generation
        self.transform = transform

    def _recorded(self, source, day, fields, *, identity=None):
        """Copy correct fields, apply independent faults, and save exact originals."""
        correct = dict(fields)
        recorded = dict(correct)
        config = getattr(self.generation, "config", None)
        if config is not None:
            day = _day(config, day)
            candidates = ("C_TIER", "C_DOB") if source in ("Customer", "CustomerMgmt") else ("SPrating",) if source == "FINWIRE:CMP" else ("T_CHRG", "T_COMM") if source == "Trade" and correct.get("T_ST_ID") == "CMPT" else ()
            if candidates:
                identity = identity or _identity(source, correct)
                for field in candidates:
                    if field in correct and correct[field] not in (None, "") and _selected(config, source, day, identity, field):
                        recorded[field] = _incorrect(field, correct, day)
                        self.generation.record_fault({"source": source, "day": str(day), "identity": identity,
                                                      "field": field, "correct": str(correct[field]), "recorded": recorded[field]})
        if self.transform is not None:
            # A custom transform sees no mutable business mapping. It remains free
            # to return test corruption, which the independent validator rejects.
            recorded = dict(self.transform(source, day, MappingProxyType(recorded)))
        return recorded

    def record(self, source, day, fields, key, flag="I", context=None):
        """Record one ordinary source row; never mutate the caller's fields."""
        traits = MappingProxyType(dict(context or {}))
        recorded = self._recorded(source, day, fields)
        config = getattr(self.generation, "config", None)
        if source == "Trade" and config is not None:
            calendar_day = _day(config, day)
            if calendar_day > config.cutover and str(calendar_day) >= config.trade_channel_start:
                profile = int(traits.get("profile", 1))
                if not 0 <= profile < len(CHANNEL_SHARES):
                    raise ValueError("A trade channel requires a known behavior profile.")
                draw = uniform(int(fields["T_ID"]), config.seed, "source/trade-channel")
                web, mobile, _ = CHANNEL_SHARES[profile]
                recorded["T_CHANNEL"] = CHANNELS[0 if draw < web else 1 if draw < web + mobile else 2]
        self.generation.emit(source, day, [{**recorded, "_key": key, "_flag": flag}])

    def xml_action(self, kind, timestamp, customer_id, customer_fields, account_fields, key):
        """Record a CMS action through the same error seam as daily customers."""
        from .customers import xml_action

        correct = {**(customer_fields or {}), "C_ID": customer_id}
        day = date.fromisoformat(str(timestamp)[:10])
        identity = _identity("CustomerMgmt", correct, kind, str(timestamp))
        recorded = self._recorded("CustomerMgmt", day, correct, identity=identity)
        accounts = [self._recorded("Account", day, fields) for fields in account_fields or ()]
        self.generation.text("offline/initial/CustomerMgmt.xml",
                             [(key, xml_action(kind, timestamp, customer_id, recorded, accounts))])

    def finwire(self, kind, day, fields, key, relative, layout):
        """Record released company, security, and financial fields before padding."""
        from .market import _fixed

        recorded = self._recorded(f"FINWIRE:{kind}", day, fields)
        self.generation.text(relative, [(key, _fixed(layout, recorded))])
