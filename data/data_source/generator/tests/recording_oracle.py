"""Keep scalar Customer and Trade fault rules as a test-only batch oracle."""

from datetime import timedelta
from decimal import Decimal

from data.data_source.generator.random_values import uniform


RATES = {"C_TIER": "customer_tier_error_rate", "C_DOB": "customer_dob_error_rate",
         "SPrating": "company_rating_error_rate", "T_CHRG": "trade_charge_error_rate",
         "T_COMM": "trade_commission_error_rate"}


def scalar_selected(config, source, day, identity, field):
    """Reproduce each named scalar draw without using production batch selection."""
    identifier = int(identity.get("C_ID", identity.get("T_ID", identity.get("CIK", 0))))
    kind = identity.get("ActionType", identity.get("C_ST_ID", identity.get("T_ST_ID", "")))
    purpose = f"source-error/{source}/{field}/{day}/{kind}"
    return uniform(identifier, config.seed, purpose) < getattr(config, RATES[field])


def scalar_faults(config, source, day, fields):
    """Return lexical recorded fields and audit evidence for one Customer or Trade."""
    recorded, entries = dict(fields), []
    names = ("C_ID", "C_ST_ID") if source == "Customer" else ("T_ID", "T_DTS", "T_ST_ID")
    identity = {name: str(fields[name]) for name in names}
    if source == "Customer":
        identity["CDC_FLAG"] = str(fields["_flag"])
        candidates = ("C_TIER", "C_DOB")
    else:
        candidates = ("T_CHRG", "T_COMM") if fields["T_ST_ID"] == "CMPT" else ()
    for field in candidates:
        if field not in fields or fields[field] in (None, "") or not scalar_selected(config, source, day, identity, field):
            continue
        if field == "C_TIER":
            incorrect = "9"
        elif field == "C_DOB":
            incorrect = (day + timedelta(days=1)).isoformat()
        else:
            incorrect = Decimal(str(fields["T_TRADE_PRICE"])) * int(fields["T_QTY"]) + Decimal("0.01")
            if incorrect > Decimal("99999999.99"):
                raise ValueError("A scalar fee fault cannot fit VALUE_T.")
            incorrect = f"{incorrect:.2f}"
        recorded[field] = incorrect
        entries.append({"source": source, "day": str(day), "identity": identity, "field": field,
                        "correct": str(fields[field]), "recorded": incorrect})
    return recorded, entries
