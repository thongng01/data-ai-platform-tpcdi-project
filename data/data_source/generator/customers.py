"""Source values, partial CMS actions, and persistent prospect snapshots."""

from datetime import date, timedelta
from xml.etree import ElementTree as ET

import numpy as np

from .personal_data import personal_columns, personal_values
from .population import rounded, uniform


EMPLOYERS = np.asarray([
    "Riverbend Health", "Summit Manufacturing", "Cedar Research",
    "Community Services", "Pioneer Logistics", "Metro Education",
])


def customer_birth_year(config, identifier=20) -> int:
    """Keep each customer's birth year within the configured adult age limits."""
    return max(config.end.year - 120, min(1940 + int(identifier) % 45, config.start.year - 18))


def customer_values(identifier: int, tier: int = 3, seed: int = 0) -> dict:
    """Build source customer fields from stable personal identity and current tier."""
    identifier = int(identifier)
    person = personal_values(identifier, seed)
    return {
        "C_ID": identifier, "C_TAX_ID": f"TAX{identifier}", "C_ST_ID": "ACTV",
        "C_L_NAME": person["last_name"], "C_F_NAME": person["first_name"], "C_M_NAME": person["middle_initial"],
        "C_GNDR": person["gender"], "C_TIER": int(tier),
        "C_DOB": f"{1940 + identifier % 45}-01-01",
        "C_ADLINE1": person["address_line1"], "C_ADLINE2": person["address_line2"], "C_ZIPCODE": person["postal_code"],
        "C_CITY": person["city"], "C_STATE_PROV": person["state"], "C_CTRY": person["country"],
        **{f"C_{part}_{number}": "" for number in range(1, 4) for part in ("CTRY", "AREA", "LOCAL", "EXT")},
        "C_EMAIL_1": person["email"], "C_EMAIL_2": person["alternate_email"],
        "C_LCL_TX_ID": "US1", "C_NAT_TX_ID": "US2",
        "C_CTRY_1": person["phone_country"], "C_AREA_1": person["phone_area"], "C_LOCAL_1": person["phone_local"],
    }


def account_values(config, account) -> dict:
    """Name an account after its owner and its brokerage or retirement purpose."""
    identifier = int(account["account_id"])
    person = personal_values(int(account["customer_id"]), config.seed)
    purpose = "Retirement" if int(account["tax_status"]) != 1 else "Brokerage"
    name = f"{person['first_name'][0]}. {person['last_name']} {purpose} {identifier}"
    return {
        "CA_ID": identifier, "CA_B_ID": int(account["broker_id"]),
        "CA_C_ID": int(account["customer_id"]), "CA_NAME": name,
        "CA_TAX_ST": int(account["tax_status"]), "CA_ST_ID": "ACTV",
    }


def _text(parent, tag, value):
    """Add one XML field, preserving explicit empty text as an empty element."""
    child = ET.SubElement(parent, tag)
    child.text = None if value is None else str(value)
    return child


def xml_action(kind, timestamp, customer_id, customer_values=None, account_values=None) -> str:
    """Omission retains a value. None and empty text emit explicit empty elements."""
    if kind not in {"NEW", "ADDACCT", "UPDCUST", "UPDACCT", "CLOSEACCT", "INACT"}:
        raise ValueError(f"Unknown CMS action: {kind}")
    action = ET.Element("TPCDI:Action", {"ActionType": kind, "ActionTS": str(timestamp)})
    customer = ET.SubElement(action, "Customer", {"C_ID": str(customer_id)})
    fields = customer_values or {}
    for name in ("C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB"):
        if name in fields:
            customer.set(name, "" if fields[name] is None else str(fields[name]))
    for tag, names in (
        ("Name", ("C_L_NAME", "C_F_NAME", "C_M_NAME")),
        ("Address", ("C_ADLINE1", "C_ADLINE2", "C_ZIPCODE", "C_CITY", "C_STATE_PROV", "C_CTRY")),
    ):
        present = [name for name in names if name in fields]
        if present:
            group = ET.SubElement(customer, tag)
            for name in present:
                _text(group, name, fields[name])
    contact = None
    for source, target in (("C_EMAIL_1", "C_PRIM_EMAIL"), ("C_EMAIL_2", "C_ALT_EMAIL")):
        if source in fields:
            if contact is None:
                contact = ET.SubElement(customer, "ContactInfo")
            _text(contact, target, fields[source])
    for number in range(1, 4):
        parts = [(f"C_{part}_{number}", target) for part, target in (
            ("CTRY", "C_CTRY_CODE"), ("AREA", "C_AREA_CODE"), ("LOCAL", "C_LOCAL"), ("EXT", "C_EXT"),
        )]
        present = [(source, target) for source, target in parts if source in fields]
        if present:
            if contact is None:
                contact = ET.SubElement(customer, "ContactInfo")
            phone = ET.SubElement(contact, f"C_PHONE_{number}")
            for source, target in present:
                _text(phone, target, fields[source])
    tax = [name for name in ("C_LCL_TX_ID", "C_NAT_TX_ID") if name in fields]
    if tax:
        group = ET.SubElement(customer, "TaxInfo")
        for name in tax:
            _text(group, name, fields[name])
    for fields in account_values or ():
        account = ET.SubElement(customer, "Account", {"CA_ID": str(fields["CA_ID"])})
        if "CA_TAX_ST" in fields:
            account.set("CA_TAX_ST", "" if fields["CA_TAX_ST"] is None else str(fields["CA_TAX_ST"]))
        for name in ("CA_B_ID", "CA_NAME"):
            if name in fields:
                _text(account, name, fields[name])
    return ET.tostring(action, encoding="unicode", short_empty_elements=True)


def _snapshot_dates(config):
    """Yield the cutover snapshot and every monthly daily-delivery snapshot."""
    yield config.cutover
    current = config.daily_start_date
    while current <= config.end:
        if current.day == 1:
            yield current
        current += timedelta(days=1)


def _prospect_columns(identifiers, identities, snapshot: date, seed: int, birth_years=None) -> dict[str, np.ndarray]:
    """Build a bounded prospect block without Python selection for each person."""
    identifiers = np.asarray(identifiers, dtype=np.int64)
    identities = np.asarray(identities, dtype=np.int64)
    if identifiers.shape != identities.shape:
        raise ValueError("Prospect and personal identity arrays must have the same shape.")
    person = personal_columns(identities, seed)
    if birth_years is None:
        birth_years = 1940 + identities % 45
    # Prospect income stays tied to the agency identity. Matching names and
    # addresses use the customer identity and never depend on a future tier.
    income = 25000 + (uniform(identifiers, seed, "prospect-income") * 175000).astype(np.int64)
    id_text = identifiers.astype(str)
    return {
        "AgencyID": np.strings.add("AG", id_text), "LastName": person["last_name"],
        "FirstName": person["first_name"], "MiddleInitial": person["middle_initial"],
        "Gender": person["gender"], "AddressLine1": person["address_line1"],
        "AddressLine2": person["address_line2"], "PostalCode": person["postal_code"],
        "City": person["city"], "State": person["state"], "Country": person["country"],
        "Phone": person["phone"], "Income": income, "NumberCars": identifiers % 3,
        "NumberChildren": identifiers % 4, "MaritalStatus": np.full(len(identifiers), "U"),
        "Age": snapshot.year - np.asarray(birth_years),
        "CreditRating": 550 + identifiers % 251, "OwnOrRentFlag": np.where(identifiers % 3, "R", "O"),
        "Employer": EMPLOYERS[identifiers % len(EMPLOYERS)], "NumberCreditCards": 1 + identifiers % 4,
        "NetWorth": income * (2 + identifiers % 10),
    }


def _prospect_values(identifier, identity, snapshot, seed, birth_year=None) -> dict:
    """Keep the scalar prospect interface equal to a one-row array selection."""
    years = None if birth_year is None else np.asarray([birth_year])
    columns = _prospect_columns([identifier], [identity], snapshot, seed, years)
    return {name: column[0].item() for name, column in columns.items()}


def generate_prospects(generation, population) -> None:
    """Save persistent prospect identities and emit bounded monthly array blocks."""
    config = generation.config
    customers = population.customers
    accounts = population.accounts
    dates = list(_snapshot_dates(config))
    counts = []
    for snapshot in dates:
        day = (snapshot - config.start).days
        active = np.count_nonzero((accounts["open_day"] <= day) & (accounts["close_day"] > day))
        counts.append(max(1, rounded(active * config.prospects_per_account)))
    # Both existing and future customers can appear, but a snapshot never carries future status.
    overlap = uniform(customers["customer_id"], config.seed, "prospect-overlap") < config.prospect_overlap
    matches = customers["customer_id"][overlap]
    dtype = np.dtype([("prospect_id", "<i8"), ("customer_id", "<i8"), ("visible_day", "<i4")])
    identities = np.zeros(max(counts), dtype=dtype)
    identities["prospect_id"] = np.arange(1, len(identities) + 1)
    # Only fill available initial slots. Extreme configured horizons can have more future joiners.
    match_count = min(len(matches), counts[0])
    identities["customer_id"][:match_count] = matches[:match_count]
    visible = 0
    for snapshot, count in zip(dates, counts, strict=True):
        if count > visible:
            identities["visible_day"][visible:count] = (snapshot - config.start).days
            visible = count
    np.save(population.path / "prospects.npy", identities, allow_pickle=False)
    block_size = min(2048, config.export_rows)
    for snapshot, count in zip(dates, counts, strict=True):
        for first in range(0, count, block_size):
            block = identities[first:min(first + block_size, count)]
            personal_ids = np.where(block["customer_id"], block["customer_id"], len(customers) + block["prospect_id"])
            years = np.maximum(config.end.year - 120, np.minimum(1940 + personal_ids % 45, config.start.year - 18))
            columns = _prospect_columns(block["prospect_id"], personal_ids, snapshot, config.seed, years)
            # Agency identity, not the lexical AG-prefixed field, sets row
            # order. The export module consumes the arrays before returning.
            generation.record_batch("Prospect", (snapshot - config.start).days, columns,
                                    order={"_identity": block["prospect_id"]})
