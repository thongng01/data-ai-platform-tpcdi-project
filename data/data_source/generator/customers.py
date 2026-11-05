"""Source values, partial CMS actions, and persistent prospect snapshots."""

from datetime import date, timedelta
from xml.etree import ElementTree as ET

import numpy as np

from .population import rounded, uniform
from .random_values import uniform as draw


def customer_values(identifier: int, tier: int = 3) -> dict:
    identifier = int(identifier)
    return {
        "C_ID": identifier, "C_TAX_ID": f"TAX{identifier}", "C_ST_ID": "ACTV",
        "C_L_NAME": f"Family{identifier}", "C_F_NAME": "Alex", "C_M_NAME": "",
        "C_GNDR": "U", "C_TIER": int(tier),
        "C_DOB": f"{1940 + identifier % 45}-01-01",
        "C_ADLINE1": f"{identifier} Main Street", "C_ADLINE2": "", "C_ZIPCODE": "10001",
        "C_CITY": "New York", "C_STATE_PROV": "NY", "C_CTRY": "USA",
        **{f"C_{part}_{number}": "" for number in range(1, 4) for part in ("CTRY", "AREA", "LOCAL", "EXT")},
        "C_EMAIL_1": f"customer{identifier}@example.test", "C_EMAIL_2": "",
        "C_LCL_TX_ID": "US1", "C_NAT_TX_ID": "US2",
    }


def account_values(config, account) -> dict:
    identifier = int(account["account_id"])
    return {
        "CA_ID": identifier, "CA_B_ID": int(account["broker_id"]),
        "CA_C_ID": int(account["customer_id"]), "CA_NAME": f"Account {identifier}",
        "CA_TAX_ST": int(account["tax_status"]), "CA_ST_ID": "ACTV",
    }


def _text(parent, tag, value):
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
    yield config.cutover
    current = config.daily_start_date
    while current <= config.end:
        if current.day == 1:
            yield current
        current += timedelta(days=1)


def _prospect_values(identifier, identity, snapshot, seed):
    fields = customer_values(identity)
    # Prospect values use only a persistent identity and the snapshot date, never a future tier.
    income = 25000 + int(draw(identifier, seed, "prospect-income") * 175000)
    return {
        "AgencyID": f"AG{identifier}", "LastName": fields["C_L_NAME"],
        "FirstName": fields["C_F_NAME"], "MiddleInitial": fields["C_M_NAME"],
        "Gender": fields["C_GNDR"], "AddressLine1": fields["C_ADLINE1"],
        "AddressLine2": fields["C_ADLINE2"], "PostalCode": fields["C_ZIPCODE"],
        "City": fields["C_CITY"], "State": fields["C_STATE_PROV"], "Country": fields["C_CTRY"],
        "Phone": "", "Income": income, "NumberCars": identifier % 3,
        "NumberChildren": identifier % 4, "MaritalStatus": "U",
        "Age": snapshot.year - date.fromisoformat(fields["C_DOB"]).year,
        "CreditRating": 550 + identifier % 251, "OwnOrRentFlag": "R" if identifier % 3 else "O",
        "Employer": f"Employer{identifier % 1000}", "NumberCreditCards": 1 + identifier % 4,
        "NetWorth": income * (2 + identifier % 10), "_key": f"{identifier:015d}",
    }


def generate_prospects(generation, population):
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
            rows = []
            for row in identities[first:min(first + block_size, count)]:
                prospect = int(row["prospect_id"])
                identity = int(row["customer_id"]) or len(customers) + prospect
                rows.append(_prospect_values(prospect, identity, snapshot, config.seed))
            generation.emit("Prospect", snapshot, rows)
