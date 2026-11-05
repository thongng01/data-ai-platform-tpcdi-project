from xml.etree import ElementTree as ET

import polars as pl

from .generation import Generation, account_group, order_key, padded


def customer_values(identifier: int, tier: int | None = None):
    return {
        "C_ID": identifier, "C_TAX_ID": f"TAX{identifier}", "C_ST_ID": "ACTV",
        "C_L_NAME": f"Family{identifier}", "C_F_NAME": "Alex", "C_M_NAME": "",
        "C_GNDR": "U", "C_TIER": tier or identifier % 3 + 1, "C_DOB": "1960-01-01",
        "C_ADLINE1": f"{identifier} Main Street", "C_ADLINE2": "", "C_ZIPCODE": "10001",
        "C_CITY": "New York", "C_STATE_PROV": "NY", "C_CTRY": "USA",
        **{f"C_{part}_{number}": "" for number in range(1, 4) for part in ("CTRY", "AREA", "LOCAL", "EXT")},
        "C_EMAIL_1": f"customer{identifier}@example.test", "C_EMAIL_2": "",
        "C_LCL_TX_ID": "US1", "C_NAT_TX_ID": "US2",
    }


def account_values(generation: Generation, customer: int, slot: int):
    return {"CA_ID": (customer - 1) * 3 + slot, "CA_B_ID": (customer - 1) % generation.config.brokers + 1,
            "CA_C_ID": customer, "CA_NAME": f"Account {customer}-{slot}", "CA_TAX_ST": customer % 3, "CA_ST_ID": "ACTV"}


def _account_element(parent, values, complete=True):
    attributes = {"CA_ID": str(values["CA_ID"])}
    if complete:
        attributes["CA_TAX_ST"] = str(values["CA_TAX_ST"])
    account = ET.SubElement(parent, "Account", attributes)
    if complete:
        ET.SubElement(account, "CA_B_ID").text = str(values["CA_B_ID"])
        ET.SubElement(account, "CA_NAME").text = values["CA_NAME"]
    return account


def _action(generation, identifier, kind, day):
    timestamp = f"{generation.config.day(day)}T00:00:00"
    action = ET.Element("TPCDI:Action", {"ActionType": kind, "ActionTS": timestamp})
    customer = ET.SubElement(action, "Customer", {"C_ID": str(identifier)})
    values = customer_values(identifier)
    if kind == "NEW":
        customer.attrib.update({name: str(values[name]) for name in ("C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB")})
        for tag, names in [
            ("Name", ["C_L_NAME", "C_F_NAME", "C_M_NAME"]),
            ("Address", ["C_ADLINE1", "C_ADLINE2", "C_ZIPCODE", "C_CITY", "C_STATE_PROV", "C_CTRY"]),
            ("TaxInfo", ["C_LCL_TX_ID", "C_NAT_TX_ID"]),
        ]:
            element = ET.SubElement(customer, tag)
            for name in names:
                ET.SubElement(element, name).text = str(values[name])
        for slot in (1, 2):
            _account_element(customer, account_values(generation, identifier, slot))
    elif kind == "ADDACCT":
        _account_element(customer, account_values(generation, identifier, 3))
    elif kind == "UPDCUST":
        customer.set("C_TIER", str((identifier + 1) % 3 + 1))
    elif kind == "UPDACCT":
        account = _account_element(customer, account_values(generation, identifier, 1), False)
        ET.SubElement(account, "CA_NAME").text = f"Updated account {identifier}"
    elif kind == "CLOSEACCT":
        _account_element(customer, account_values(generation, identifier, 2), False)
    return f"{timestamp}/{identifier:015d}/{kind}", ET.tostring(action, encoding="unicode")


def generate_customers(generation: Generation):
    config = generation.config
    for first in range(1, config.customers + 1, config.customer_batch_size):
        rows = []
        for identifier in range(first, min(first + config.customer_batch_size, config.customers + 1)):
            rows.append(_action(generation, identifier, "NEW", 0))
            if identifier % 3 == 0:
                rows.append(_action(generation, identifier, "UPDCUST", 1))
            if identifier % 5 == 0:
                rows.append(_action(generation, identifier, "ADDACCT", 2))
            if identifier % 4 == 0:
                rows.append(_action(generation, identifier, "UPDACCT", 3))
            if identifier % 7 == 0:
                rows.append(_action(generation, identifier, "CLOSEACCT", config.historical_days - 2))
            if identifier % 11 == 0:
                rows.append(_action(generation, identifier, "INACT", config.historical_days - 1))
        generation.text("Batch1/CustomerMgmt.xml", rows)
    for batch in range(2, config.incremental_days + 2):
        day = config.historical_days + batch - 2
        new_first = config.customers + (batch - 2) * config.new_customers_per_day + 1
        new_last = new_first + config.new_customers_per_day
        for first in range(1, new_last, config.customer_batch_size):
            last = min(first + config.customer_batch_size, new_last)
            customer_rows = []
            account_rows = []
            for identifier in range(first, last):
                active = not (identifier <= config.customers and identifier % 11 == 0)
                new = new_first <= identifier < new_last
                if new or (active and identifier % 13 == 0):
                    values = customer_values(identifier, (identifier + batch - 1) % 3 + 1 if not new else None)
                    customer_rows.append({**values, "_flag": "I" if new else "U", "_key": f"{identifier:015d}"})
            accounts = account_group(config, first, last, day)
            for account in accounts.iter_rows(named=True):
                identifier = account["customer"]
                new = new_first <= identifier < new_last
                if new or identifier % 17 == 0:
                    values = account_values(generation, identifier, account["slot"])
                    if not new:
                        values["CA_NAME"] = f"Updated account {identifier} batch {batch}"
                    account_rows.append({**values, "_flag": "I" if new else "U", "_key": f"{account['account']:015d}"})
            if customer_rows:
                generation.emit("Customer", batch, pl.DataFrame(customer_rows))
            if account_rows:
                generation.emit("Account", batch, pl.DataFrame(account_rows))
    for batch in range(1, config.incremental_days + 2):
        for first in range(1, config.total_customers + 1, config.customer_batch_size):
            rows = []
            for identifier in range(first, min(first + config.customer_batch_size, config.total_customers + 1)):
                rows.append({
                    "AgencyID": f"AG{identifier}", "LastName": f"Family{identifier}", "FirstName": "Alex", "MiddleInitial": "",
                    "Gender": "U", "AddressLine1": f"{identifier} Main Street", "AddressLine2": "", "PostalCode": "10001",
                    "City": "New York", "State": "NY", "Country": "USA", "Phone": "", "Income": 50000 + batch * 100,
                    "NumberCars": 1, "NumberChildren": 0, "MaritalStatus": "U", "Age": config.day(0).year - 1960,
                    "CreditRating": 700, "OwnOrRentFlag": "R", "Employer": "POC Employer", "NumberCreditCards": 1,
                    "NetWorth": 100000, "_key": f"{identifier:015d}",
                })
            generation.emit("Prospect", batch, pl.DataFrame(rows))
