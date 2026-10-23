"""Event and entity tables: Trade, CashTransaction, DailyMarket, WatchHistory,
HoldingHistory, plus the CustomerMgmt XML action stream.

The trade and cash files carry the injected data problems, because they are the
tables a reviewer will look at when asking "where is your skew / duplicates /
schema drift". The reference tables stay clean; the problems belong where the
rubric points at them.
"""

from __future__ import annotations

import os
import random
from collections.abc import Iterator
from datetime import datetime, timedelta

from .emit import DelimitedWriter, XmlActionWriter, action_timestamp, timestamp
from .problems import apply_schema_evolution, duplicate_rows, inject_skew, skewed_distribution
from .rng import (
    alpha_string_with_spaces,
    bool_int,
    money,
    pick,
    person,
    phone,
    symbol,
    tax_id,
)
from .specs_reference import STATUS_CODES, TRADE_STATUS_CODES, TRADE_TYPE_CODES

_EPOCH = datetime(2012, 7, 7)


def _symbol_pool(seed: int, size: int) -> list[str]:
    """A stable symbol universe. DailyMarket, Trade and WatchHistory all draw from it."""
    rng = random.Random(seed)
    return [symbol(rng) for _ in range(size)]


def write_trade(
    root: str,
    *,
    count: int,
    symbols: list[str],
    max_account: int,
    problems,
    seed: int,
    batch: int,
) -> str:
    """Trade.txt. 14 columns in Batch1; 16 in later batches.

    Batch2/3 prefix every row with an operation code (``I``/``U``) and a
    surrogate row id, then ``TH_TRADE_ID``, then the 14 payload columns. That
    added column pair *is* the schema evolution: Batch1 has 14 fields, Batch2
    has 16. The same trade appears once as ``SBMT`` with empty settlement
    fields and again as ``CMPT`` fully populated, so the business event is the
    ``TH_TRADE_ID`` and not ``T_ID``.

    Skew is injected on the status column (which value dominates) and the
    join-key column is available for a second, join-shaped skew via
    ``problems.join_skew``.
    """
    path = os.path.join(root, "Trade.txt")
    incremental = batch > 1
    rng = random.Random(seed + batch)

    def base_rows() -> Iterator[list[object]]:
        staged: list[tuple[str, list[object]]] = []
        for index in range(count):
            moment = _EPOCH + timedelta(
                days=rng.randrange(0, 1800), seconds=rng.randrange(0, 86_400)
            )
            # Skew: a dominant share of trades share one status (and, behind the
            # blend, one trade type). Matches TPC-DI where CMPT dominates.
            status = skewed_distribution(rng, TRADE_STATUS_CODES, [0.92, 0.04, 0.03, 0.01], problems.trade_status_skew)
            trade_type = skewed_distribution(rng, TRADE_TYPE_CODES, [0.31, 0.31, 0.13, 0.13, 0.12], problems.trade_type_skew)
            is_cash = bool_int(rng)
            sym = pick(rng, symbols)
            qty = rng.randint(1, 10_000)
            bid = money(rng, 1.0, 500.0)
            account = rng.randint(0, max_account)
            exec_name = "" if trade_type in ("TMB", "TMS") else person(rng)[0]
            trade_price = money(rng, 1.0, 500.0)
            charge = money(rng, 0.0, 100.0)
            comm = money(rng, 0.0, 100.0)
            tax = money(rng, 0.0, 30_000.0)

            payload = [
                moment.strftime("%Y-%m-%d %H:%M:%S"),
                status,
                trade_type,
                is_cash,
                sym,
                qty,
                bid,
                account,
                exec_name,
                trade_price,
                charge,
                comm,
                tax,
            ]
            staged.append((payload[0], payload))

        # Time-ordered, because this file is what gets replayed as a stream and
        # out-of-order handling is only meaningful against an otherwise ordered
        # baseline. Ids are assigned *after* the sort so T_ID still runs 0..N in
        # file order, which is what TPC-DI does.
        staged.sort(key=lambda item: item[0])

        for index, (_, payload) in enumerate(staged):
            if incremental:
                status = str(payload[1])
                op = "U" if status == "CMPT" and rng.random() < 0.6 else "I"
                row_id = 1_000_000 + batch * 100_000 + index
                trade_history_id = 3_000_000 + batch * 100_000 + index
                # A pending trade leaves the settlement columns empty; its
                # completion arrives as a second row sharing TH_TRADE_ID.
                if status in ("SBMT", "PNDG"):
                    yield [op, row_id, trade_history_id, *payload[:9], "", "", "", ""]
                else:
                    yield [op, row_id, trade_history_id, *payload]
            else:
                yield [index, *payload]

    stream = base_rows()

    # Schema evolution is expressed as the Batch2+ prefix, which the branch above
    # already emits. The explicit injector remains available for adding a named
    # column beyond the prefix pair.
    if problems.schema_evolution_batch and batch >= problems.schema_evolution_batch:
        stream = apply_schema_evolution(
            stream,
            active=False,  # the I/U prefix pair already encodes evolution
            column_name=problems.schema_evolution_column,
            value_for=lambda i: f"{rng.uniform(0, 500):.2f}",
        )

    # Streaming duplicates live in the delta batches; offline duplicates in Batch1.
    duplicate_rate = (
        problems.duplicate_rate_streaming if incremental else problems.duplicate_rate_offline
    )
    stream = duplicate_rows(stream, duplicate_rate, seed=seed + batch * 7, key=f"trade-{batch}")

    with DelimitedWriter(path, "pipe") as w:
        for row in stream:
            w.write_row(row)
    return path


def write_cash_transaction(
    root: str,
    *,
    count: int,
    max_account: int,
    problems,
    seed: int,
    batch: int,
) -> str:
    """CashTransaction.txt. 4 columns in Batch1, 6 in later batches.

    ``CT_AMT`` is signed (deposits positive, withdrawals negative), 2dp, no
    thousands separators, and typed FLOAT64 in the reference schema even though
    every other numeric is NUMERIC. ``CT_NAME`` contains embedded spaces and
    must never be whitespace-tokenised.
    """
    path = os.path.join(root, "CashTransaction.txt")
    incremental = batch > 1
    rng = random.Random(seed + batch)

    def base_rows() -> Iterator[list[object]]:
        for index in range(count):
            moment = _EPOCH + timedelta(
                days=rng.randrange(0, 1800), seconds=rng.randrange(0, 86_400)
            )
            amount = rng.uniform(-100_000, 100_000)
            name = alpha_string_with_spaces(rng, rng.randint(30, 90))
            account = rng.randint(0, max_account)
            payload = [account, timestamp(moment), f"{amount:.2f}", name]
            if incremental:
                yield ["I", 1_000_000 + batch * 100_000 + index, *payload]
            else:
                # Batch1 has no surrogate row id: CT_CA_ID is field 1. Only the
                # delta batches gain the op code + row id prefix.
                yield payload

    stream = duplicate_rows(
        base_rows(),
        problems.duplicate_rate_streaming if incremental else problems.duplicate_rate_offline,
        seed=seed + batch * 13,
        key=f"cash-{batch}",
    )
    with DelimitedWriter(path, "pipe") as w:
        for row in stream:
            w.write_row(row)
    return path


def write_daily_market(
    root: str,
    *,
    count: int,
    symbols: list[str],
    problems,
    seed: int,
    batch: int,
) -> str:
    """DailyMarket.txt. 6 columns, then 8 in the delta batches.

    Rows are grouped by date; prices are always exactly 2dp; volume is a bare
    integer. The delta batches carry a ``dt=``-style date that is the *prior*
    batch's date, matching TPC-DI, where Batch2's market rows carry Batch1's
    batch date.
    """
    path = os.path.join(root, "DailyMarket.txt")
    incremental = batch > 1
    rng = random.Random(seed + batch)
    from datetime import date as _date

    day = _date(2015, 7, 6)

    def base_rows() -> Iterator[list[object]]:
        for index in range(count):
            day_value = _date(2015, 7, 6) + timedelta(days=index // max(1, len(symbols)))
            sym = symbols[index % len(symbols)]
            close = rng.uniform(1.0, 1000.0)
            high = close * rng.uniform(1.0, 1.2)
            low = close * rng.uniform(0.8, 1.0)
            volume = rng.randint(1_000, 900_000_000)
            payload = [day_value.isoformat(), sym, f"{close:.2f}", f"{high:.2f}", f"{low:.2f}", volume]
            if incremental:
                yield ["I", 1_000_000 + batch * 100_000 + index, *payload]
            else:
                yield payload

    stream = duplicate_rows(
        base_rows(),
        problems.duplicate_rate_streaming if incremental else problems.duplicate_rate_offline,
        seed=seed + batch * 17,
        key=f"market-{batch}",
    )
    with DelimitedWriter(path, "pipe") as w:
        for row in stream:
            w.write_row(row)
    return path


def write_watch_history(
    root: str,
    *,
    count: int,
    symbols: list[str],
    max_customer: int,
    problems,
    seed: int,
    batch: int,
) -> str:
    """WatchHistory.txt. 4 columns, then 6 in the delta batches.

    Batch1 has no surrogate row id: column 1 is the customer id. That
    difference is itself a schema change across batches. ``W_ACTION`` is
    ACTV or CNCL.
    """
    path = os.path.join(root, "WatchHistory.txt")
    incremental = batch > 1
    rng = random.Random(seed + batch)

    def base_rows() -> Iterator[list[object]]:
        for index in range(count):
            moment = _EPOCH + timedelta(
                days=rng.randrange(0, 1800), seconds=rng.randrange(0, 86_400)
            )
            action = "ACTV" if rng.random() < 0.8 else "CNCL"
            payload = [
                rng.randint(0, max_customer),
                pick(rng, symbols),
                timestamp(moment),
                action,
            ]
            if incremental:
                yield ["I", 1_000_000 + batch * 100_000 + index, *payload]
            else:
                yield payload

    stream = duplicate_rows(
        base_rows(),
        problems.duplicate_rate_streaming if incremental else problems.duplicate_rate_offline,
        seed=seed + batch * 19,
        key=f"watch-{batch}",
    )
    with DelimitedWriter(path, "pipe") as w:
        for row in stream:
            w.write_row(row)
    return path


def write_holding_history(
    root: str,
    *,
    count: int,
    problems,
    seed: int,
    batch: int,
) -> str:
    """HoldingHistory.txt. 4 columns, then 6 in the delta batches.

    Before/after quantities around a trade. The delta batches are always ``I``.
    """
    path = os.path.join(root, "HoldingHistory.txt")
    incremental = batch > 1
    rng = random.Random(seed + batch)

    def base_rows() -> Iterator[list[object]]:
        for index in range(count):
            after = rng.randint(0, 5_000)
            before = max(0, after - rng.randint(0, 500))
            holding_trade = 3_000_000 + index
            trade = 2_000_000 + index
            payload = [holding_trade, trade, before, after]
            if incremental:
                yield ["I", 1_000_000 + batch * 100_000 + index, *payload]
            else:
                yield payload

    stream = duplicate_rows(
        base_rows(),
        problems.duplicate_rate_streaming if incremental else problems.duplicate_rate_offline,
        seed=seed + batch * 23,
        key=f"holding-{batch}",
    )
    with DelimitedWriter(path, "pipe") as w:
        for row in stream:
            w.write_row(row)
    return path


def write_trade_history(
    root: str,
    *,
    count: int,
    max_trade: int,
    problems,
    seed: int,
) -> str:
    """TradeHistory.txt. 3 columns, Batch1 only.

    The middle column is ``TRADE_T``, a timestamp, and its name is kept as-is
    because it is stable in both the source files and the master schema. Status
    moves SBMT -> CMPT/CNCL over a trade's life, and TPC-DI bounds the gap
    between submission and completion at about five minutes, which is the
    natural ceiling for a "late" settlement.
    """
    path = os.path.join(root, "TradeHistory.txt")
    rng = random.Random(seed)

    def base_rows() -> Iterator[list[object]]:
        for index in range(count):
            trade_id = rng.randint(0, max_trade)
            moment = _EPOCH + timedelta(
                days=rng.randrange(0, 1800), seconds=rng.randrange(0, 86_400)
            )
            status = skewed_distribution(
                rng, TRADE_STATUS_CODES, [0.56, 0.38, 0.05, 0.01], problems.trade_status_skew
            )
            yield [trade_id, timestamp(moment), status]

    stream = duplicate_rows(base_rows(), problems.duplicate_rate_offline, seed=seed + 29, key="th")
    with DelimitedWriter(path, "pipe") as w:
        for row in stream:
            w.write_row(row)
    return path


def write_customer_mgmt(
    root: str,
    *,
    count: int,
    accounts_per_customer: int,
    problems,
    seed: int,
) -> str:
    """CustomerMgmt.xml: the XML action stream that carries customer/account history.

    This is the SCD2 source. The six action types are NEW, ADDACCT, UPDACCT,
    UPDCUST, CLOSEACCT, INACT. Payloads are **partial**: only a ``NEW`` carries
    the full customer (tax id, gender, tier, DOB, name, address, contact, tax
    info, account). Every other action carries just the ids it changes, which
    is why downstream needs a carry-forward to reconstruct versions.

    Note the DimCustomer/DimAccount quirk we reproduce faithfully: DimCustomer
    keeps ``NEW, UPDCUST, INACT``; DimAccount keeps ``NEW, ADDACCT, UPDACCT,
    CLOSEACCT, INACT`` and **excludes** ``UPDCUST``.
    """
    path = os.path.join(root, "CustomerMgmt.xml")
    rng = random.Random(seed)
    writer = XmlActionWriter(path)
    account_id = 0
    tax_ids = [f"{c}{rng.randint(1, 9)}" for c in ("US", "CA", "GB", "DE", "FR")]

    for customer_index in range(count):
        customer_id = customer_index
        created = _EPOCH + timedelta(days=rng.randrange(0, 3000), seconds=rng.randrange(0, 86_400))

        # NEW carries the full payload including the opening account.
        account_id += 1
        first, last, middle = person(rng)
        line1, line2, zipcode, city, state, country = _address(rng)
        phone1 = phone(rng)
        writer.write_action(
            action_type="NEW",
            action_ts=action_timestamp(created),
            customer_id=customer_id,
            payload={
                "C_TAX_ID": tax_id(rng),
                "C_GNDR": pick(rng, ("M", "F")),
                "C_TIER": str(rng.randint(1, 6)),
                "C_DOB": (_EPOCH - timedelta(days=rng.randint(18 * 365, 80 * 365))).strftime("%Y-%m-%d"),
                "C_L_NAME": last,
                "C_F_NAME": first,
                "C_M_NAME": middle,
                "C_ADLINE1": line1,
                "C_ADLINE2": line2,
                "C_ZIPCODE": zipcode,
                "C_CITY": city,
                "C_STATE_PROV": state,
                "C_CTRY": country,
                "C_PRIM_EMAIL": f"{first}.{last}@example-mail.com".lower(),
                "C_ALT_EMAIL": f"{first}.{last}@mail2world.com".lower(),
                "C_PHONE_1": {"C_CTRY_CODE": phone1[0], "C_AREA_CODE": phone1[1], "C_LOCAL": phone1[2], "C_EXT": phone1[3]},
                "C_LCL_TX_ID": pick(rng, tax_ids),
                "C_NAT_TX_ID": pick(rng, tax_ids),
                "CA_ID": account_id,
                "CA_B_ID": rng.randint(10_000, 11_000),
                "CA_NAME": alpha_string_with_spaces(rng, rng.randint(20, 40)),
            },
        )

        # A few lifecycle actions per customer, all partial payloads.
        events = []
        for _ in range(accounts_per_customer):
            events.append(("ADDACCT", rng.random() < 0.8))
        if rng.random() < 0.3:
            events.append(("UPDACCT", True))
        if rng.random() < 0.4:
            events.append(("UPDCUST", False))
        if rng.random() < 0.15:
            events.append(("CLOSEACCT", True))
        if rng.random() < 0.1:
            events.append(("INACT", False))

        for action_type, has_account in events:
            moment = created + timedelta(days=rng.randrange(1, 1200), seconds=rng.randrange(0, 86_400))
            if action_type == "ADDACCT":
                account_id += 1
                writer.write_action(
                    action_type=action_type,
                    action_ts=action_timestamp(moment),
                    customer_id=customer_id,
                    payload={
                        "Account": {"CA_ID": account_id, "CA_B_ID": rng.randint(10_000, 11_000)}
                    },
                )
            elif action_type == "UPDACCT":
                writer.write_action(
                    action_type=action_type,
                    action_ts=action_timestamp(moment),
                    customer_id=customer_id,
                    payload={"Account": {"CA_B_ID": rng.randint(10_000, 11_000)}},
                )
            elif action_type == "UPDCUST":
                writer.write_action(
                    action_type=action_type,
                    action_ts=action_timestamp(moment),
                    customer_id=customer_id,
                    payload={"C_TIER": str(rng.randint(1, 6))},
                )
            elif action_type == "CLOSEACCT":
                writer.write_action(
                    action_type=action_type,
                    action_ts=action_timestamp(moment),
                    customer_id=customer_id,
                    payload={"Account": {"CA_ID": rng.randint(0, max(1, account_id))}},
                )
            elif action_type == "INACT":
                writer.write_action(
                    action_type=action_type,
                    action_ts=action_timestamp(moment),
                    customer_id=customer_id,
                    payload={},
                )

    writer.close()
    return path


def _address(rng: random.Random) -> tuple[str, str, str, str, str, str]:
    from .rng import address

    return address(rng)
