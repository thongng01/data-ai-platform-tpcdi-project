"""Measure a chronological attrition model using only saved source observations."""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path
from xml.etree import ElementTree as ET

import numpy as np
import polars as pl

from .random_values import scalar
from .business import ATTRITION_HORIZON_DAYS as HORIZON_DAYS

FEATURES = ("deposit_count_7d", "deposit_count_30d", "withdrawal_count_30d",
            "buy_count_7d", "buy_count_30d", "sell_count_7d", "sell_count_30d",
            "net_external_30d", "external_outflow_30d", "tenure_days")


@dataclass(frozen=True)
class CashObservation:
    """Hold fields visible in a cash transaction, before any feature calculation."""

    account_id: int
    day: date
    amount_cents: int
    name: str


@dataclass(frozen=True)
class AccountLifetime:
    """Hold an opening and an optional inactive date observed in account sources."""

    account_id: int
    opened: date
    closed: date | None = None


def observations(lifetimes: list[AccountLifetime], events: list[CashObservation], end: date) -> pl.DataFrame:
    """Build weekly features through t and closure labels in (t, t+30 days]."""
    rows = []
    by_account = {}
    for event in events:
        by_account.setdefault(event.account_id, []).append(event)
    for lifetime in lifetimes:
        account_events = sorted(by_account.get(lifetime.account_id, []), key=lambda event: event.day)
        latest = min(end - timedelta(days=HORIZON_DAYS), (lifetime.closed - timedelta(days=1)) if lifetime.closed else end)
        snapshot = lifetime.opened + timedelta(days=30)
        left = right = 0
        while snapshot <= latest:
            while right < len(account_events) and account_events[right].day <= snapshot:
                right += 1
            while left < right and account_events[left].day <= snapshot - timedelta(days=30):
                left += 1
            window = account_events[left:right]
            deposits = [event for event in window if event.name == "Deposit"]
            withdrawals = [event for event in window if event.name == "Withdrawal"]
            buys = [event for event in window if event.name.startswith("Trade settlement ") and event.amount_cents < 0]
            sells = [event for event in window if event.name.startswith("Trade settlement ") and event.amount_cents > 0]
            last_week = snapshot - timedelta(days=7)
            net = sum(event.amount_cents for event in deposits + withdrawals)
            row = {"account_id": lifetime.account_id, "snapshot": snapshot,
                   "deposit_count_7d": sum(event.day > last_week for event in deposits),
                   "deposit_count_30d": len(deposits), "withdrawal_count_30d": len(withdrawals),
                   "buy_count_7d": sum(event.day > last_week for event in buys), "buy_count_30d": len(buys),
                   "sell_count_7d": sum(event.day > last_week for event in sells), "sell_count_30d": len(sells),
                   "net_external_30d": net, "external_outflow_30d": -sum(event.amount_cents for event in withdrawals),
                   "tenure_days": (snapshot - lifetime.opened).days,
                   "rule": len(deposits) == 0 and net < 0,
                   "label": bool(lifetime.closed and snapshot < lifetime.closed <= snapshot + timedelta(days=30))}
            rows.append(row)
            snapshot += timedelta(days=7)
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def average_precision(labels: np.ndarray, scores: np.ndarray) -> float | None:
    """Integrate precision over distinct score thresholds, treating ties together."""
    labels = np.asarray(labels, dtype=np.int64)
    if not np.any(labels):
        return None
    order = np.argsort(-np.asarray(scores), kind="stable")
    y, ordered = labels[order], np.asarray(scores)[order]
    boundaries = np.r_[np.flatnonzero(np.diff(ordered)) + 1, len(y)]
    true = np.cumsum(y)[boundaries - 1]
    recall = true / np.sum(y)
    precision = true / boundaries
    return float(np.sum(np.diff(np.r_[0., recall]) * precision))


def _precision(labels: np.ndarray, selected: np.ndarray) -> float | None:
    """Return measured precision, or None when the method makes no predictions."""
    return float(np.mean(labels[selected])) if np.any(selected) else None


def _matrix(frame: pl.DataFrame) -> np.ndarray:
    """Use a fixed observable feature list; IDs and labels cannot enter the model."""
    matrix = frame.select(FEATURES).to_numpy().astype(np.float64)
    # Signed logarithms keep cents and counts usable without hidden wealth.
    return np.sign(matrix) * np.log1p(np.abs(matrix))


def _fit_logistic(x: np.ndarray, y: np.ndarray, penalty: float) -> np.ndarray:
    """Fit a deterministic small linear model without another runtime dependency."""
    weights = np.zeros(x.shape[1])
    for _ in range(400):
        logits = np.clip(x @ weights, -30, 30)
        probability = 1 / (1 + np.exp(-logits))
        gradient = x.T @ (probability - y) / len(y)
        gradient[1:] += penalty * weights[1:]
        weights -= .15 * gradient
    return weights


def evaluate(frame: pl.DataFrame) -> dict:
    """Use chronological splits with 30-day label gaps and validation-only tuning."""
    if frame.is_empty():
        return {"status": "insufficient_data", "reason": "No fully observed account snapshots."}
    dates = sorted(frame["snapshot"].unique().to_list())
    if len(dates) < 10:
        return {"status": "insufficient_data", "reason": "Too few chronological snapshot dates."}
    train_end = dates[int((len(dates) - 1) * .5)]
    validation_end = dates[int((len(dates) - 1) * .75)]
    gap = timedelta(days=HORIZON_DAYS)
    train = frame.filter(pl.col("snapshot") <= train_end)
    validation = frame.filter((pl.col("snapshot") > train_end + gap) & (pl.col("snapshot") <= validation_end))
    test = frame.filter(pl.col("snapshot") > validation_end + gap)
    splits = {"train": train, "validation": validation, "test": test}
    sizes = {name: {"rows": part.height, "positive_labels": int(part["label"].sum()) if part.height else 0}
             for name, part in splits.items()}
    if any(part.is_empty() or part["label"].n_unique() < 2 for part in splits.values()):
        return {"status": "insufficient_data", "splits": sizes,
                "reason": "Every chronological split needs closures and nonclosures."}
    matrices = {name: _matrix(part) for name, part in splits.items()}
    mean = matrices["train"].mean(axis=0)
    scale = np.maximum(matrices["train"].std(axis=0), .01)
    matrices = {name: np.column_stack([np.ones(len(matrix)), (matrix - mean) / scale]) for name, matrix in matrices.items()}
    labels = {name: part["label"].to_numpy().astype(float) for name, part in splits.items()}
    candidates = []
    for penalty in (.0001, .001, .01, .1):
        weights = _fit_logistic(matrices["train"], labels["train"], penalty)
        score = average_precision(labels["validation"], matrices["validation"] @ weights)
        candidates.append((score, penalty, weights))
    _, penalty, weights = max(candidates, key=lambda item: item[0])
    rule = test["rule"].to_numpy()
    model_score = matrices["test"] @ weights
    rule_ap = average_precision(labels["test"], rule.astype(float))
    model_ap = average_precision(labels["test"], model_score)
    precision = _precision(labels["test"], rule)
    # Match the rule's prediction count, rather than pick a threshold on test labels.
    predictions = np.zeros(test.height, dtype=bool)
    count = int(np.count_nonzero(rule))
    if count:
        predictions[np.argsort(-model_score, kind="stable")[:count]] = True
    return {"status": "measured", "metric": "average_precision", "features": list(FEATURES),
            "splits": sizes, "train_end": str(train_end), "validation_end": str(validation_end),
            "label_gap_days": HORIZON_DAYS, "regularization": penalty,
            "model_average_precision": model_ap, "rule_average_precision": rule_ap,
            "model_precision_at_rule_count": _precision(labels["test"], predictions),
            "rule_precision": precision, "rule_predictions": count,
            "model_beats_rule": model_ap > rule_ap,
            "rule_precision_below_30_percent": precision < .3 if precision is not None else None}


def _source_lifetimes(root: Path, config) -> list[AccountLifetime]:
    """Derive account lifetimes from CMS actions and dated daily account extracts."""
    opened, closed = {}, {}
    xml = root / "offline/initial/CustomerMgmt.xml"
    context = ET.iterparse(xml, events=("start", "end"))
    _, root_element = next(context)
    for event, element in context:
        if event != "end" or element.tag.rsplit("}", 1)[-1] != "Action":
            continue
        day = date.fromisoformat(element.attrib["ActionTS"][:10])
        kind = element.attrib["ActionType"]
        for account in element.findall("Customer/Account"):
            identifier = int(account.attrib["CA_ID"])
            if kind in ("NEW", "ADDACCT"):
                opened[identifier] = day
            elif kind in ("INACT", "CLOSEACCT"):
                closed[identifier] = day
        element.clear()
        root_element.remove(element)
    for path in sorted((root / "offline/daily").glob("*/Account_*.txt")):
        day = date.fromisoformat(f"{path.parent.name[:4]}-{path.parent.name[4:6]}-{path.parent.name[6:]}")
        with path.open(encoding="ascii") as stream:
            for line in stream:
                fields = line.rstrip("\n").split("|")
                identifier = int(fields[2])
                if fields[0] == "I":
                    opened[identifier] = day
                if fields[-1] == "INAC":
                    closed[identifier] = day
    return [AccountLifetime(identifier, day, closed.get(identifier)) for identifier, day in sorted(opened.items())]


def check_signal(root: Path, config, sample_accounts: int = 512) -> dict:
    """Sample IDs deterministically; write evidence outside the ingestion paths."""
    root = Path(root)
    lifetimes = _source_lifetimes(root, config)
    sampled = sorted(lifetimes, key=lambda row: scalar(row.account_id, config.seed, "signal-check-sample"))[:sample_accounts]
    identifiers = {row.account_id for row in sampled}
    events = []
    with (root / "offline/initial/CashTransaction.txt").open(encoding="ascii") as stream:
        for line in stream:
            _, account, timestamp, amount, name = line.rstrip("\n").split("|")
            if int(account) in identifiers:
                events.append(CashObservation(int(account), date.fromisoformat(timestamp[:10]), int(Decimal(amount) * 100), name))
    for path in sorted((root / "stream/cash_transactions").glob("*.jsonl")):
        with path.open(encoding="ascii") as stream:
            for line in stream:
                event = json.loads(line)
                account = int(event["ct_ca_id"])
                if account in identifiers:
                    events.append(CashObservation(account, date.fromisoformat(event["ct_dts"][:10]),
                                                  int(Decimal(event["ct_amt"]) * 100), event["ct_name"]))
    frame = observations(sampled, events, config.end)
    result = evaluate(frame)
    result["sample_accounts"] = len(sampled)
    result["sample_method"] = "Smallest stable named account hashes, without label filtering."
    destination = root / "summary"
    destination.mkdir(exist_ok=True)
    if not frame.is_empty():
        frame.write_parquet(destination / "signal_observations.parquet")
    (destination / "signal_check.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result
