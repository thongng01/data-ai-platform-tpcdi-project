import numpy as np
import polars as pl

from .generation import Generation, account_group, order_key, padded
from .random_values import money, values


def _plans(generation: Generation, day: int, first: int, last: int):
    config = generation.config
    accounts = account_group(config, first, last, day).filter(pl.col("closed") > day + 2)
    plans = accounts.join(pl.DataFrame({"cycle": range(config.cycles_per_account_day)}), how="cross")
    if plans.is_empty():
        return plans
    plans = plans.with_columns(
        (((day * config.total_customers * 3 + pl.col("account") - 1) * config.cycles_per_account_day + pl.col("cycle")) * 2 + 1).alias("buy_id")
    )
    identifiers = plans["buy_id"].to_numpy()
    quantities = (values(identifiers, config.seed, "quantity") % 90 + 10).astype(np.int64)
    securities = (values(identifiers, config.seed, "security") % config.securities + 1).astype(np.int64)
    prices = 1000 + securities % 90 * 100 + (values(identifiers, config.seed, "price") % 51).astype(np.int64)
    kind = values(identifiers, config.seed, "type") % 100
    canceled = (kind < 60) & (values(identifiers, config.seed, "cancel") % 10000 < config.cancel_basis_points)
    return plans.with_columns(
        pl.Series("quantity", quantities), pl.Series("buy_price", prices),
        pl.Series("sell_price", prices + (values(identifiers, config.seed, "price_change") % 201).astype(np.int64) - 100),
        pl.Series("security", securities),
        pl.Series("cash", (values(identifiers, config.seed, "cash") % 10000 < config.cash_basis_points).astype(np.int64)),
        pl.Series("buy_limit", kind < 60), pl.Series("canceled", canceled),
        pl.Series("sell_kind", (values(identifiers, config.seed, "sell_type") % 100).astype(np.int64)),
    ).with_columns(pl.concat_str(pl.lit("S"), pl.col("security").cast(pl.String).str.pad_start(10, "0")).alias("symbol"))


def _trade_events(generation: Generation, plans: pl.DataFrame, day: int, selling: bool):
    config = generation.config
    if selling:
        plans = plans.filter(~pl.col("canceled"))
    if plans.is_empty():
        return
    trade_day = day + int(selling)
    plans = plans.with_columns(
        (pl.col("buy_id") + int(selling)).alias("T_ID"),
        (pl.col("sell_price") if selling else pl.col("buy_price")).alias("price"),
        ((pl.col("sell_kind") >= 10) & (pl.col("sell_kind") < 60) if selling else pl.col("buy_limit")).alias("is_limit"),
    ).with_columns(
        (pl.when(pl.col("sell_kind") < 10).then(pl.lit("TSL"))
         .when(pl.col("sell_kind") < 60).then(pl.lit("TLS")).otherwise(pl.lit("TMS")) if selling else
         pl.when(pl.col("buy_limit")).then(pl.lit("TLB")).otherwise(pl.lit("TMB"))).alias("T_TT_ID"),
        pl.lit(25).alias("charge"), pl.lit(50).alias("commission"), pl.lit(0).alias("tax"),
    )
    events = []
    for stage in range(3):
        event_day = trade_day + (stage == 2)
        if event_day >= config.total_days:
            continue
        selected = plans.filter(pl.col("is_limit")) if stage == 1 else plans
        if selected.is_empty():
            continue
        timestamp = f"{config.day(event_day)} {'09:00:00' if stage == 2 else ('10:00:00' if stage == 0 else '12:00:00')}"
        terminal = pl.lit("CMPT") if selling else pl.when(pl.col("canceled")).then(pl.lit("CNCL")).otherwise(pl.lit("CMPT"))
        status = terminal if stage == 2 else (pl.lit("SBMT") if stage == 1 else
                 pl.when(pl.col("is_limit")).then(pl.lit("PNDG")).otherwise(pl.lit("SBMT")))
        selected = selected.with_columns(
            pl.lit(timestamp).alias("T_DTS"), status.alias("T_ST_ID"),
            pl.lit(event_day).alias("event_day"), pl.lit(stage).alias("stage"),
            pl.lit("I" if stage == 0 else "U").alias("_flag"),
        ).with_columns(
            pl.col("cash").alias("T_IS_CASH"), pl.col("symbol").alias("T_S_SYMB"),
            pl.col("quantity").alias("T_QTY"), money("price").alias("T_BID_PRICE"),
            pl.col("account").alias("T_CA_ID"), pl.lit("POC Broker").alias("T_EXEC_NAME"),
            *[pl.when(pl.col("T_ST_ID") == "CMPT").then(money(column)).otherwise(None).alias(target)
              for column, target in [("price", "T_TRADE_PRICE"), ("charge", "T_CHRG"), ("commission", "T_COMM"), ("tax", "T_TAX")]],
        )
        events.append(selected)
        if stage == 2:
            completed = selected.filter(pl.col("T_ST_ID") == "CMPT")
            batch = config.batch(event_day)
            holding = completed.with_columns(
                pl.col("buy_id").alias("HH_H_T_ID"), pl.col("T_ID").alias("HH_T_ID"),
                (pl.col("quantity") if selling else pl.lit(0)).alias("HH_BEFORE_QTY"),
                (pl.lit(0) if selling else pl.col("quantity")).alias("HH_AFTER_QTY"),
                order_key(pl.col("T_DTS"), padded(pl.col("T_ID"))), pl.lit("I").alias("_flag"),
            )
            generation.emit("HoldingHistory", batch, holding)
            cash = completed.filter(pl.col("cash") == 1).with_columns(
                ((pl.col("price") * pl.col("quantity")) * (1 if selling else -1) - pl.col("charge") - pl.col("commission") - pl.col("tax")).alias("amount"),
            ).with_columns(
                pl.col("account").alias("CT_CA_ID"), pl.col("T_DTS").alias("CT_DTS"), money("amount").alias("CT_AMT"),
                pl.concat_str(pl.lit("Trade "), pl.col("T_ID").cast(pl.String)).alias("CT_NAME"),
                order_key(pl.col("T_DTS"), padded(pl.col("T_ID"))), pl.lit("I").alias("_flag"),
            )
            generation.emit("CashTransaction", batch, cash)
    if not events:
        return
    history = pl.concat(events)
    historical = history.filter(pl.col("event_day") < config.historical_days)
    snapshot = historical.sort(["T_ID", "T_DTS"]).unique("T_ID", keep="last").with_columns(order_key(padded(pl.col("T_ID"))))
    generation.emit("Trade", 1, snapshot)
    generation.emit("TradeHistory", 1, historical.with_columns(
        pl.col("T_ID").alias("TH_T_ID"), pl.col("T_DTS").alias("TH_DTS"), pl.col("T_ST_ID").alias("TH_ST_ID"),
        order_key(padded(pl.col("T_ID")), pl.col("T_DTS")),
    ))
    for batch in range(2, config.incremental_days + 2):
        incremental = history.filter(pl.col("event_day") == config.historical_days + batch - 2)
        generation.emit("Trade", batch, incremental.with_columns(order_key(pl.col("T_DTS"), padded(pl.col("T_ID")), padded(pl.col("stage")))))


def generate_activity(generation: Generation):
    config = generation.config
    for day in range(config.total_days):
        for first in range(1, config.total_customers + 1, config.customer_batch_size):
            last = min(first + config.customer_batch_size, config.total_customers + 1)
            plans = _plans(generation, day, first, last)
            if not plans.is_empty():
                _trade_events(generation, plans, day, False)
                _trade_events(generation, plans, day, True)
            accounts = account_group(config, first, last, day)
            customers = accounts.select("customer").unique().with_columns(
                pl.concat_str(pl.lit("S"), ((pl.col("customer") - 1) % config.securities + 1).cast(pl.String).str.pad_start(10, "0")).alias("symbol")
            )
            for timestamp, action in [("13:00:00", "ACTV"), ("15:00:00", "CNCL")]:
                watch = customers.with_columns(
                    pl.col("customer").alias("W_C_ID"), pl.col("symbol").alias("W_S_SYMB"),
                    pl.lit(f"{config.day(day)} {timestamp}").alias("W_DTS"), pl.lit(action).alias("W_ACTION"),
                ).with_columns(order_key(pl.col("W_DTS"), padded(pl.col("W_C_ID"))))
                generation.emit("WatchHistory", config.batch(day), watch)
