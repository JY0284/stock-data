"""JSON-friendly wrappers around StockStore for agent tool integration.

Each function is designed to be:
- Small and composable (one purpose per function)
- Return JSON-serializable dict/list outputs
- Support `limit` and `columns` for efficient responses
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Literal

import pandas as pd

from stock_data.store import How, ResolvedSymbol, StockStore, open_store

# Module-level store cache for repeated calls (singleton pattern for agents).
_store_cache: dict[str, StockStore] = {}


def _get_store(store_dir: str = "store") -> StockStore:
    """Get or create a cached StockStore instance."""
    if store_dir not in _store_cache:
        _store_cache[store_dir] = open_store(store_dir)
    return _store_cache[store_dir]


def _df_to_payload(
    df: pd.DataFrame,
    *,
    limit: int | None = None,
    include_meta: bool = True,
) -> dict[str, Any]:
    """Convert DataFrame to JSON-serializable payload."""
    if limit is not None:
        df = df.head(int(limit))
    rows = df.to_dict(orient="records")
    payload: dict[str, Any] = {"rows": rows}
    if include_meta:
        payload["row_count"] = len(rows)
    return payload


# -----------------------------------------------------------------------------
# Identity / Universe
# -----------------------------------------------------------------------------

def resolve_symbol(
    symbol_or_ts_code: str,
    *,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Resolve a symbol (e.g. '300888') or ts_code (e.g. '300888.SZ') to full info."""
    store = _get_store(store_dir)
    r = store.resolve(symbol_or_ts_code)
    return asdict(r)


def get_stock_basic(
    *,
    ts_code: str | None = None,
    symbol: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get stock basic info. Optionally filter by ts_code or symbol."""
    store = _get_store(store_dir)
    df = store.stock_basic(ts_code=ts_code, symbol=symbol, columns=columns, limit=limit)
    return _df_to_payload(df, limit=limit)


def get_stock_company(
    ts_code: str,
    *,
    columns: list[str] | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get company profile for a ts_code."""
    store = _get_store(store_dir)
    df = store.stock_company(ts_code=ts_code, columns=columns)
    return _df_to_payload(df)


def get_universe(
    *,
    list_status: str | None = "L",
    exchange: str | None = None,
    market: str | None = None,
    industry: str | None = None,
    area: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get filtered universe of stocks."""
    store = _get_store(store_dir)
    df = store.universe(
        list_status=list_status,
        exchange=exchange,
        market=market,
        industry=industry,
        area=area,
        columns=columns,
    )
    return _df_to_payload(df, limit=limit)


# -----------------------------------------------------------------------------
# Calendar
# -----------------------------------------------------------------------------

def get_trade_cal(
    *,
    exchange: str | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get trade calendar for an exchange."""
    store = _get_store(store_dir)
    df = store.trade_cal(exchange=exchange)
    return _df_to_payload(df, limit=limit)


def get_trading_days(
    start_date: str,
    end_date: str,
    *,
    exchange: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get list of trading days in a date range."""
    store = _get_store(store_dir)
    days = store.trading_days(start_date, end_date, exchange=exchange)
    return {"trading_days": days, "count": len(days)}


def is_trading_day(
    date: str,
    *,
    exchange: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Check if a date is a trading day."""
    store = _get_store(store_dir)
    result = store.is_trading_day(date, exchange=exchange)
    return {"date": date, "is_trading_day": result}


def get_prev_trade_date(
    date: str,
    *,
    exchange: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get the previous trading day before a date."""
    store = _get_store(store_dir)
    result = store.prev_trade_date(date, exchange=exchange)
    return {"date": date, "prev_trade_date": result}


def get_next_trade_date(
    date: str,
    *,
    exchange: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get the next trading day after a date."""
    store = _get_store(store_dir)
    result = store.next_trade_date(date, exchange=exchange)
    return {"date": date, "next_trade_date": result}


# -----------------------------------------------------------------------------
# IPO / Events
# -----------------------------------------------------------------------------

def get_new_share(
    *,
    year: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    ts_code: str | None = None,
    symbol_or_sub_code: str | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get IPO / new share info."""
    store = _get_store(store_dir)
    df = store.new_share(
        year=year,
        start_date=start_date,
        end_date=end_date,
        ts_code=ts_code,
        symbol_or_sub_code=symbol_or_sub_code,
        limit=limit,
    )
    return _df_to_payload(df)


def get_namechange(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get name change history for a stock."""
    store = _get_store(store_dir)
    df = store.namechange(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return _df_to_payload(df)


# -----------------------------------------------------------------------------
# Market Data
# -----------------------------------------------------------------------------

def get_daily_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get daily OHLCV prices for a stock."""
    store = _get_store(store_dir)
    df = store.daily(ts_code, start_date=start_date, end_date=end_date, columns=columns)
    return _df_to_payload(df, limit=limit)


def get_adj_factor(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get adjustment factors for a stock."""
    store = _get_store(store_dir)
    df = store.adj_factor(ts_code, start_date=start_date, end_date=end_date)
    return _df_to_payload(df, limit=limit)


def get_daily_basic(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get daily basic indicators (turnover, valuation, market cap, etc.)."""
    store = _get_store(store_dir)
    df = store.daily_basic(ts_code, start_date=start_date, end_date=end_date, columns=columns)
    return _df_to_payload(df, limit=limit)


def get_daily_adj_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    how: Literal["qfq", "hfq", "both"] = "both",
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get adjusted daily prices (qfq=forward, hfq=backward, both=all)."""
    store = _get_store(store_dir)
    df = store.daily_adj(ts_code, start_date=start_date, end_date=end_date, how=how)
    return _df_to_payload(df, limit=limit)


def get_weekly_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get weekly OHLCV prices for a stock."""
    store = _get_store(store_dir)
    df = store.weekly(ts_code, start_date=start_date, end_date=end_date, columns=columns)
    return _df_to_payload(df, limit=limit)


def get_monthly_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get monthly OHLCV prices for a stock."""
    store = _get_store(store_dir)
    df = store.monthly(ts_code, start_date=start_date, end_date=end_date, columns=columns)
    return _df_to_payload(df, limit=limit)


def get_stk_limit(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get limit-up/limit-down prices for a stock."""
    store = _get_store(store_dir)
    df = store.stk_limit(ts_code, start_date=start_date, end_date=end_date)
    return _df_to_payload(df, limit=limit)


def get_suspend_d(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get suspension/resumption events for a stock."""
    store = _get_store(store_dir)
    df = store.suspend_d(ts_code, start_date=start_date, end_date=end_date)
    return _df_to_payload(df, limit=limit)


# -----------------------------------------------------------------------------
# Generic
# -----------------------------------------------------------------------------

def query_dataset(
    dataset: str,
    *,
    where: dict[str, Any] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    order_by: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Generic dataset query (escape hatch)."""
    store = _get_store(store_dir)
    df = store.read(
        dataset,
        where=where,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        limit=limit,
        order_by=order_by,
    )
    return _df_to_payload(df)
