"""JSON-friendly wrappers around StockStore for agent tool integration.

Each function is designed to be:
- Small and composable (one purpose per function)
- Return JSON-serializable dict/list outputs
- Support pagination via `offset` and `limit` for progressive navigation
- Return clean, compact responses (nulls removed, metadata included)
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


def _clean_row(row: dict) -> dict:
    """Remove None/NaN values from a row dict to save tokens."""
    return {k: v for k, v in row.items() if v is not None and not (isinstance(v, float) and pd.isna(v))}


def _df_to_payload(
    df: pd.DataFrame,
    *,
    offset: int = 0,
    limit: int | None = None,
    compact: bool = True,
) -> dict[str, Any]:
    """Convert DataFrame to JSON-serializable payload with pagination metadata.
    
    Args:
        df: Source DataFrame
        offset: Number of rows to skip (for pagination)
        limit: Max rows to return after offset
        compact: If True, remove null/NaN values from each row
    
    Returns:
        {
            "rows": [...],
            "total_count": N,     # total rows before pagination
            "showing": "M-N",     # range being shown (1-indexed for humans)
            "has_more": bool,     # whether more rows exist after this page
        }
    """
    total = len(df)
    
    # Apply pagination
    if offset > 0:
        df = df.iloc[offset:]
    if limit is not None and limit > 0:
        df = df.head(int(limit))
    
    rows = df.to_dict(orient="records")
    if compact:
        rows = [_clean_row(r) for r in rows]
    
    # Calculate showing range (1-indexed for human readability)
    start_idx = offset + 1
    end_idx = offset + len(rows)
    
    return {
        "rows": rows,
        "total_count": total,
        "showing": f"{start_idx}-{end_idx}" if rows else "0-0",
        "has_more": (offset + len(rows)) < total,
    }


def _single_row_payload(df: pd.DataFrame, compact: bool = True) -> dict[str, Any]:
    """Convert a single-row DataFrame to a flat dict (or null if empty)."""
    if df.empty:
        return {"found": False, "data": None}
    row = df.iloc[0].to_dict()
    if compact:
        row = _clean_row(row)
    return {"found": True, "data": row}


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
    name_contains: str | None = None,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 20,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get stock basic info with pagination support.
    
    Args:
        ts_code: Filter by exact ts_code (e.g., '000001.SZ')
        symbol: Filter by exact symbol (e.g., '000001')
        name_contains: Filter by name containing substring (e.g., '卫星' or '银行')
        columns: Specific columns to return (default: ts_code, name, industry, market, list_date)
        offset: Skip first N rows (for pagination, 0-indexed)
        limit: Max rows to return (default 20, max 100)
    """
    store = _get_store(store_dir)
    
    # Default to essential columns for list view
    if columns is None:
        columns = ["ts_code", "symbol", "name", "industry", "market", "list_date", "list_status"]
    
    df = store.stock_basic(ts_code=ts_code, symbol=symbol, columns=None)  # get all first for filtering
    
    # Apply name filter if provided
    if name_contains and "name" in df.columns:
        df = df[df["name"].str.contains(name_contains, case=False, na=False)]
    
    # Select columns
    cols = [c for c in columns if c in df.columns]
    if cols:
        df = df[cols]
    
    # Clamp limit
    limit = min(limit or 20, 100)
    
    return _df_to_payload(df, offset=offset, limit=limit)


def get_stock_basic_detail(
    ts_code: str,
    *,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get detailed stock basic info for a single stock (all columns)."""
    store = _get_store(store_dir)
    df = store.stock_basic(ts_code=ts_code)
    return _single_row_payload(df)


def get_stock_company(
    ts_code: str,
    *,
    columns: list[str] | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get company profile for a ts_code."""
    store = _get_store(store_dir)
    df = store.stock_company(ts_code=ts_code, columns=columns)
    return _single_row_payload(df)


def get_universe(
    *,
    list_status: str | None = "L",
    exchange: str | None = None,
    market: str | None = None,
    industry: str | None = None,
    area: str | None = None,
    offset: int = 0,
    limit: int = 20,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get filtered universe of stocks with pagination.
    
    Args:
        list_status: 'L' for listed, 'D' for delisted, 'P' for paused. Default 'L'.
        exchange: 'SSE' (Shanghai) or 'SZSE' (Shenzhen)
        market: '主板', '创业板', '科创板', 'CDR', etc.
        industry: Industry name (e.g., '银行', '白酒')
        area: Province/region (e.g., '北京', '广东')
        offset: Skip first N rows (0-indexed)
        limit: Max rows (default 20, max 100)
    
    Returns compact list with ts_code, name, industry, market only.
    """
    store = _get_store(store_dir)
    df = store.universe(
        list_status=list_status,
        exchange=exchange,
        market=market,
        industry=industry,
        area=area,
        columns=["ts_code", "name", "industry", "market"],
    )
    limit = min(limit or 20, 100)
    return _df_to_payload(df, offset=offset, limit=limit)


def list_industries(
    *,
    store_dir: str = "store",
) -> dict[str, Any]:
    """List all unique industries and their stock counts."""
    store = _get_store(store_dir)
    df = store.stock_basic(columns=["industry"])
    if df.empty:
        return {"industries": [], "count": 0}
    counts = df["industry"].value_counts().to_dict()
    return {"industries": list(counts.keys()), "count": len(counts), "stock_counts": counts}


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
    offset: int = 0,
    limit: int = 20,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get IPO / new share info with pagination."""
    store = _get_store(store_dir)
    df = store.new_share(
        year=year,
        start_date=start_date,
        end_date=end_date,
        ts_code=ts_code,
        symbol_or_sub_code=symbol_or_sub_code,
    )
    limit = min(limit or 20, 100)
    return _df_to_payload(df, offset=offset, limit=limit)


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
    offset: int = 0,
    limit: int = 30,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get daily OHLCV prices for a stock with pagination.
    
    Default columns: trade_date, open, high, low, close, vol, pct_chg
    Data is sorted by trade_date descending (most recent first).
    """
    store = _get_store(store_dir)
    if columns is None:
        columns = ["trade_date", "open", "high", "low", "close", "vol", "pct_chg"]
    df = store.daily(ts_code, start_date=start_date, end_date=end_date, columns=None)
    
    # Sort by date descending
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    
    # Select columns
    cols = [c for c in columns if c in df.columns]
    if cols:
        df = df[cols]
    
    limit = min(limit or 30, 200)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_adj_factor(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    offset: int = 0,
    limit: int = 30,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get adjustment factors for a stock."""
    store = _get_store(store_dir)
    df = store.adj_factor(ts_code, start_date=start_date, end_date=end_date)
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    limit = min(limit or 30, 200)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_daily_basic(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 30,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get daily valuation metrics (PE, PB, market cap, etc.) with pagination.
    
    Default columns: trade_date, pe_ttm, pb, total_mv, circ_mv, turnover_rate
    """
    store = _get_store(store_dir)
    if columns is None:
        columns = ["trade_date", "pe_ttm", "pb", "total_mv", "circ_mv", "turnover_rate"]
    df = store.daily_basic(ts_code, start_date=start_date, end_date=end_date, columns=None)
    
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    
    cols = [c for c in columns if c in df.columns]
    if cols:
        df = df[cols]
    
    limit = min(limit or 30, 200)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_daily_adj_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    how: Literal["qfq", "hfq", "both"] = "qfq",
    offset: int = 0,
    limit: int = 30,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get adjusted daily prices (qfq=forward, hfq=backward, both=all)."""
    store = _get_store(store_dir)
    df = store.daily_adj(ts_code, start_date=start_date, end_date=end_date, how=how)
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    limit = min(limit or 30, 200)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_weekly_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 20,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get weekly OHLCV prices for a stock."""
    store = _get_store(store_dir)
    if columns is None:
        columns = ["trade_date", "open", "high", "low", "close", "vol", "pct_chg"]
    df = store.weekly(ts_code, start_date=start_date, end_date=end_date, columns=None)
    
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    
    cols = [c for c in columns if c in df.columns]
    if cols:
        df = df[cols]
    
    limit = min(limit or 20, 100)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_monthly_prices(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 12,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get monthly OHLCV prices for a stock."""
    store = _get_store(store_dir)
    if columns is None:
        columns = ["trade_date", "open", "high", "low", "close", "vol", "pct_chg"]
    df = store.monthly(ts_code, start_date=start_date, end_date=end_date, columns=None)
    
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    
    cols = [c for c in columns if c in df.columns]
    if cols:
        df = df[cols]
    
    limit = min(limit or 12, 60)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_stk_limit(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    offset: int = 0,
    limit: int = 30,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get limit-up/limit-down prices for a stock."""
    store = _get_store(store_dir)
    df = store.stk_limit(ts_code, start_date=start_date, end_date=end_date)
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date", ascending=False)
    limit = min(limit or 30, 200)
    return _df_to_payload(df, offset=offset, limit=limit)


def get_suspend_d(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    offset: int = 0,
    limit: int = 30,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Get suspension/resumption events for a stock."""
    store = _get_store(store_dir)
    df = store.suspend_d(ts_code, start_date=start_date, end_date=end_date)
    limit = min(limit or 30, 100)
    return _df_to_payload(df, offset=offset, limit=limit)


# -----------------------------------------------------------------------------
# Search - fuzzy matching for user queries
# -----------------------------------------------------------------------------

def search_stocks(
    query: str,
    *,
    offset: int = 0,
    limit: int = 20,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Search stocks by name, symbol, or ts_code (fuzzy matching).
    
    This is the primary tool for finding stocks when user provides partial info.
    Searches in: ts_code, symbol, name, industry.
    
    Args:
        query: Search term (e.g., '卫星', '银行', '300888', '贵州茅台')
        offset: Skip first N results
        limit: Max results (default 20)
    
    Returns matching stocks with: ts_code, name, industry, market
    """
    store = _get_store(store_dir)
    df = store.stock_basic(columns=["ts_code", "symbol", "name", "industry", "market", "list_status"])
    
    if df.empty:
        return {"rows": [], "total_count": 0, "showing": "0-0", "has_more": False, "query": query}
    
    # Only listed stocks
    df = df[df["list_status"] == "L"]
    
    query_lower = query.lower()
    
    # Match in any of these columns
    mask = (
        df["ts_code"].str.lower().str.contains(query_lower, na=False) |
        df["symbol"].str.contains(query_lower, na=False) |
        df["name"].str.contains(query, na=False) |  # Chinese name: case-sensitive
        df["industry"].str.contains(query, na=False)
    )
    df = df[mask]
    
    # Select display columns
    df = df[["ts_code", "name", "industry", "market"]]
    
    limit = min(limit or 20, 100)
    result = _df_to_payload(df, offset=offset, limit=limit)
    result["query"] = query
    return result


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
    offset: int = 0,
    limit: int = 50,
    order_by: str | None = None,
    store_dir: str = "store",
) -> dict[str, Any]:
    """Generic dataset query with pagination (escape hatch for advanced queries).
    
    Available datasets: daily, weekly, monthly, daily_basic, adj_factor, 
    stk_limit, suspend_d, stock_basic, stock_company, trade_cal, new_share, namechange
    """
    store = _get_store(store_dir)
    df = store.read(
        dataset,
        where=where,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        order_by=order_by,
    )
    limit = min(limit or 50, 200)
    return _df_to_payload(df, offset=offset, limit=limit)
