#!/usr/bin/env python3
"""Demo: Using the StockStore API to query local data.

This script demonstrates the same workflow as print_stock_300888.py,
but using the cleaner StockStore API instead of raw DuckDB queries.

Usage:
    python demos/use_store_api.py [symbol] [store_dir]

Examples:
    python demos/use_store_api.py 300888 store
    python demos/use_store_api.py 000001.SZ store
"""
from __future__ import annotations

import sys

from stock_data.store import open_store


def main() -> int:
    # Parse arguments
    symbol_or_ts_code = sys.argv[1].strip() if len(sys.argv) >= 2 else "300888"
    store_dir = sys.argv[2].strip() if len(sys.argv) >= 3 else "store"

    # Open the store (with caching enabled by default)
    store = open_store(store_dir)

    # 1. Resolve symbol to ts_code
    print("=" * 60)
    print("Resolve symbol -> ts_code")
    print("=" * 60)
    resolved = store.resolve(symbol_or_ts_code)
    print(f"  symbol:    {resolved.symbol}")
    print(f"  ts_code:   {resolved.ts_code}")
    print(f"  list_date: {resolved.list_date}")

    ts_code = resolved.ts_code
    start_date = resolved.list_date or "19900101"

    # 2. Stock basic info
    print("\n" + "=" * 60)
    print("Stock Basic")
    print("=" * 60)
    df = store.stock_basic(ts_code=ts_code)
    print(df.to_string(index=False))

    # 3. Company profile
    print("\n" + "=" * 60)
    print("Stock Company")
    print("=" * 60)
    df = store.stock_company(ts_code=ts_code)
    if df.empty:
        print("(no data)")
    else:
        # Print selected columns for readability
        cols = ["ts_code", "com_name", "chairman", "province", "city", "employees"]
        cols = [c for c in cols if c in df.columns]
        print(df[cols].to_string(index=False))

    # 4. Name changes
    print("\n" + "=" * 60)
    print("Name Changes")
    print("=" * 60)
    df = store.namechange(ts_code=ts_code)
    if df.empty:
        print("(no name changes)")
    else:
        print(df.to_string(index=False))

    # 5. IPO info
    print("\n" + "=" * 60)
    print("IPO / New Share")
    print("=" * 60)
    df = store.new_share(ts_code=ts_code)
    if df.empty:
        # Try by symbol
        df = store.new_share(symbol_or_sub_code=resolved.symbol)
    if df.empty:
        print("(no IPO data)")
    else:
        print(df.to_string(index=False))

    # 6. Daily prices (last 10 rows)
    print("\n" + "=" * 60)
    print(f"Daily Prices (last 10, since {start_date})")
    print("=" * 60)
    df = store.daily(ts_code, start_date=start_date)
    print(df.tail(10).to_string(index=False))
    print(f"... total rows: {len(df)}")

    # 7. Adjusted prices (last 5 rows, qfq only)
    print("\n" + "=" * 60)
    print("Daily Adjusted Prices (last 5, QFQ)")
    print("=" * 60)
    df = store.daily_adj(ts_code, start_date=start_date, how="qfq")
    # Show key columns
    cols = ["ts_code", "trade_date", "close", "adj_factor", "qfq_close"]
    cols = [c for c in cols if c in df.columns]
    print(df[cols].tail(5).to_string(index=False))

    # 8. Calendar helpers demo
    print("\n" + "=" * 60)
    print("Calendar Helpers")
    print("=" * 60)
    trading_days = store.trading_days("20260101", "20260131")
    print(f"Trading days in Jan 2026: {len(trading_days)}")
    print(f"  First: {trading_days[0] if trading_days else 'N/A'}")
    print(f"  Last:  {trading_days[-1] if trading_days else 'N/A'}")

    today_check = "20260123"
    print(f"Is {today_check} a trading day? {store.is_trading_day(today_check)}")
    print(f"Previous trading day: {store.prev_trade_date(today_check)}")
    print(f"Next trading day: {store.next_trade_date(today_check)}")

    # 9. Universe demo
    print("\n" + "=" * 60)
    print("Universe (listed stocks count)")
    print("=" * 60)
    df = store.universe(list_status="L")
    print(f"Total listed stocks: {len(df)}")
    # Count by exchange
    if "exchange" in df.columns:
        counts = df.groupby("exchange").size()
        for ex, cnt in counts.items():
            print(f"  {ex}: {cnt}")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        raise SystemExit(0)
