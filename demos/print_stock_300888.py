from __future__ import annotations

import os
import sys

import duckdb
import pandas as pd


def _connect(store_dir: str) -> duckdb.DuckDBPyConnection:
    duckdb_path = os.path.join(store_dir, "duckdb", "market.duckdb")
    if not os.path.exists(duckdb_path):
        raise FileNotFoundError(f"DuckDB not found: {duckdb_path}")
    return duckdb.connect(duckdb_path, read_only=True)


def _resolve_ts_code(
    con: duckdb.DuckDBPyConnection,
    *,
    parquet_dir: str,
    symbol: str,
) -> tuple[str, str | None]:
    df = con.execute(
        """
        SELECT ts_code, list_date
        FROM read_parquet(?, union_by_name=true)
        WHERE symbol = ?
        ORDER BY ts_code
        """,
        [os.path.join(parquet_dir, "stock_basic", "latest.parquet"), symbol],
    ).fetchdf()

    if df.empty:
        raise RuntimeError(f"symbol not found in stock_basic: {symbol}")

    ts_code = str(df.loc[0, "ts_code"])
    list_date = df.loc[0, "list_date"]
    list_date = str(list_date) if pd.notna(list_date) and str(list_date).strip() else None
    return ts_code, list_date


def _print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> int:
    # Usage: python demos/print_stock_300888.py [symbol] [store_dir]
    symbol = sys.argv[1].strip() if len(sys.argv) >= 2 else "300888"
    store_dir = sys.argv[2].strip() if len(sys.argv) >= 3 else "store"

    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)

    con = _connect(store_dir)

    parquet_dir = os.path.join(store_dir, "parquet")

    _print_section("Resolve symbol -> ts_code")
    ts_code, list_date = _resolve_ts_code(con, parquet_dir=parquet_dir, symbol=symbol)
    print({"symbol": symbol, "ts_code": ts_code, "list_date": list_date})

    _print_section("stock_basic (latest)")
    df_stock_basic = con.execute(
        """
        SELECT *
        FROM read_parquet(?, union_by_name=true)
        WHERE ts_code = ?
        """,
        [os.path.join(parquet_dir, "stock_basic", "latest.parquet"), ts_code],
    ).fetchdf()
    print(df_stock_basic.to_string(index=False))

    _print_section("stock_company (latest)")
    df_stock_company = con.execute(
        """
        SELECT *
        FROM read_parquet(?, union_by_name=true)
        WHERE ts_code = ?
        """,
        [os.path.join(parquet_dir, "stock_company", "latest.parquet"), ts_code],
    ).fetchdf()
    print(df_stock_company.to_string(index=False))

    _print_section("namechange (all years)")
    df_namechange = con.execute(
        """
        SELECT *
        FROM read_parquet(?, union_by_name=true)
        WHERE ts_code = ?
        ORDER BY ann_date
        """,
        [os.path.join(parquet_dir, "namechange", "*.parquet"), ts_code],
    ).fetchdf()
    if df_namechange.empty:
        print("(no rows)")
    else:
        print(df_namechange.to_string(index=False))

    _print_section("new_share (IPO info; all years)")
    # new_share has both ts_code and sub_code; match either.
    df_new_share = con.execute(
        """
        SELECT *
        FROM read_parquet(?, union_by_name=true)
        WHERE ts_code = ? OR sub_code = ?
        ORDER BY ipo_date
        """,
        [os.path.join(parquet_dir, "new_share", "*.parquet"), ts_code, symbol],
    ).fetchdf()
    if df_new_share.empty:
        print("(no rows)")
    else:
        print(df_new_share.to_string(index=False))

    # Use listing date to prune partitions by trade_date (Parquet is partitioned by year/month).
    start_date = list_date or "19900101"

    _print_section(f"daily (all rows; trade_date >= {start_date})")
    df_daily = con.execute(
        """
        SELECT *
        FROM v_daily
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    print(df_daily.to_string(index=False))

    _print_section(f"adj_factor (all rows; trade_date >= {start_date})")
    df_adj = con.execute(
        """
        SELECT *
        FROM v_adj_factor
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    print(df_adj.to_string(index=False))

    _print_section(f"daily_basic (all rows; trade_date >= {start_date})")
    df_daily_basic = con.execute(
        """
        SELECT *
        FROM v_daily_basic
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    print(df_daily_basic.to_string(index=False))

    _print_section(f"stk_limit (all rows; trade_date >= {start_date})")
    df_stk_limit = con.execute(
        """
        SELECT *
        FROM v_stk_limit
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    if df_stk_limit.empty:
        print("(no rows)")
    else:
        print(df_stk_limit.to_string(index=False))

    _print_section(f"suspend_d (all rows; trade_date >= {start_date})")
    df_suspend = con.execute(
        """
        SELECT *
        FROM v_suspend_d
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    if df_suspend.empty:
        print("(no rows)")
    else:
        print(df_suspend.to_string(index=False))

    _print_section(f"weekly (all rows; trade_date >= {start_date})")
    df_weekly = con.execute(
        """
        SELECT *
        FROM v_weekly
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    if df_weekly.empty:
        print("(no rows)")
    else:
        print(df_weekly.to_string(index=False))

    _print_section(f"monthly (all rows; trade_date >= {start_date})")
    df_monthly = con.execute(
        """
        SELECT *
        FROM v_monthly
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    if df_monthly.empty:
        print("(no rows)")
    else:
        print(df_monthly.to_string(index=False))

    _print_section(f"derived: v_daily_adj (daily + adj_factor; trade_date >= {start_date})")
    df_daily_adj = con.execute(
        """
        SELECT *
        FROM v_daily_adj
        WHERE ts_code = ? AND trade_date >= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date],
    ).fetchdf()
    if df_daily_adj.empty:
        print("(no rows)")
    else:
        print(df_daily_adj.to_string(index=False))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        # Allow `python demos/print_stock_300888.py ... | head` without a noisy traceback.
        raise SystemExit(0)
