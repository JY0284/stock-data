from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_data.store import open_store


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_trade_date_read_accepts_iso_dates(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    parquet_dir = store_dir / "parquet"

    # Minimal stock_basic required for store initialization paths in some helpers.
    _write_parquet(
        parquet_dir / "stock_basic" / "latest.parquet",
        pd.DataFrame(
            [
                {
                    "ts_code": "513100.SH",
                    "symbol": "513100",
                    "name": "Test ETF",
                    "list_date": "20100101",
                    "list_status": "L",
                    "exchange": "SSE",
                }
            ]
        ),
    )

    # Trade calendar used for partition pruning.
    _write_parquet(
        parquet_dir / "trade_cal" / "SSE_latest.parquet",
        pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20260207", "is_open": 1, "pretrade_date": "20260206"},
                {"exchange": "SSE", "cal_date": "20260210", "is_open": 1, "pretrade_date": "20260207"},
            ]
        ),
    )

    # Write two etf_daily partitions.
    _write_parquet(
        parquet_dir / "etf_daily" / "year=2026" / "month=02" / "trade_date=20260207.parquet",
        pd.DataFrame(
            [
                {
                    "ts_code": "513100.SH",
                    "trade_date": "20260207",
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                    "vol": 100.0,
                    "amount": 1000.0,
                }
            ]
        ),
    )
    _write_parquet(
        parquet_dir / "etf_daily" / "year=2026" / "month=02" / "trade_date=20260210.parquet",
        pd.DataFrame(
            [
                {
                    "ts_code": "513100.SH",
                    "trade_date": "20260210",
                    "open": 2.0,
                    "high": 2.1,
                    "low": 1.9,
                    "close": 2.05,
                    "vol": 200.0,
                    "amount": 2000.0,
                }
            ]
        ),
    )

    store = open_store(str(store_dir))
    try:
        df_iso = store.read(
            "etf_daily",
            where={"ts_code": "513100.SH"},
            start_date="2026-02-07",
            end_date="2026-02-10",
            order_by="trade_date",
        )
        df_raw = store.read(
            "etf_daily",
            where={"ts_code": "513100.SH"},
            start_date="20260207",
            end_date="20260210",
            order_by="trade_date",
        )
    finally:
        store.close()

    assert len(df_iso) == 2
    assert len(df_raw) == 2
    assert df_iso["trade_date"].astype(str).tolist() == df_raw["trade_date"].astype(str).tolist()


def test_invalid_date_format_raises(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    parquet_dir = store_dir / "parquet"

    _write_parquet(
        parquet_dir / "stock_basic" / "latest.parquet",
        pd.DataFrame(
            [
                {
                    "ts_code": "513100.SH",
                    "symbol": "513100",
                    "name": "Test ETF",
                    "list_date": "20100101",
                    "list_status": "L",
                    "exchange": "SSE",
                }
            ]
        ),
    )

    store = open_store(str(store_dir))
    try:
        try:
            store.read("etf_daily", start_date="2026.02.07", end_date="20260210")
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "YYYYMMDD" in str(e)
    finally:
        store.close()
