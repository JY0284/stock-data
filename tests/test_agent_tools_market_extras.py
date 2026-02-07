from __future__ import annotations

from pathlib import Path

import pandas as pd


def _set_test_config(monkeypatch, tmp_path: Path, yaml_text: str) -> None:
    cfg_path = tmp_path / "stock_data_test_config.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setenv("STOCK_DATA_CONFIG", str(cfg_path))
    from stock_data.config import clear_config_cache

    clear_config_cache()


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_agent_tools_market_extras(tmp_path: Path, monkeypatch) -> None:
    _set_test_config(monkeypatch, tmp_path, """categories: {}\n""")

    from stock_data.agent_tools import clear_store_cache, get_fx_daily, get_moneyflow

    clear_store_cache()

    store_dir = tmp_path / "store"

    moneyflow_20260102 = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260102",
                "buy_sm_vol": 1,
                "buy_sm_amount": 1.1,
                "sell_sm_vol": 2,
                "sell_sm_amount": 2.2,
                "net_mf_amount": -1.1,
            }
        ]
    )
    _write_parquet(
        store_dir
        / "parquet"
        / "moneyflow"
        / "year=2026"
        / "month=01"
        / "trade_date=20260102.parquet",
        moneyflow_20260102,
    )

    fx_daily = pd.DataFrame(
        [
            {
                "ts_code": "USDCNH.FXCM",
                "trade_date": "20260102",
                "bid_open": 7.1,
                "bid_close": 7.2,
                "ask_open": 7.11,
                "ask_close": 7.21,
                "tick_qty": 123,
                "exchange": "FXCM",
            }
        ]
    )
    _write_parquet(
        store_dir / "parquet" / "fx_daily" / "year=2026" / "month=01" / "trade_date=20260102.parquet",
        fx_daily,
    )

    out = get_moneyflow(trade_date="20260102", store_dir=str(store_dir), limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["trade_date"] == "20260102"

    out2 = get_fx_daily("USDCNH.FXCM", store_dir=str(store_dir), limit=10)
    assert out2["total_count"] == 1
    assert out2["rows"][0]["ts_code"] == "USDCNH.FXCM"
