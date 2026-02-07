from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from stock_data.web_service import WebSettings, create_app


def _set_test_config(monkeypatch, tmp_path: Path, yaml_text: str) -> None:
    cfg_path = tmp_path / "stock_data_test_config.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setenv("STOCK_DATA_CONFIG", str(cfg_path))
    from stock_data.config import clear_config_cache

    clear_config_cache()


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_web_service_market_extras_endpoints(tmp_path: Path, monkeypatch) -> None:
    # Force-enable all categories regardless of repo-level stock_data.yaml
    _set_test_config(monkeypatch, tmp_path, """categories: {}\n""")

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

    app = create_app(settings=WebSettings(store_dir=str(store_dir), cache_enabled=False))
    with TestClient(app) as client:
        r = client.get("/datasets")
        assert r.status_code == 200
        names = {d["name"] for d in r.json()["datasets"]}
        assert "moneyflow" in names
        assert "fx_daily" in names

        r = client.get("/moneyflow", params={"trade_date": "20260102", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "moneyflow"
        assert body["rows"] == 1
        assert body["data"][0]["trade_date"] == "20260102"

        r = client.get("/fx_daily", params={"ts_code": "USDCNH.FXCM", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "fx_daily"
        assert body["ts_code"] == "USDCNH.FXCM"
        assert body["rows"] == 1
        assert body["data"][0]["trade_date"] == "20260102"

        # /query should also work for both datasets
        r = client.get("/query", params={"dataset": "moneyflow", "where": '{"trade_date":"20260102"}', "limit": 10})
        assert r.status_code == 200
        assert r.json()["rows"] == 1

        r = client.get("/query", params={"dataset": "fx_daily", "ts_code": "USDCNH.FXCM", "limit": 10})
        assert r.status_code == 200
        assert r.json()["rows"] == 1
