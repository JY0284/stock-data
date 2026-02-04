from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_data.agent_tools import clear_store_cache, get_cn_m, get_cn_sf, get_cpi, get_lpr


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_agent_tools_macro_getters(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"

    lpr = pd.DataFrame([
        {"date": "20260101", "1y": 3.1, "5y": 3.5},
        {"date": "20260102", "1y": 3.2, "5y": 3.6},
    ])
    cpi = pd.DataFrame([
        {"month": "202601", "nt_val": 101.2},
        {"month": "202512", "nt_val": 100.8},
    ])
    cn_sf = pd.DataFrame([
        {"month": "202601", "sf_tongbi": 12.3},
        {"month": "202512", "sf_tongbi": 10.0},
    ])
    cn_m = pd.DataFrame([
        {"month": "202601", "m2": 290000.0},
        {"month": "202512", "m2": 288000.0},
    ])

    _write_parquet(store_dir / "parquet" / "lpr" / "latest.parquet", lpr)
    _write_parquet(store_dir / "parquet" / "cpi" / "latest.parquet", cpi)
    _write_parquet(store_dir / "parquet" / "cn_sf" / "latest.parquet", cn_sf)
    _write_parquet(store_dir / "parquet" / "cn_m" / "latest.parquet", cn_m)

    clear_store_cache(str(store_dir))

    out = get_lpr(store_dir=str(store_dir), limit=10)
    assert out["total_count"] == 2
    assert out["rows"][0]["date"] == "20260102"  # sorted desc

    out = get_lpr(store_dir=str(store_dir), start_date="20260102", end_date="20260102", limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["date"] == "20260102"

    out = get_lpr(store_dir=str(store_dir), date="20260101", columns=["date", "1y"], limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["date"] == "20260101"
    assert "1y" in out["rows"][0]
    assert "5y" not in out["rows"][0]

    out = get_cpi(store_dir=str(store_dir), month="202601", limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["month"] == "202601"

    out = get_cpi(store_dir=str(store_dir), start_month="202601", end_month="202601", limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["month"] == "202601"

    out = get_cn_sf(store_dir=str(store_dir), limit=10)
    assert out["total_count"] == 2
    assert out["rows"][0]["month"] == "202601"

    out = get_cn_sf(store_dir=str(store_dir), start_month="202512", end_month="202512", limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["month"] == "202512"

    out = get_cn_m(store_dir=str(store_dir), month="202512", limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["month"] == "202512"

    out = get_cn_m(store_dir=str(store_dir), start_month="202601", end_month="202601", limit=10)
    assert out["total_count"] == 1
    assert out["rows"][0]["month"] == "202601"
