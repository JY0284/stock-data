from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from stock_data.runner import run_command
from stock_data.store import open_store


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_macro_snapshots_are_readable_and_queryable(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"

    lpr = pd.DataFrame([
        {"date": "20260101", "1y": 3.1},
        {"date": "20260102", "1y": 3.2},
    ])
    cpi = pd.DataFrame([
        {"month": "202601", "nt_val": 101.2},
        {"month": "202512", "nt_val": 100.8},
    ])
    cn_sf = pd.DataFrame([
        {"month": "202601", "sf_tongbi": 12.3},
    ])
    cn_m = pd.DataFrame([
        {"month": "202601", "m2": 290000.0},
    ])

    _write_parquet(store_dir / "parquet" / "lpr" / "latest.parquet", lpr)
    _write_parquet(store_dir / "parquet" / "cpi" / "latest.parquet", cpi)
    _write_parquet(store_dir / "parquet" / "cn_sf" / "latest.parquet", cn_sf)
    _write_parquet(store_dir / "parquet" / "cn_m" / "latest.parquet", cn_m)

    s = open_store(str(store_dir))
    try:
        df = s.read("lpr")
        assert len(df) == 2
        df2 = s.read("cpi")
        assert len(df2) == 2
    finally:
        s.close()

    # Query mode should see v_* views.
    args = SimpleNamespace(cmd="query", store=str(store_dir), rpm=1, workers=1, sql="SELECT count(*) AS n FROM v_lpr")
    assert run_command(args, token="") == 0
