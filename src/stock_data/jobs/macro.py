from __future__ import annotations

import logging

import pandas as pd

from stock_data.rate_limit import RateLimiter
from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.storage.parquet_writer import ParquetWriter
from stock_data.tushare_client import TushareClient


logger = logging.getLogger(__name__)


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def _yyyymmdd_to_yyyymm(d: str) -> str:
    s = str(d)
    if len(s) < 6:
        raise ValueError(f"Invalid yyyymmdd: {d}")
    return s[:6]


def run_macro(
    cfg: RunConfig,
    *,
    token: str,
    catalog: DuckDBCatalog,
    datasets: list[str],
    start_date: str | None,
    end_date: str,
) -> None:
    """Ingest simple macro datasets as single snapshot parquet files.

    Design: clear and simple.
    - store as `parquet/<dataset>/latest.parquet`
    - use `asof=<end_date>` ingestion_state key

    Datasets:
    - lpr: Tushare `shibor_lpr` (daily-ish) via start_date/end_date
    - cpi: Tushare `cn_cpi` (monthly) via start_m/end_m
    - cn_sf: 社融 (monthly) via start_m/end_m
    - cn_m: 货币供应量 (monthly) via start_m/end_m

    Note: Some endpoints require sufficient permissions/points.
    """

    if not datasets:
        return

    limiter = RateLimiter(rpm=cfg.rpm)
    client = TushareClient(token=token, limiter=limiter)
    w = _writer(cfg)

    # Reasonable defaults if caller didn't supply start_date (update mode).
    # Keep bounded so calls are fast and avoid huge responses.
    if start_date is None:
        # last ~10 years
        start_date = f"{int(end_date[:4]) - 9}0101"

    start_m = _yyyymmdd_to_yyyymm(start_date)
    end_m = _yyyymmdd_to_yyyymm(end_date)

    def _write_snapshot(ds: str, df: pd.DataFrame) -> None:
        w.write_snapshot(ds, df, name="latest")

    if "lpr" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="lpr", partition_key=key, status="running")
        try:
            df = client.query("shibor_lpr", start_date=str(start_date), end_date=str(end_date))
            if df is None:
                df = pd.DataFrame()
            _write_snapshot("lpr", df)
            catalog.set_state(dataset="lpr", partition_key=key, status="completed", row_count=int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="lpr", partition_key=key, status="failed", error=str(e))
            raise

    if "cpi" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="cpi", partition_key=key, status="running")
        try:
            df = client.query("cn_cpi", start_m=str(start_m), end_m=str(end_m))
            if df is None:
                df = pd.DataFrame()
            _write_snapshot("cpi", df)
            catalog.set_state(dataset="cpi", partition_key=key, status="completed", row_count=int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="cpi", partition_key=key, status="failed", error=str(e))
            raise

    if "cn_sf" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="cn_sf", partition_key=key, status="running")
        try:
            # 社融（月度）接口名为 sf_month
            df = client.query("sf_month", start_m=str(start_m), end_m=str(end_m))
            if df is None:
                df = pd.DataFrame()
            _write_snapshot("cn_sf", df)
            catalog.set_state(dataset="cn_sf", partition_key=key, status="completed", row_count=int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="cn_sf", partition_key=key, status="failed", error=str(e))
            raise

    if "cn_m" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="cn_m", partition_key=key, status="running")
        try:
            df = client.query("cn_m", start_m=str(start_m), end_m=str(end_m))
            if df is None:
                df = pd.DataFrame()
            _write_snapshot("cn_m", df)
            catalog.set_state(dataset="cn_m", partition_key=key, status="completed", row_count=int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="cn_m", partition_key=key, status="failed", error=str(e))
            raise
