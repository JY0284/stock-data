from __future__ import annotations

import logging
import os
import datetime as _dt

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


def _add_days_yyyymmdd(s: str, days: int) -> str:
    d = _dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    return (d + _dt.timedelta(days=days)).strftime("%Y%m%d")


def _add_months_yyyymm(s: str, months: int) -> str:
    """Add months to YYYYMM (can be negative)."""
    y = int(s[:4])
    m = int(s[4:6])
    idx = (y * 12 + (m - 1)) + int(months)
    if idx < 0:
        idx = 0
    y2, m0 = divmod(idx, 12)
    return f"{y2:04d}{m0 + 1:02d}"


def _read_snapshot_if_exists(parquet_dir: str, ds: str) -> pd.DataFrame | None:
    path = os.path.join(parquet_dir, ds, "latest.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        return df if isinstance(df, pd.DataFrame) else None
    except Exception:
        return None


def _merge_snapshot(existing: pd.DataFrame | None, new: pd.DataFrame, *, key_col: str) -> pd.DataFrame:
    if existing is None or existing.empty:
        merged = new
    else:
        merged = pd.concat([existing, new], ignore_index=True)

    if key_col in merged.columns:
        merged[key_col] = merged[key_col].astype(str)
        merged = merged.sort_values(key_col, kind="stable")
        merged = merged.drop_duplicates(subset=[key_col], keep="last")
        merged = merged.reset_index(drop=True)
    return merged


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

    # Update-mode optimization: for snapshot datasets, avoid re-pulling a huge
    # history window every run. If we already have a local snapshot, fetch only
    # the tail via Tushare daterange params and merge/dedupe.
    inc_backfill_days = 14
    inc_backfill_months = 1
    try:
        v = os.environ.get("STOCK_DATA_MACRO_INCREMENTAL_BACKFILL_DAYS")
        if v is not None and str(v).strip() != "":
            inc_backfill_days = max(0, int(v))
    except Exception:
        inc_backfill_days = 14
    try:
        v = os.environ.get("STOCK_DATA_MACRO_INCREMENTAL_BACKFILL_MONTHS")
        if v is not None and str(v).strip() != "":
            inc_backfill_months = max(0, int(v))
    except Exception:
        inc_backfill_months = 1

    end_m = _yyyymmdd_to_yyyymm(end_date)

    def _write_snapshot(ds: str, df: pd.DataFrame) -> None:
        w.write_snapshot(ds, df, name="latest")

    if "lpr" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="lpr", partition_key=key, status="running")
        try:
            existing = _read_snapshot_if_exists(cfg.parquet_dir, "lpr") if start_date is None else None
            eff_start = start_date
            if eff_start is None:
                eff_start = f"{int(end_date[:4]) - 9}0101"
                if existing is not None and not existing.empty and "date" in existing.columns:
                    max_date = str(existing["date"].astype(str).max())
                    if max_date and len(max_date) == 8:
                        eff_start = _add_days_yyyymmdd(max_date, -inc_backfill_days)

            df_new = client.query_all("shibor_lpr", start_date=str(eff_start), end_date=str(end_date))
            if df_new is None:
                df_new = pd.DataFrame()
            if df_new.empty and existing is not None:
                catalog.set_state(dataset="lpr", partition_key=key, status="skipped", row_count=0, error="empty response")
            else:
                df_out = _merge_snapshot(existing, df_new, key_col="date") if start_date is None else df_new
                _write_snapshot("lpr", df_out)
                catalog.set_state(dataset="lpr", partition_key=key, status="completed", row_count=int(len(df_out)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="lpr", partition_key=key, status="failed", error=str(e))
            raise

    if "cpi" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="cpi", partition_key=key, status="running")
        try:
            existing = _read_snapshot_if_exists(cfg.parquet_dir, "cpi") if start_date is None else None
            eff_start_m = _yyyymmdd_to_yyyymm(start_date) if start_date is not None else f"{int(end_date[:4]) - 9}01"
            if start_date is None and existing is not None and not existing.empty and "month" in existing.columns:
                max_m = str(existing["month"].astype(str).max())
                if max_m and len(max_m) == 6:
                    eff_start_m = _add_months_yyyymm(max_m, -inc_backfill_months)

            df_new = client.query_all("cn_cpi", start_m=str(eff_start_m), end_m=str(end_m))
            if df_new is None:
                df_new = pd.DataFrame()
            if df_new.empty and existing is not None:
                catalog.set_state(dataset="cpi", partition_key=key, status="skipped", row_count=0, error="empty response")
            else:
                df_out = _merge_snapshot(existing, df_new, key_col="month") if start_date is None else df_new
                _write_snapshot("cpi", df_out)
                catalog.set_state(dataset="cpi", partition_key=key, status="completed", row_count=int(len(df_out)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="cpi", partition_key=key, status="failed", error=str(e))
            raise

    if "cn_sf" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="cn_sf", partition_key=key, status="running")
        try:
            # 社融（月度）接口名为 sf_month
            existing = _read_snapshot_if_exists(cfg.parquet_dir, "cn_sf") if start_date is None else None
            eff_start_m = _yyyymmdd_to_yyyymm(start_date) if start_date is not None else f"{int(end_date[:4]) - 9}01"
            if start_date is None and existing is not None and not existing.empty and "month" in existing.columns:
                max_m = str(existing["month"].astype(str).max())
                if max_m and len(max_m) == 6:
                    eff_start_m = _add_months_yyyymm(max_m, -inc_backfill_months)

            df_new = client.query_all("sf_month", start_m=str(eff_start_m), end_m=str(end_m))
            if df_new is None:
                df_new = pd.DataFrame()
            if df_new.empty and existing is not None:
                catalog.set_state(dataset="cn_sf", partition_key=key, status="skipped", row_count=0, error="empty response")
            else:
                df_out = _merge_snapshot(existing, df_new, key_col="month") if start_date is None else df_new
                _write_snapshot("cn_sf", df_out)
                catalog.set_state(dataset="cn_sf", partition_key=key, status="completed", row_count=int(len(df_out)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="cn_sf", partition_key=key, status="failed", error=str(e))
            raise

    if "cn_m" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="cn_m", partition_key=key, status="running")
        try:
            existing = _read_snapshot_if_exists(cfg.parquet_dir, "cn_m") if start_date is None else None
            eff_start_m = _yyyymmdd_to_yyyymm(start_date) if start_date is not None else f"{int(end_date[:4]) - 9}01"
            if start_date is None and existing is not None and not existing.empty and "month" in existing.columns:
                max_m = str(existing["month"].astype(str).max())
                if max_m and len(max_m) == 6:
                    eff_start_m = _add_months_yyyymm(max_m, -inc_backfill_months)

            df_new = client.query_all("cn_m", start_m=str(eff_start_m), end_m=str(end_m))
            if df_new is None:
                df_new = pd.DataFrame()
            if df_new.empty and existing is not None:
                catalog.set_state(dataset="cn_m", partition_key=key, status="skipped", row_count=0, error="empty response")
            else:
                df_out = _merge_snapshot(existing, df_new, key_col="month") if start_date is None else df_new
                _write_snapshot("cn_m", df_out)
                catalog.set_state(dataset="cn_m", partition_key=key, status="completed", row_count=int(len(df_out)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="cn_m", partition_key=key, status="failed", error=str(e))
            raise
