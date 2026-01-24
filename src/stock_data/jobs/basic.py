from __future__ import annotations

import os
from typing import Iterable

import pandas as pd

from stock_data.rate_limit import RateLimiter
from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.storage.parquet_writer import ParquetWriter
from stock_data.tushare_client import TushareClient
from stock_data.utils_dates import year_windows


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def run_basic(
    cfg: RunConfig,
    *,
    token: str,
    catalog: DuckDBCatalog,
    datasets: list[str],
    start_date: str | None,
    end_date: str,
) -> None:
    if not datasets:
        return

    limiter = RateLimiter(rpm=cfg.rpm)
    client = TushareClient(token=token, limiter=limiter)
    w = _writer(cfg)

    # 1) stock_basic snapshot
    if "stock_basic" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="stock_basic", partition_key=key, status="running")
        try:
            df = client.query(
                "stock_basic",
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type",
            )
            # also include delisted/paused for completeness
            df_d = client.query(
                "stock_basic",
                exchange="",
                list_status="D",
                fields="ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type",
            )
            df_p = client.query(
                "stock_basic",
                exchange="",
                list_status="P",
                fields="ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type",
            )
            df_all = pd.concat([df, df_d, df_p], ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")
            w.write_snapshot("stock_basic", df_all, name="latest")
            catalog.set_state(dataset="stock_basic", partition_key=key, status="completed", row_count=int(len(df_all)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="stock_basic", partition_key=key, status="failed", error=str(e))
            raise

    # 2) trade_cal snapshot (SSE by default; used for date generation)
    if "trade_cal" in datasets:
        key = f"SSE:{end_date}"
        catalog.set_state(dataset="trade_cal", partition_key=key, status="running")
        try:
            df = client.query("trade_cal", exchange="SSE", start_date="19900101", end_date=end_date, is_open="")
            w.write_snapshot("trade_cal", df, name="SSE_latest")
            catalog.set_state(dataset="trade_cal", partition_key=key, status="completed", row_count=int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="trade_cal", partition_key=key, status="failed", error=str(e))
            raise

    # 3) stock_company snapshot (split by exchange)
    if "stock_company" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="stock_company", partition_key=key, status="running")
        try:
            parts = []
            for ex in ["SSE", "SZSE", "BSE"]:
                parts.append(
                    client.query(
                        "stock_company",
                        exchange=ex,
                        fields="ts_code,com_name,com_id,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope",
                    )
                )
            df = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")
            w.write_snapshot("stock_company", df, name="latest")
            catalog.set_state(dataset="stock_company", partition_key=key, status="completed", row_count=int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="stock_company", partition_key=key, status="failed", error=str(e))
            raise

    # 4) new_share (IPO list) – windowed by year during backfill; else keep last 10y snapshot.
    if "new_share" in datasets:
        if start_date:
            windows = year_windows(start_date, end_date)
        else:
            # update mode: last 10 years is usually enough and bounded
            y = int(end_date[:4]) - 9
            windows = year_windows(f"{y}0101", end_date)

        for (year, ws, we) in windows:
            key = f"year={year}"
            if key in catalog.completed_partitions("new_share") and start_date is None:
                # For update mode, we still want to refresh the most recent year.
                if year != end_date[:4]:
                    continue
            catalog.set_state(dataset="new_share", partition_key=key, status="running")
            try:
                df = client.query("new_share", start_date=ws, end_date=we)
                w.write_snapshot("new_share", df, name=f"year={year}")
                catalog.set_state(dataset="new_share", partition_key=key, status="completed", row_count=int(len(df)))
            except Exception as e:  # noqa: BLE001
                catalog.set_state(dataset="new_share", partition_key=key, status="failed", error=str(e))
                raise

    # 5) namechange – windowed by year (ann_date window)
    if "namechange" in datasets:
        if start_date:
            windows = year_windows(start_date, end_date)
        else:
            y = int(end_date[:4]) - 9
            windows = year_windows(f"{y}0101", end_date)

        for (year, ws, we) in windows:
            key = f"year={year}"
            if key in catalog.completed_partitions("namechange") and start_date is None:
                if year != end_date[:4]:
                    continue
            catalog.set_state(dataset="namechange", partition_key=key, status="running")
            try:
                df = client.query("namechange", start_date=ws, end_date=we)
                w.write_snapshot("namechange", df, name=f"year={year}")
                catalog.set_state(dataset="namechange", partition_key=key, status="completed", row_count=int(len(df)))
            except Exception as e:  # noqa: BLE001
                catalog.set_state(dataset="namechange", partition_key=key, status="failed", error=str(e))
                raise

