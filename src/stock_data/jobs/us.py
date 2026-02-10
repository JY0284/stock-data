from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from stock_data.rate_limit import RateLimiter
from stock_data.retry import RateLimitError, TransientError
from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.storage.parquet_writer import ParquetWriter
from stock_data.tushare_client import TushareClient


logger = logging.getLogger(__name__)


def _normalize_trade_date_partition_key(key: str | None) -> str | None:
    if not key:
        return None
    if key.startswith("trade_date="):
        return key.split("=", 1)[1]
    return key


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def _add_days_yyyymmdd(s: str, days: int) -> str:
    d = _dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    return (d + _dt.timedelta(days=days)).strftime("%Y%m%d")


def _fetch_us_open_trade_dates(client: TushareClient, start_date: str, end_date: str) -> list[str]:
    """Fetch US open trading dates using us_tradecal API."""
    df = client.query_all("us_tradecal", start_date=start_date, end_date=end_date, is_open="1")
    if df is None or df.empty:
        return []
    out = df["cal_date"].astype(str).tolist()
    return sorted(out)


def run_us(
    cfg: RunConfig,
    *,
    token: str,
    catalog: DuckDBCatalog,
    datasets: list[str],
    start_date: str | None,
    end_date: str,
) -> None:
    """Ingest US stock datasets.

    Datasets:
    - us_basic: US stock list (snapshot)
    - us_tradecal: US trading calendar (snapshot)
    - us_daily: US daily bars (trade_date partitioned)
    """
    if not datasets:
        return

    limiter = RateLimiter(rpm=cfg.rpm)
    client = TushareClient(token=token, limiter=limiter)
    w = _writer(cfg)

    # 1) us_basic snapshot (paginated API, max 6000 per page)
    if "us_basic" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="us_basic", partition_key=key, status="running")
        try:
            df = client.query_all("us_basic", page_size=6000)
            if df is None:
                df = pd.DataFrame()
            if not df.empty and "ts_code" in df.columns:
                df = df.drop_duplicates(subset=["ts_code"], keep="first")
            w.write_snapshot("us_basic", df, name="latest")
            catalog.set_state(dataset="us_basic", partition_key=key, status="completed", row_count=int(len(df)))
            logger.info("us: us_basic completed rows=%d", len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="us_basic", partition_key=key, status="failed", error=str(e))
            raise

    # 2) us_tradecal snapshot
    if "us_tradecal" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="us_tradecal", partition_key=key, status="running")
        try:
            # Fetch full calendar from 1990 to end_date
            df = client.query_all("us_tradecal", start_date="19900101", end_date=end_date)
            if df is None:
                df = pd.DataFrame()
            w.write_snapshot("us_tradecal", df, name="latest")
            catalog.set_state(dataset="us_tradecal", partition_key=key, status="completed", row_count=int(len(df)))
            logger.info("us: us_tradecal completed rows=%d", len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="us_tradecal", partition_key=key, status="failed", error=str(e))
            raise

    # 3) us_daily (trade_date partitioned)
    if "us_daily" in datasets:
        _run_us_daily(cfg, client, catalog, w, start_date=start_date, end_date=end_date)


def _run_us_daily(
    cfg: RunConfig,
    client: TushareClient,
    catalog: DuckDBCatalog,
    w: ParquetWriter,
    *,
    start_date: str | None,
    end_date: str,
) -> None:
    """Run ingestion for us_daily (trade_date partitioned)."""

    # Get US trading calendar
    cal_start = start_date or "20000101"  # US stocks data typically available from 2000+
    if start_date is None:
        # In update mode, look back from last completed partition
        last_raw = catalog.last_completed_partition("us_daily", min_row_count=1)
        last = _normalize_trade_date_partition_key(last_raw)
        if last:
            cal_start = last
        else:
            # No local history yet; keep bounded
            cal_start = _add_days_yyyymmdd(end_date, -120)

    open_dates = _fetch_us_open_trade_dates(client, cal_start, end_date)
    if not open_dates:
        logger.info("us: no US open dates in [%s, %s]", cal_start, end_date)
        return
    effective_end = open_dates[-1]

    # Determine which dates need fetching
    completed_raw = catalog.completed_partitions("us_daily", min_row_count=1)
    completed = {
        d
        for d in (_normalize_trade_date_partition_key(k) for k in completed_raw)
        if d is not None
    }
    if start_date is None:
        # Update mode: from last completed to now
        last_raw = catalog.last_completed_partition("us_daily", min_row_count=1)
        last = _normalize_trade_date_partition_key(last_raw)
        if last:
            start_idx = bisect_left(open_dates, last)
            if start_idx >= len(open_dates):
                start_idx = max(0, len(open_dates) - 1)
            candidates = open_dates[start_idx:]
        else:
            candidates = [effective_end]
        tasks = [d for d in candidates if d not in completed]
    else:
        # Backfill mode
        tasks = [d for d in open_dates if start_date <= d <= effective_end and d not in completed]

    if not tasks:
        logger.info("us: nothing to do for us_daily (effective_end=%s)", effective_end)
        return

    logger.info(
        "us: start us_daily (tasks=%d, workers=%d, rpm=%d)",
        len(tasks),
        cfg.workers,
        cfg.rpm,
    )

    running: set[str] = set()
    running_lock = threading.Lock()

    def _fetch_and_store(trade_date: str) -> int:
        key = trade_date
        catalog.set_state(dataset="us_daily", partition_key=key, status="running")
        with running_lock:
            running.add(trade_date)
        try:
            logger.debug("us: fetch start us_daily trade_date=%s", trade_date)
            df = client.query_all("us_daily", trade_date=trade_date)
            if df is None:
                df = pd.DataFrame()

            if df.empty:
                catalog.set_state(
                    dataset="us_daily",
                    partition_key=key,
                    status="skipped",
                    row_count=0,
                    error="empty response for us_daily",
                )
                logger.warning("us: skipped empty us_daily trade_date=%s", trade_date)
                return 0

            w.write_trade_date_partition("us_daily", trade_date, df)
            catalog.set_state(dataset="us_daily", partition_key=key, status="completed", row_count=int(len(df)))
            logger.debug("us: fetch done us_daily trade_date=%s rows=%d", trade_date, len(df))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="us_daily", partition_key=key, status="failed", error=str(e))
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning("us: fetch failed us_daily trade_date=%s (%s)", trade_date, e)
            else:
                logger.exception("us: fetch failed us_daily trade_date=%s", trade_date)
            raise
        finally:
            with running_lock:
                running.discard(trade_date)

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        future_to_task = {ex.submit(_fetch_and_store, d): d for d in tasks}
        console = Console(stderr=True)
        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:
            ptask = progress.add_task("us_daily", total=len(future_to_task))
            failed: list[tuple[str, str]] = []

            for f in as_completed(future_to_task):
                d = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"us_daily (running={running_count} failed={len(failed)}) last={d} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((d, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"us_daily (running={running_count} failed={len(failed)}) last=FAILED {d}",
                    )
                finally:
                    progress.advance(ptask, 1)

            if failed:
                sample = "; ".join([d for (d, _) in failed[:10]])
                logger.error("us: %d us_daily task(s) failed; sample: %s", len(failed), sample)
                raise RuntimeError(f"us: {len(failed)} us_daily task(s) failed; sample: {sample}")

    logger.info("us: completed us_daily (tasks=%d)", len(tasks))
