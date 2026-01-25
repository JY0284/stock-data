from __future__ import annotations

import datetime as _dt
import logging
import threading
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
from stock_data.utils_dates import parse_yyyymmdd


logger = logging.getLogger(__name__)


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def _generate_quarterly_periods(start_date: str, end_date: str) -> list[str]:
    """Generate quarterly report periods (end_date) from start_date to end_date.
    
    Returns periods like: 20100331, 20100630, 20100930, 20101231, ...
    """
    start = parse_yyyymmdd(start_date)
    end = parse_yyyymmdd(end_date)
    
    periods = []
    year = start.year
    # Start from Q1 of the start year
    while True:
        for month in [3, 6, 9, 12]:
            # Last day of the quarter
            if month == 3:
                period_date = _dt.date(year, 3, 31)
            elif month == 6:
                period_date = _dt.date(year, 6, 30)
            elif month == 9:
                period_date = _dt.date(year, 9, 30)
            else:  # month == 12
                period_date = _dt.date(year, 12, 31)
            
            if period_date < start:
                continue
            if period_date > end:
                return periods
            
            periods.append(period_date.strftime("%Y%m%d"))
        
        year += 1
        if year > end.year + 1:  # Safety break
            break
    
    return periods


def _run_ts_code_datasets(
    cfg: RunConfig,
    client: TushareClient,
    catalog: DuckDBCatalog,
    w: ParquetWriter,
    datasets: list[str],
) -> None:
    """Run ingestion for ts_code-based datasets (one file per stock).
    
    For dividend and fina_audit, we query each stock individually and store
    the full history as <dataset>/ts_code=<code>.parquet
    """
    import os
    
    # Get stock codes
    stock_codes = _get_stock_codes(catalog)
    logger.info("finance: found %d stock codes for ts_code datasets", len(stock_codes))
    
    # Build tasks: (dataset, ts_code)
    tasks: list[tuple[str, str]] = []
    for ds in datasets:
        completed = catalog.completed_partitions(ds)
        for code in stock_codes:
            # Partition key uses safe format
            key = f"ts_code={code.replace('.', '_')}"
            if key not in completed:
                tasks.append((ds, code))
    
    if not tasks:
        logger.info("finance: nothing to do for ts_code datasets=%s", ",".join(datasets))
        return
    
    logger.info(
        "finance: start ts_code datasets (datasets=%s, tasks=%d, workers=%d, rpm=%d)",
        ",".join(sorted(set(datasets))),
        len(tasks),
        cfg.workers,
        cfg.rpm,
    )
    
    running: set[tuple[str, str]] = set()
    running_lock = threading.Lock()
    
    def _fetch_and_store(dataset: str, ts_code: str) -> int:
        key = f"ts_code={ts_code.replace('.', '_')}"
        catalog.set_state(dataset=dataset, partition_key=key, status="running")
        with running_lock:
            running.add((dataset, ts_code))
        try:
            logger.debug("finance: fetch start dataset=%s ts_code=%s", dataset, ts_code)
            
            # Query by ts_code
            df = client.query(dataset, ts_code=ts_code)
            
            if df is None:
                df = pd.DataFrame()
            
            w.write_ts_code_partition(dataset, ts_code, df)
            catalog.set_state(dataset=dataset, partition_key=key, status="completed", row_count=int(len(df)))
            logger.debug("finance: fetch done dataset=%s ts_code=%s rows=%d", dataset, ts_code, int(len(df)))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset=dataset, partition_key=key, status="failed", error=str(e))
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning(
                    "finance: fetch failed dataset=%s ts_code=%s (%s)",
                    dataset,
                    ts_code,
                    e,
                )
            else:
                logger.exception("finance: fetch failed dataset=%s ts_code=%s", dataset, ts_code)
            raise
        finally:
            with running_lock:
                running.discard((dataset, ts_code))
    
    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        future_to_task = {ex.submit(_fetch_and_store, ds, code): (ds, code) for (ds, code) in tasks}
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
            ptask = progress.add_task("finance-tscode", total=len(future_to_task))
            failed: list[tuple[str, str, str]] = []
            
            for f in as_completed(future_to_task):
                ds, code = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"finance-tscode (running={running_count} failed={len(failed)}) last={ds} {code} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((ds, code, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"finance-tscode (running={running_count} failed={len(failed)}) last=FAILED {ds} {code}",
                    )
                finally:
                    progress.advance(ptask, 1)
            
            if failed:
                sample = "; ".join([f"{ds}:{code}" for (ds, code, _) in failed[:10]])
                logger.error("finance: %d ts_code task(s) failed; sample: %s", len(failed), sample)
                raise RuntimeError(f"finance: {len(failed)} ts_code task(s) failed; sample: {sample}")
    
    logger.info("finance: completed ts_code datasets (tasks=%d)", len(tasks))


def _get_stock_codes(catalog: DuckDBCatalog) -> list[str]:
    """Get all stock codes from stock_basic parquet file."""
    import os
    stock_basic_path = os.path.join(catalog.parquet_root, "stock_basic", "latest.parquet")
    if not os.path.exists(stock_basic_path):
        raise RuntimeError("stock_basic not found. Run basic datasets first.")
    import pandas as pd
    df = pd.read_parquet(stock_basic_path)
    return df["ts_code"].tolist()


def _get_index_codes(catalog: DuckDBCatalog) -> list[str]:
    """Get all index codes from index_basic parquet file."""
    import os
    index_basic_path = os.path.join(catalog.parquet_root, "index_basic", "latest.parquet")
    if not os.path.exists(index_basic_path):
        raise RuntimeError("index_basic not found. Run basic datasets first.")
    import pandas as pd
    df = pd.read_parquet(index_basic_path)
    return df["ts_code"].tolist()


def run_finance(
    cfg: RunConfig,
    *,
    token: str,
    catalog: DuckDBCatalog,
    datasets: list[str],
    start_date: str | None,
    end_date: str,
) -> None:
    """Run financial data ingestion for VIP endpoints (full-market quarterly data).
    
    Financial datasets are partitioned by report period (end_date), not trading date.
    We use *_vip endpoints to fetch all stocks for a given quarter in one call.
    
    Some datasets (dividend, fina_audit) require ts_code parameter and are partitioned
    by ts_code (one file per stock containing full history).
    """
    if not datasets:
        return

    limiter = RateLimiter(rpm=cfg.rpm)
    client = TushareClient(token=token, limiter=limiter)
    w = _writer(cfg)

    # Separate dataset types
    snapshot_datasets = {"disclosure_date"}
    ts_code_datasets = {"dividend", "fina_audit"}
    period_datasets = [d for d in datasets if d not in snapshot_datasets and d not in ts_code_datasets]
    
    # Handle snapshot datasets first
    if "disclosure_date" in datasets:
        key = f"asof={end_date}"
        catalog.set_state(dataset="disclosure_date", partition_key=key, status="running")
        try:
            logger.info("finance: fetching disclosure_date snapshot")
            df = client.query("disclosure_date", end_date=end_date)
            if df is None:
                df = pd.DataFrame()
            w.write_snapshot("disclosure_date", df, name="latest")
            catalog.set_state(dataset="disclosure_date", partition_key=key, status="completed", row_count=int(len(df)))
            logger.info("finance: disclosure_date completed (rows=%d)", int(len(df)))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="disclosure_date", partition_key=key, status="failed", error=str(e))
            logger.exception("finance: disclosure_date failed")
            raise

    # Handle ts_code-based datasets (dividend, fina_audit)
    ts_code_to_fetch = [d for d in datasets if d in ts_code_datasets]
    if ts_code_to_fetch:
        _run_ts_code_datasets(cfg, client, catalog, w, ts_code_to_fetch)

    if not period_datasets:
        return

    # Generate quarterly periods
    if start_date is None:
        # Update mode: start from last completed partition per dataset
        # For simplicity, we'll use the last 10 years as a reasonable window
        start_year = int(end_date[:4]) - 10
        start_date = f"{start_year}0101"
    
    periods = _generate_quarterly_periods(start_date, end_date)
    if not periods:
        logger.info("finance: no periods in [%s, %s]", start_date, end_date)
        return

    logger.info(
        "finance: generated %d periods from %s to %s",
        len(periods),
        periods[0] if periods else "-",
        periods[-1] if periods else "-",
    )

    # Build tasks: (dataset, period)
    tasks: list[tuple[str, str]] = []
    for ds in period_datasets:
        completed = catalog.completed_partitions(ds)
        for period in periods:
            if period not in completed:
                tasks.append((ds, period))

    if not tasks:
        logger.info(
            "finance: nothing to do (datasets=%s, start=%s, end=%s)",
            ",".join(period_datasets),
            start_date,
            end_date,
        )
        return

    logger.info(
        "finance: start (datasets=%s, tasks=%d, workers=%d, rpm=%d)",
        ",".join(sorted(set(period_datasets))),
        len(tasks),
        cfg.workers,
        cfg.rpm,
    )

    # Map dataset names to their VIP API endpoints
    vip_endpoints = {
        "income": "income_vip",
        "balancesheet": "balancesheet_vip",
        "cashflow": "cashflow_vip",
        "forecast": "forecast_vip",
        "express": "express_vip",
        "fina_indicator": "fina_indicator_vip",
        "fina_mainbz": "fina_mainbz_vip",
    }
    
    # Non-VIP endpoints that have been disabled (require ts_code, cannot query by period alone)
    # non_vip_endpoints = {
    #     "dividend": "dividend",  # Requires ts_code
    #     "fina_audit": "fina_audit",  # Requires ts_code
    # }
    non_vip_endpoints = {}

    def _fetch_and_store(dataset: str, period: str) -> int:
        key = period
        catalog.set_state(dataset=dataset, partition_key=key, status="running")
        with running_lock:
            running.add((dataset, period))
        try:
            logger.debug("finance: fetch start dataset=%s period=%s", dataset, period)
            
            # Determine the API endpoint
            if dataset in vip_endpoints:
                api_name = vip_endpoints[dataset]
            elif dataset in non_vip_endpoints:
                api_name = non_vip_endpoints[dataset]
            else:
                raise ValueError(f"Unknown finance dataset: {dataset}")
            
            # Call the API with period parameter
            df = client.query(api_name, period=period)
            
            if df is None:
                df = pd.DataFrame()
            
            w.write_end_date_partition(dataset, period, df)
            catalog.set_state(dataset=dataset, partition_key=key, status="completed", row_count=int(len(df)))
            logger.debug("finance: fetch done dataset=%s period=%s rows=%d", dataset, period, int(len(df)))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset=dataset, partition_key=key, status="failed", error=str(e))
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning(
                    "finance: fetch failed dataset=%s period=%s (%s)",
                    dataset,
                    period,
                    e,
                )
            else:
                logger.exception("finance: fetch failed dataset=%s period=%s", dataset, period)
            raise
        finally:
            with running_lock:
                running.discard((dataset, period))

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        running: set[tuple[str, str]] = set()
        running_lock = threading.Lock()

        future_to_task = {ex.submit(_fetch_and_store, ds, p): (ds, p) for (ds, p) in tasks}
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
            ptask = progress.add_task("finance", total=len(future_to_task))
            failed: list[tuple[str, str, str]] = []
            
            for f in as_completed(future_to_task):
                ds, p = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"finance (running={running_count} failed={len(failed)}) last={ds} {p} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((ds, p, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"finance (running={running_count} failed={len(failed)}) last=FAILED {ds} {p}",
                    )
                finally:
                    progress.advance(ptask, 1)

            if failed:
                sample = "; ".join([f"{ds}:{p}" for (ds, p, _) in failed[:10]])
                logger.error("finance: %d task(s) failed; sample: %s", len(failed), sample)
                raise RuntimeError(f"finance: {len(failed)} task(s) failed; sample: {sample}")

    logger.info("finance: completed (tasks=%d)", len(tasks))
