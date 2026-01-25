from __future__ import annotations

import logging
import os
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


logger = logging.getLogger(__name__)


ETF_DATASETS = {"fund_nav", "fund_share", "fund_div"}


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def _get_etf_codes(catalog: DuckDBCatalog) -> list[str]:
    """Get all ETF codes from fund_basic parquet file."""
    fund_basic_path = os.path.join(catalog.parquet_root, "fund_basic", "latest.parquet")
    if not os.path.exists(fund_basic_path):
        raise RuntimeError("fund_basic not found. Run basic datasets first (include fund_basic).")
    df = pd.read_parquet(fund_basic_path)
    return df["ts_code"].tolist()


def run_etf(
    cfg: RunConfig,
    *,
    token: str,
    catalog: DuckDBCatalog,
    datasets: list[str],
) -> None:
    """Run ETF data ingestion for ts_code-based datasets.
    
    ETF datasets (fund_nav, fund_share, fund_div) are partitioned by ts_code.
    Each file contains the full history for that ETF.
    """
    if not datasets:
        return

    limiter = RateLimiter(rpm=cfg.rpm)
    client = TushareClient(token=token, limiter=limiter)
    w = _writer(cfg)

    # Get ETF codes
    etf_codes = _get_etf_codes(catalog)
    logger.info("etf: found %d ETF codes", len(etf_codes))

    # Build tasks: (dataset, ts_code)
    tasks: list[tuple[str, str]] = []
    for ds in datasets:
        if ds not in ETF_DATASETS:
            continue
        completed = catalog.completed_partitions(ds)
        for code in etf_codes:
            key = f"ts_code={code.replace('.', '_')}"
            if key not in completed:
                tasks.append((ds, code))

    if not tasks:
        logger.info("etf: nothing to do (datasets=%s)", ",".join(datasets))
        return

    logger.info(
        "etf: start (datasets=%s, tasks=%d, workers=%d, rpm=%d)",
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
            logger.debug("etf: fetch start dataset=%s ts_code=%s", dataset, ts_code)

            # Query by ts_code
            df = client.query(dataset, ts_code=ts_code)

            if df is None:
                df = pd.DataFrame()

            w.write_ts_code_partition(dataset, ts_code, df)
            catalog.set_state(dataset=dataset, partition_key=key, status="completed", row_count=int(len(df)))
            logger.debug("etf: fetch done dataset=%s ts_code=%s rows=%d", dataset, ts_code, int(len(df)))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset=dataset, partition_key=key, status="failed", error=str(e))
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning("etf: fetch failed dataset=%s ts_code=%s (%s)", dataset, ts_code, e)
            else:
                logger.exception("etf: fetch failed dataset=%s ts_code=%s", dataset, ts_code)
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
            ptask = progress.add_task("etf", total=len(future_to_task))
            failed: list[tuple[str, str, str]] = []

            for f in as_completed(future_to_task):
                ds, code = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"etf (running={running_count} failed={len(failed)}) last={ds} {code} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((ds, code, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"etf (running={running_count} failed={len(failed)}) last=FAILED {ds} {code}",
                    )
                finally:
                    progress.advance(ptask, 1)

            if failed:
                sample = "; ".join([f"{ds}:{code}" for (ds, code, _) in failed[:10]])
                logger.error("etf: %d task(s) failed; sample: %s", len(failed), sample)
                raise RuntimeError(f"etf: {len(failed)} task(s) failed; sample: {sample}")

    logger.info("etf: completed (tasks=%d)", len(tasks))
