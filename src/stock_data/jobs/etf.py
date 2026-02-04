from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as _dt
import glob as _glob

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


ETF_DATASETS = {"fund_nav", "fund_share", "fund_div"}


def _add_days_yyyymmdd(s: str, days: int) -> str:
    d = parse_yyyymmdd(s)
    return (d + _dt.timedelta(days=days)).strftime("%Y%m%d")


def _effective_end_date(client: TushareClient, end_date: str) -> str:
    """Resolve an 'effective' end date as the latest open trade day <= end_date.

    This avoids scheduling ts_code datasets against a non-open day and lets us
    compare local max dates against a realistic market calendar.
    """
    cal_start = _add_days_yyyymmdd(end_date, -90)
    df = client.query("trade_cal", exchange="SSE", start_date=cal_start, end_date=end_date, is_open="1")
    if df is None or getattr(df, "empty", False):
        return end_date
    try:
        dates = sorted(df["cal_date"].astype(str).tolist())
        return dates[-1] if dates else end_date
    except Exception:
        return end_date


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
    start_date: str | None,
    end_date: str | None,
    refresh_days: int | None = None,
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

    effective_end: str | None = None
    if end_date is not None:
        try:
            effective_end = _effective_end_date(client, str(end_date))
        except Exception:
            effective_end = str(end_date)

    # Get ETF codes
    etf_codes = _get_etf_codes(catalog)
    logger.info("etf: found %d ETF codes", len(etf_codes))

    refresh_seconds: int | None = None
    if refresh_days is not None:
        days = int(refresh_days)
        refresh_seconds = (days * 24 * 60 * 60) if days > 0 else None

    def _date_col_for_dataset(dataset: str) -> str | None:
        if dataset == "fund_nav":
            return "nav_date"
        if dataset == "fund_share":
            return "trade_date"
        return None

    def _max_date_by_code(dataset: str, date_col: str) -> dict[str, str]:
        g = catalog.parquet_glob(dataset)
        if not _glob.glob(g, recursive=True):
            return {}
        try:
            with catalog.connect() as con:
                rows = con.execute(
                    f"SELECT ts_code, MAX({date_col}) AS mx FROM read_parquet('{g}', union_by_name=true) GROUP BY ts_code;"
                ).fetchall()
            out: dict[str, str] = {}
            for ts, mx in rows:
                if ts is None or mx is None:
                    continue
                s = str(mx)
                if s.strip():
                    out[str(ts)] = s
            return out
        except Exception:
            logger.exception("etf: failed to compute per-code max(%s) for %s", date_col, dataset)
            return {}

    def _file_exists_for_code(dataset: str, ts_code: str) -> bool:
        safe = ts_code.replace(".", "_")
        path = os.path.join(catalog.parquet_root, dataset, f"ts_code={safe}.parquet")
        return os.path.exists(path)

    def _recently_attempted_keys(dataset: str, seconds: int | None) -> set[str]:
        if seconds is None:
            return set()
        try:
            with catalog.connect() as con:
                rows = con.execute(
                    """
                    SELECT partition_key
                    FROM ingestion_state
                    WHERE dataset = ?
                      AND status IN ('completed', 'skipped')
                      AND updated_at >= (NOW() - (? * INTERVAL '1 second'));
                    """,
                    [dataset, int(seconds)],
                ).fetchall()
            return {str(r[0]) for r in rows}
        except Exception:
            return set()

    # Build tasks: correctness-first.
    # 1) Always include codes missing locally or whose max(date) < effective_end.
    # 2) Optionally include already-caught-up codes for periodic force-refresh (upstream revisions).
    # 3) Back off per-code when upstream is empty/stale to avoid thrashing.
    tasks: list[tuple[str, str]] = []
    for ds in datasets:
        if ds not in ETF_DATASETS:
            continue

        date_col = _date_col_for_dataset(ds)
        max_by_code = _max_date_by_code(ds, date_col) if (date_col and effective_end) else {}
        completed_recent = catalog.completed_partitions(ds, newer_than_seconds=refresh_seconds) if refresh_seconds else set()

        stall_retry_seconds = 2 * 60 * 60
        try:
            v = os.environ.get("STOCK_DATA_ETF_STALL_RETRY_SECONDS")
            if v is not None and str(v).strip() != "":
                stall_retry_seconds = max(0, int(v))
        except Exception:
            stall_retry_seconds = 2 * 60 * 60

        attempted_recent = _recently_attempted_keys(ds, stall_retry_seconds if stall_retry_seconds > 0 else None)

        for code in etf_codes:
            key = f"ts_code={code.replace('.', '_')}"
            exists = _file_exists_for_code(ds, code)

            if date_col is None:
                # fund_div: no reliable incremental time key; only run if missing locally or force-refresh is requested.
                force_refresh = (refresh_seconds is not None) and (key not in completed_recent)
                if (not exists or force_refresh) and (key not in attempted_recent):
                    tasks.append((ds, code))
                continue

            local_max = max_by_code.get(code) if exists else None
            behind = (effective_end is not None) and (local_max is None or local_max < str(effective_end))
            force_refresh = (refresh_seconds is not None) and (key not in completed_recent)

            if (behind or force_refresh) and (key not in attempted_recent):
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

    def _existing_ts_code_df(dataset: str, ts_code: str) -> pd.DataFrame | None:
        safe = ts_code.replace(".", "_")
        path = os.path.join(catalog.parquet_root, dataset, f"ts_code={safe}.parquet")
        if not os.path.exists(path):
            return None
        try:
            df = pd.read_parquet(path)
            return df if isinstance(df, pd.DataFrame) else None
        except Exception:
            return None

    def _merge_ts_code(existing: pd.DataFrame | None, new: pd.DataFrame, *, key_col: str) -> pd.DataFrame:
        if existing is None or existing.empty:
            merged = new
        else:
            merged = pd.concat([existing, new], ignore_index=True)
        if key_col in merged.columns:
            subset = [key_col]
            if "ts_code" in merged.columns:
                subset = ["ts_code", key_col]
            merged[key_col] = merged[key_col].astype(str)
            merged = merged.sort_values(key_col, kind="stable")
            merged = merged.drop_duplicates(subset=subset, keep="last")
            merged = merged.reset_index(drop=True)
        return merged

    def _incremental_date_col(dataset: str) -> str | None:
        # Only enable incremental mode where Tushare supports start_date/end_date
        # and the local schema has a clear time key.
        return _date_col_for_dataset(dataset)

    def _fetch_and_store(dataset: str, ts_code: str) -> int:
        key = f"ts_code={ts_code.replace('.', '_')}"
        catalog.set_state(dataset=dataset, partition_key=key, status="running")
        with running_lock:
            running.add((dataset, ts_code))
        try:
            logger.debug("etf: fetch start dataset=%s ts_code=%s", dataset, ts_code)

            date_col = _incremental_date_col(dataset)
            existing = _existing_ts_code_df(dataset, ts_code) if (date_col and start_date is None) else None
            eff_start = start_date
            if eff_start is None and existing is not None and not existing.empty and date_col in existing.columns:
                try:
                    eff_start = str(existing[date_col].astype(str).max())
                except Exception:
                    eff_start = None

            # Prefer incremental query when possible; fall back to full-history.
            if date_col and eff_start is not None:
                params = {"ts_code": ts_code, "start_date": str(eff_start)}
                if effective_end is not None:
                    params["end_date"] = str(effective_end)
                df_new = client.query(dataset, **params)
            else:
                df_new = client.query(dataset, ts_code=ts_code)

            if df_new is None:
                df_new = pd.DataFrame()

            # If upstream returns empty, do not overwrite an existing parquet and do not
            # mark as completed. Empty responses can happen due to throttling or upstream
            # publish delays; allow later retries.
            if getattr(df_new, "empty", False):
                catalog.set_state(
                    dataset=dataset,
                    partition_key=key,
                    status="skipped",
                    row_count=0,
                    error=f"empty response for {dataset}",
                )
                logger.warning("etf: skipped empty response dataset=%s ts_code=%s", dataset, ts_code)
                return 0

            if existing is not None and date_col and not df_new.empty:
                df_out = _merge_ts_code(existing, df_new, key_col=date_col)
            else:
                df_out = df_new

            w.write_ts_code_partition(dataset, ts_code, df_out)
            catalog.set_state(dataset=dataset, partition_key=key, status="completed", row_count=int(len(df_out)))
            logger.debug("etf: fetch done dataset=%s ts_code=%s rows=%d", dataset, ts_code, int(len(df_out)))
            return int(len(df_out))
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
