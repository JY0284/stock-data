from __future__ import annotations

from bisect import bisect_left
import datetime as _dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import threading
import time

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


def _add_days_yyyymmdd(s: str, days: int) -> str:
    d = parse_yyyymmdd(s)
    return (d + _dt.timedelta(days=days)).strftime("%Y%m%d")


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def _fetch_open_trade_dates(client: TushareClient, start_date: str, end_date: str) -> list[str]:
    df = client.query("trade_cal", exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    if df is None or df.empty:
        return []
    # Ensure str type
    out = df["cal_date"].astype(str).tolist()
    return sorted(out)


def _week_ends(open_dates: list[str]) -> list[str]:
    by_week: dict[tuple[int, int], str] = {}
    for d in open_dates:
        dd = parse_yyyymmdd(d)
        key = (dd.isocalendar().year, dd.isocalendar().week)
        prev = by_week.get(key)
        if prev is None or d > prev:
            by_week[key] = d
    return sorted(by_week.values())


def _month_ends(open_dates: list[str]) -> list[str]:
    by_month: dict[tuple[int, int], str] = {}
    for d in open_dates:
        dd = parse_yyyymmdd(d)
        key = (dd.year, dd.month)
        prev = by_month.get(key)
        if prev is None or d > prev:
            by_month[key] = d
    return sorted(by_month.values())


def _needed_dates_for_dataset(
    *,
    dataset: str,
    open_dates: list[str],
    catalog: DuckDBCatalog,
    start_date: str,
) -> list[str]:
    completed = catalog.completed_partitions(dataset)
    return [d for d in open_dates if d >= start_date and d not in completed]


def _get_index_codes(catalog: DuckDBCatalog) -> list[str]:
    """Get major index codes from index_basic parquet file.
    
    Only returns SSE and SZSE indices (Shanghai and Shenzhen Stock Exchange).
    Other markets (CSI, MSCI, SW, OTH) have too many indices (~10K+) and are rarely needed.
    """
    import os
    index_basic_path = os.path.join(catalog.parquet_root, "index_basic", "latest.parquet")
    if not os.path.exists(index_basic_path):
        raise RuntimeError("index_basic not found. Run basic datasets first.")
    df = pd.read_parquet(index_basic_path)
    # Filter to only major Chinese exchange indices
    major_markets = ["SSE", "SZSE"]
    df = df[df["market"].isin(major_markets)]
    return df["ts_code"].tolist()


def _run_index_daily(
    cfg: RunConfig,
    client: TushareClient,
    catalog: DuckDBCatalog,
    w: ParquetWriter,
) -> None:
    """Run ingestion for index_daily (one file per index containing full history)."""
    import os
    
    # Get index codes
    index_codes = _get_index_codes(catalog)
    logger.info("market: found %d index codes for index_daily", len(index_codes))
    
    # Build tasks: ts_code
    tasks: list[str] = []
    completed = catalog.completed_partitions("index_daily")
    for code in index_codes:
        key = f"ts_code={code.replace('.', '_')}"
        if key not in completed:
            tasks.append(code)
    
    if not tasks:
        logger.info("market: nothing to do for index_daily")
        return
    
    logger.info(
        "market: start index_daily (tasks=%d, workers=%d, rpm=%d)",
        len(tasks),
        cfg.workers,
        cfg.rpm,
    )
    
    running: set[str] = set()
    running_lock = threading.Lock()
    
    def _fetch_and_store(ts_code: str) -> int:
        key = f"ts_code={ts_code.replace('.', '_')}"
        catalog.set_state(dataset="index_daily", partition_key=key, status="running")
        with running_lock:
            running.add(ts_code)
        try:
            logger.debug("market: fetch start index_daily ts_code=%s", ts_code)
            
            # Query all history for this index
            df = client.query("index_daily", ts_code=ts_code)
            
            if df is None:
                df = pd.DataFrame()
            
            w.write_ts_code_partition("index_daily", ts_code, df)
            catalog.set_state(dataset="index_daily", partition_key=key, status="completed", row_count=int(len(df)))
            logger.debug("market: fetch done index_daily ts_code=%s rows=%d", ts_code, int(len(df)))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="index_daily", partition_key=key, status="failed", error=str(e))
            from stock_data.retry import RateLimitError, TransientError
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning("market: fetch failed index_daily ts_code=%s (%s)", ts_code, e)
            else:
                logger.exception("market: fetch failed index_daily ts_code=%s", ts_code)
            raise
        finally:
            with running_lock:
                running.discard(ts_code)
    
    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        future_to_task = {ex.submit(_fetch_and_store, code): code for code in tasks}
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
            ptask = progress.add_task("index_daily", total=len(future_to_task))
            failed: list[tuple[str, str]] = []
            
            for f in as_completed(future_to_task):
                code = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"index_daily (running={running_count} failed={len(failed)}) last={code} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((code, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"index_daily (running={running_count} failed={len(failed)}) last=FAILED {code}",
                    )
                finally:
                    progress.advance(ptask, 1)
            
            if failed:
                sample = "; ".join([code for (code, _) in failed[:10]])
                logger.error("market: %d index_daily task(s) failed; sample: %s", len(failed), sample)
                raise RuntimeError(f"market: {len(failed)} index_daily task(s) failed; sample: {sample}")
    
    logger.info("market: completed index_daily (tasks=%d)", len(tasks))


def run_market(
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

    # Handle index_daily separately (ts_code-based)
    if "index_daily" in datasets:
        _run_index_daily(cfg, client, catalog, w)
        datasets = [d for d in datasets if d != "index_daily"]
        if not datasets:
            return

    # We always need an exchange calendar to generate correct partitions.
    cal_start = start_date or "19900101"
    if start_date is None:
        # In update mode, avoid huge calendar pulls.
        lasts = []
        for ds in datasets:
            last = catalog.last_completed_partition(ds)
            if last:
                lasts.append(last)
        if lasts:
            cal_start = min(lasts)
        else:
            # No local history yet; keep the calendar bounded for "update".
            cal_start = _add_days_yyyymmdd(end_date, -120)

    # For weekly/monthly scheduling, we may need a short look-ahead window to
    # determine true period ends (to avoid scheduling incomplete periods).
    cal_end = end_date
    if "weekly" in datasets or "monthly" in datasets:
        lookahead = 45 if "monthly" in datasets else 10
        cal_end = _add_days_yyyymmdd(end_date, lookahead)

    open_dates_all = _fetch_open_trade_dates(client, cal_start, cal_end)
    open_dates = [d for d in open_dates_all if d <= end_date]
    if not open_dates:
        logger.info("market: no open dates in [%s, %s]", cal_start, end_date)
        return
    effective_end = open_dates[-1]

    # Decide per-dataset date lists.
    date_map: dict[str, list[str]] = {}

    # Daily-ish datasets use every open trade date.
    dailyish = {"daily", "adj_factor", "daily_basic", "stk_limit", "suspend_d", "etf_daily"}
    must_nonempty = {"daily", "adj_factor", "daily_basic", "weekly", "monthly", "etf_daily"}
    if start_date is None:
        # incremental: start from the last completed partition per-dataset
        for ds in datasets:
            if ds not in dailyish:
                continue
            min_rows = 1 if ds in must_nonempty else None
            last = catalog.last_completed_partition(ds, min_row_count=min_rows)
            completed = catalog.completed_partitions(ds, min_row_count=min_rows)
            if not last:
                candidates = [effective_end]
            else:
                start_idx = bisect_left(open_dates, last)
                if start_idx >= len(open_dates):
                    start_idx = max(0, len(open_dates) - 1)
                candidates = open_dates[start_idx:]
            date_map[ds] = [d for d in candidates if d not in completed]
    else:
        for ds in datasets:
            if ds in dailyish:
                min_rows = 1 if ds in must_nonempty else None
                completed = catalog.completed_partitions(ds, min_row_count=min_rows)
                date_map[ds] = [d for d in open_dates if start_date <= d <= effective_end and d not in completed]

    # Weekly/monthly are only needed at week/month ends.
    if "weekly" in datasets:
        # Determine true week ends using the (possibly) extended calendar, then
        # keep only those <= effective_end.
        week_ends = [d for d in _week_ends(open_dates_all) if d <= effective_end]
        if start_date is None:
            last = catalog.last_completed_partition("weekly", min_row_count=1)
            if last:
                start_idx = bisect_left(week_ends, last)
                if start_idx >= len(week_ends):
                    start_idx = max(0, len(week_ends) - 1)
                week_ends = week_ends[start_idx:]
            else:
                week_ends = [week_ends[-1]] if week_ends else []
        else:
            week_ends = [d for d in week_ends if start_date <= d <= effective_end]
        completed = catalog.completed_partitions("weekly", min_row_count=1)
        date_map["weekly"] = [d for d in week_ends if d not in completed]

    if "monthly" in datasets:
        # Determine true month ends using the (possibly) extended calendar, then
        # keep only those <= effective_end.
        month_ends = [d for d in _month_ends(open_dates_all) if d <= effective_end]
        if start_date is None:
            last = catalog.last_completed_partition("monthly", min_row_count=1)
            if last:
                start_idx = bisect_left(month_ends, last)
                if start_idx >= len(month_ends):
                    start_idx = max(0, len(month_ends) - 1)
                month_ends = month_ends[start_idx:]
            else:
                month_ends = [month_ends[-1]] if month_ends else []
        else:
            month_ends = [d for d in month_ends if start_date <= d <= effective_end]
        completed = catalog.completed_partitions("monthly", min_row_count=1)
        date_map["monthly"] = [d for d in month_ends if d not in completed]

    # Build tasks.
    tasks: list[tuple[str, str]] = []
    for ds, dates in date_map.items():
        for d in dates:
            tasks.append((ds, d))

    # Sanity check: in backfill mode, daily-ish datasets should cover every open trade date.
    if start_date is not None:
        dailyish = {"daily", "adj_factor", "daily_basic", "stk_limit", "suspend_d"}
        for ds in datasets:
            if ds not in dailyish:
                continue
            expected_all = [d for d in open_dates if start_date <= d <= effective_end]
            min_rows = 1 if ds in must_nonempty else None
            completed = catalog.completed_partitions(ds, min_row_count=min_rows)
            expected = [d for d in expected_all if d not in completed]
            actual = date_map.get(ds, [])
            if set(actual) != set(expected):
                logger.warning(
                    "market: scheduling mismatch ds=%s expected=%d actual=%d (expected_range=%s..%s actual_range=%s..%s)",
                    ds,
                    len(expected),
                    len(actual),
                    expected[0] if expected else "-",
                    expected[-1] if expected else "-",
                    actual[0] if actual else "-",
                    actual[-1] if actual else "-",
                )

    if not tasks:
        logger.info(
            "market: nothing to do (datasets=%s, start=%s, end=%s)",
            ",".join(datasets),
            start_date,
            effective_end,
        )
        return

    logger.info(
        "market: start (datasets=%s, tasks=%d, workers=%d, rpm=%d, start=%s, end=%s)",
        ",".join(sorted(set(datasets))),
        len(tasks),
        cfg.workers,
        cfg.rpm,
        start_date,
        effective_end,
    )

    def _fetch_and_store(dataset: str, trade_date: str) -> int:
        key = trade_date
        catalog.set_state(dataset=dataset, partition_key=key, status="running")
        with running_lock:
            running.add((dataset, trade_date))
        try:
            logger.debug("market: fetch start dataset=%s trade_date=%s", dataset, trade_date)
            if dataset == "daily":
                df = client.query("daily", trade_date=trade_date)
            elif dataset == "adj_factor":
                df = client.query("adj_factor", trade_date=trade_date)
            elif dataset == "daily_basic":
                df = client.query("daily_basic", trade_date=trade_date)
            elif dataset == "stk_limit":
                df = client.query("stk_limit", trade_date=trade_date)
            elif dataset == "suspend_d":
                df_s = client.query("suspend_d", suspend_type="S", trade_date=trade_date)
                df_r = client.query("suspend_d", suspend_type="R", trade_date=trade_date)
                df = pd.concat([df_s, df_r], ignore_index=True)
            # index_daily is not supported - requires ts_code parameter, cannot query by trade_date alone
            elif dataset == "etf_daily":
                # Tushare does not provide a separate "etf_daily" endpoint in `pro.query`.
                # The commonly used daily bars endpoint for exchange-traded funds is `fund_daily`.
                df = client.query("fund_daily", trade_date=trade_date)
            elif dataset == "weekly":
                df = client.query("weekly", trade_date=trade_date)
            elif dataset == "monthly":
                df = client.query("monthly", trade_date=trade_date)
            else:
                raise ValueError(f"Unknown dataset: {dataset}")

            if df is None:
                df = pd.DataFrame()
            w.write_trade_date_partition(dataset, trade_date, df)
            catalog.set_state(dataset=dataset, partition_key=key, status="completed", row_count=int(len(df)))
            logger.debug("market: fetch done dataset=%s trade_date=%s rows=%d", dataset, trade_date, int(len(df)))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset=dataset, partition_key=key, status="failed", error=str(e))
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning(
                    "market: fetch failed dataset=%s trade_date=%s (%s)",
                    dataset,
                    trade_date,
                    e,
                )
            else:
                logger.exception("market: fetch failed dataset=%s trade_date=%s", dataset, trade_date)
            raise
        finally:
            with running_lock:
                running.discard((dataset, trade_date))

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        running: set[tuple[str, str]] = set()
        running_lock = threading.Lock()

        future_to_task = {ex.submit(_fetch_and_store, ds, d): (ds, d) for (ds, d) in tasks}
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
            ptask = progress.add_task("market", total=len(future_to_task))
            failed: list[tuple[str, str, str]] = []
            # Exhaust to surface exceptions early.
            for f in as_completed(future_to_task):
                ds, d = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"market (running={running_count} failed={len(failed)}) last={ds} {d} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((ds, d, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"market (running={running_count} failed={len(failed)}) last=FAILED {ds} {d}",
                    )
                finally:
                    progress.advance(ptask, 1)

            if failed:
                remaining = [(ds, d) for (ds, d, _) in failed]

                # Best-effort extra rounds for a small number of flaky partitions.
                # This is especially helpful when Tushare returns occasional empty frames
                # under load/throttling but would succeed later.
                for round_idx in range(1, 4):
                    if not remaining:
                        break
                    logger.warning(
                        "market: retry round %d for %d failed task(s)",
                        round_idx,
                        len(remaining),
                    )
                    # Give upstream time to recover and let token window reset.
                    time.sleep(10.0 if round_idx == 1 else 65.0)

                    next_failed: list[tuple[str, str, str]] = []
                    with ThreadPoolExecutor(max_workers=min(2, cfg.workers)) as ex2:
                        future_to_task2 = {ex2.submit(_fetch_and_store, ds, d): (ds, d) for (ds, d) in remaining}
                        for f2 in as_completed(future_to_task2):
                            ds2, d2 = future_to_task2[f2]
                            try:
                                f2.result()
                            except Exception as e:  # noqa: BLE001
                                next_failed.append((ds2, d2, str(e)))

                    remaining = [(ds, d) for (ds, d, _) in next_failed]

                if remaining:
                    sample = "; ".join([f"{ds}:{d}" for (ds, d) in remaining[:10]])
                    logger.error("market: %d task(s) failed; sample: %s", len(remaining), sample)
                    raise RuntimeError(f"market: {len(remaining)} task(s) failed; sample: {sample}")

    logger.info("market: completed (tasks=%d)", len(tasks))
