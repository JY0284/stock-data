from __future__ import annotations

from bisect import bisect_left
import datetime as _dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
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
    codes = df["ts_code"].dropna().astype(str).tolist()
    # Defensive: index_basic can contain duplicates; avoid double-fetching.
    return sorted(set(codes))


def _run_index_daily(
    cfg: RunConfig,
    client: TushareClient,
    catalog: DuckDBCatalog,
    w: ParquetWriter,
    *,
    end_date: str,
    refresh_days: int | None = None,
) -> None:
    """Run ingestion for index_daily (one file per index containing full history)."""
    import os

    # Determine the latest open trading day up to end_date. This is used to
    # detect stalls where we "refreshed" recently but parquet data is behind.
    cal_start = _add_days_yyyymmdd(end_date, -90)
    open_dates = _fetch_open_trade_dates(client, cal_start, end_date)
    effective_end = open_dates[-1] if open_dates else end_date

    def _key(ts_code: str) -> str:
        return f"ts_code={ts_code.replace('.', '_')}"

    def _recently_attempted_keys(seconds: int | None) -> set[str]:
        if seconds is None:
            return set()
        try:
            with catalog.connect() as con:
                rows = con.execute(
                    """
                    SELECT partition_key
                    FROM ingestion_state
                    WHERE dataset = 'index_daily'
                      AND status IN ('completed', 'skipped')
                      AND updated_at >= (NOW() - (? * INTERVAL '1 second'));
                    """,
                    [int(seconds)],
                ).fetchall()
            return {str(r[0]) for r in rows}
        except Exception:
            return set()

    def _index_daily_max_trade_date() -> str | None:
        g = catalog.parquet_glob("index_daily")
        if not glob.glob(g, recursive=True):
            return None
        try:
            with catalog.connect() as con:
                row = con.execute(
                    f"SELECT MAX(trade_date) FROM read_parquet('{g}', union_by_name=true);"
                ).fetchone()
            if not row:
                return None
            v = row[0]
            return str(v) if v is not None and str(v).strip() else None
        except Exception:
            logger.exception("market: failed to compute index_daily max(trade_date)")
            return None

    # Get index codes
    index_codes = _get_index_codes(catalog)
    logger.info("market: found %d index codes for index_daily", len(index_codes))
    
    # Build tasks: correctness-first.
    # 1) Always include codes that are missing locally or whose max(trade_date) < effective_end.
    # 2) Optionally include already-caught-up codes for periodic force-refresh based on recency.
    tasks: list[str] = []

    refresh_seconds: int | None = None
    if refresh_days is not None:
        days = int(refresh_days)
        refresh_seconds = (days * 24 * 60 * 60) if days > 0 else None

    max_by_code: dict[str, str] = {}
    g = catalog.parquet_glob("index_daily")
    if glob.glob(g, recursive=True):
        try:
            with catalog.connect() as con:
                rows = con.execute(
                    f"SELECT ts_code, MAX(trade_date) AS max_td FROM read_parquet('{g}', union_by_name=true) GROUP BY ts_code;"
                ).fetchall()
            for ts, td in rows:
                if ts is None or td is None:
                    continue
                s = str(td)
                if s.strip():
                    max_by_code[str(ts)] = s
        except Exception:
            logger.exception("market: failed to compute per-code max(trade_date) for index_daily")
            max_by_code = {}

    # Avoid thrash when upstream hasn't published the latest day yet.
    stall_retry_seconds = 2 * 60 * 60
    try:
        v = os.environ.get("STOCK_DATA_INDEX_DAILY_STALL_RETRY_SECONDS")
        if v is not None and str(v).strip() != "":
            stall_retry_seconds = max(0, int(v))
    except Exception:
        stall_retry_seconds = 2 * 60 * 60

    attempted_recent = _recently_attempted_keys(stall_retry_seconds if stall_retry_seconds > 0 else None)
    completed_recent = _recently_attempted_keys(refresh_seconds)

    for code in index_codes:
        key = _key(code)
        local_max = max_by_code.get(code)

        behind = (local_max is None) or (local_max < effective_end)
        force_refresh = (refresh_seconds is not None) and (key not in completed_recent)

        if behind:
            # Ensure we catch up to the latest open day.
            if key not in attempted_recent:
                tasks.append(code)
        elif force_refresh:
            # Optional periodic refresh even when already caught up.
            if key not in attempted_recent:
                tasks.append(code)
    
    if not tasks:
        logger.info("market: nothing to do for index_daily (effective_end=%s)", effective_end)
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
        key = _key(ts_code)
        catalog.set_state(dataset="index_daily", partition_key=key, status="running")
        with running_lock:
            running.add(ts_code)
        try:
            logger.debug("market: fetch start index_daily ts_code=%s", ts_code)

            # Prefer incremental query when local parquet already has history.
            existing = None
            existing_max: str | None = None
            try:
                safe = ts_code.replace(".", "_")
                path = os.path.join(catalog.parquet_root, "index_daily", f"ts_code={safe}.parquet")
                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    if existing is not None and not existing.empty and "trade_date" in existing.columns:
                        existing_max = str(existing["trade_date"].astype(str).max())
            except Exception:
                existing = None
                existing_max = None

            if existing_max:
                df = client.query("index_daily", ts_code=ts_code, start_date=str(existing_max), end_date=str(effective_end))
            else:
                # Query all history for this index (first-time ingestion)
                df = client.query("index_daily", ts_code=ts_code)
            
            if df is None:
                df = pd.DataFrame()

            # If upstream returns empty, don't overwrite an existing parquet and don't
            # mark as completed. Empty responses are common with throttling / transient
            # upstream issues.
            if getattr(df, "empty", False):
                catalog.set_state(
                    dataset="index_daily",
                    partition_key=key,
                    status="skipped",
                    row_count=0,
                    error="empty response for index_daily",
                )
                logger.warning("market: skipped empty index_daily ts_code=%s", ts_code)
                return 0
            
            # Detect stale responses: if upstream returns data that doesn't reach the
            # latest open trading day, do not overwrite local parquet and do not mark
            # as completed (so a later update can retry).
            try:
                if not df.empty and "trade_date" in df.columns:
                    resp_max = str(df["trade_date"].astype(str).max())
                    if resp_max and resp_max < effective_end:
                        catalog.set_state(
                            dataset="index_daily",
                            partition_key=key,
                            status="skipped",
                            row_count=0,
                            error=f"stale index_daily response max_trade_date={resp_max} < effective_end={effective_end}",
                        )
                        logger.warning(
                            "market: skipped stale index_daily ts_code=%s (max_trade_date=%s effective_end=%s)",
                            ts_code,
                            resp_max,
                            effective_end,
                        )
                        return 0
            except Exception:
                # If schema is unexpected, proceed to write anyway.
                pass

            # Merge incremental tail into existing history.
            df_out = df
            if existing is not None and existing_max and not getattr(df, "empty", False) and "trade_date" in df.columns:
                try:
                    merged = pd.concat([existing, df], ignore_index=True)
                    merged["trade_date"] = merged["trade_date"].astype(str)
                    subset = ["trade_date"]
                    if "ts_code" in merged.columns:
                        subset = ["ts_code", "trade_date"]
                    merged = merged.sort_values("trade_date", kind="stable")
                    merged = merged.drop_duplicates(subset=subset, keep="last")
                    df_out = merged.reset_index(drop=True)
                except Exception:
                    df_out = df

            w.write_ts_code_partition("index_daily", ts_code, df_out)
            catalog.set_state(dataset="index_daily", partition_key=key, status="completed", row_count=int(len(df_out)))
            logger.debug("market: fetch done index_daily ts_code=%s rows=%d", ts_code, int(len(df_out)))
            return int(len(df_out))
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


def _run_fx_daily(
    cfg: RunConfig,
    client: TushareClient,
    catalog: DuckDBCatalog,
    w: ParquetWriter,
    *,
    start_date: str | None,
    end_date: str,
) -> None:
    """Run ingestion for fx_daily (trade_date-partitioned).

    Doc: https://tushare.pro/document/2?doc_id=179

    This is intentionally implemented like other trade_date-partitioned datasets:
    - no extra symbol-list dataset required
    - no required env vars

    We schedule weekdays only (Mon-Fri) because FX daily data is typically 24/5.
    trade_date is documented as GMT date (often one day behind Beijing).

    Optional env var: STOCK_DATA_FX_EXCHANGE (default "FXCM").
    """
    import os

    exchange = os.environ.get("STOCK_DATA_FX_EXCHANGE", "FXCM").strip() or "FXCM"

    # If upstream ends up requiring ts_code, fall back to a small built-in
    # universe so `update/backfill` works out of the box (no env-var gating).
    # Kept intentionally small to control cost.
    default_ts_codes_fxcm = [
        "USDCNH.FXCM",
        "EURUSD.FXCM",
        "USDJPY.FXCM",
        "GBPUSD.FXCM",
    ]

    def _weekday_dates(start_yyyymmdd: str, end_yyyymmdd: str) -> list[str]:
        start_d = parse_yyyymmdd(start_yyyymmdd)
        end_d = parse_yyyymmdd(end_yyyymmdd)
        out: list[str] = []
        cur = start_d
        while cur <= end_d:
            # 0=Mon ... 6=Sun
            if cur.weekday() < 5:
                out.append(cur.strftime("%Y%m%d"))
            cur = cur + _dt.timedelta(days=1)
        return out

    # In update mode, avoid pulling huge windows.
    cal_start = start_date or "19900101"
    if start_date is None:
        last = catalog.last_completed_partition("fx_daily")
        if last:
            cal_start = last
        else:
            cal_start = _add_days_yyyymmdd(end_date, -120)

    dates_all = _weekday_dates(cal_start, end_date)
    if start_date is None and dates_all:
        # Include last day again to make update idempotent.
        dates_all = dates_all[-120:] if len(dates_all) > 120 else dates_all

    completed = catalog.completed_partitions("fx_daily")
    dates = [d for d in dates_all if d not in completed]
    if not dates:
        logger.info("market: nothing to do for fx_daily (start=%s end=%s)", cal_start, end_date)
        return

    logger.info(
        "market: start fx_daily (tasks=%d, workers=%d, rpm=%d, start=%s, end=%s, exchange=%s)",
        len(dates),
        cfg.workers,
        cfg.rpm,
        cal_start,
        end_date,
        exchange,
    )

    running: set[str] = set()
    running_lock = threading.Lock()

    def _fetch_and_store(trade_date: str) -> int:
        key = trade_date
        catalog.set_state(dataset="fx_daily", partition_key=key, status="running")
        with running_lock:
            running.add(trade_date)
        try:
            try:
                df = client.query("fx_daily", trade_date=trade_date, exchange=exchange)
            except Exception as e:  # noqa: BLE001
                msg = str(e).lower()
                needs_ts_code = ("ts_code" in msg) and ("required" in msg or "missing" in msg)
                if needs_ts_code and exchange.upper() == "FXCM":
                    parts: list[pd.DataFrame] = []
                    for ts_code in default_ts_codes_fxcm:
                        try:
                            dfi = client.query(
                                "fx_daily",
                                ts_code=ts_code,
                                trade_date=trade_date,
                                exchange=exchange,
                            )
                            if dfi is not None and not getattr(dfi, "empty", False):
                                parts.append(dfi)
                        except Exception:
                            # Keep going; we don't want one pair to block the partition.
                            logger.debug("market: fx_daily fallback failed ts_code=%s trade_date=%s", ts_code, trade_date)
                    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
                else:
                    raise
            if df is None:
                df = pd.DataFrame()

            # Same "today might be empty" behavior as other market datasets.
            today = _dt.date.today().strftime("%Y%m%d")
            if trade_date == today and (getattr(df, "empty", False) or len(getattr(df, "columns", [])) == 0):
                catalog.set_state(
                    dataset="fx_daily",
                    partition_key=key,
                    status="skipped",
                    row_count=0,
                    error="empty response for today; likely not published yet",
                )
                logger.info("market: skipped empty today dataset=fx_daily trade_date=%s", trade_date)
                return 0

            w.write_trade_date_partition("fx_daily", trade_date, df)
            catalog.set_state(dataset="fx_daily", partition_key=key, status="completed", row_count=int(len(df)))
            return int(len(df))
        except Exception as e:  # noqa: BLE001
            catalog.set_state(dataset="fx_daily", partition_key=key, status="failed", error=str(e))
            if isinstance(e, (TransientError, RateLimitError)):
                logger.warning("market: fetch failed fx_daily trade_date=%s (%s)", trade_date, e)
            else:
                logger.exception("market: fetch failed fx_daily trade_date=%s", trade_date)
            raise
        finally:
            with running_lock:
                running.discard(trade_date)

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        future_to_task = {ex.submit(_fetch_and_store, d): d for d in dates}
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
            ptask = progress.add_task("fx_daily", total=len(future_to_task))
            failed: list[tuple[str, str]] = []

            for f in as_completed(future_to_task):
                d = future_to_task[f]
                try:
                    rows = f.result()
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"fx_daily (running={running_count} failed={len(failed)}) last={d} rows={rows}",
                    )
                except Exception as e:  # noqa: BLE001
                    failed.append((d, str(e)))
                    with running_lock:
                        running_count = len(running)
                    progress.update(
                        ptask,
                        description=f"fx_daily (running={running_count} failed={len(failed)}) last=FAILED {d}",
                    )
                finally:
                    progress.advance(ptask, 1)

            if failed:
                sample = "; ".join([d for (d, _) in failed[:10]])
                logger.error("market: %d fx_daily task(s) failed; sample: %s", len(failed), sample)
                raise RuntimeError(f"market: {len(failed)} fx_daily task(s) failed; sample: {sample}")

    logger.info("market: completed fx_daily (tasks=%d)", len(dates))


def run_market(
    cfg: RunConfig,
    *,
    token: str,
    catalog: DuckDBCatalog,
    datasets: list[str],
    start_date: str | None,
    end_date: str,
    index_daily_refresh_days: int | None = None,
) -> None:
    if not datasets:
        return

    limiter = RateLimiter(rpm=cfg.rpm)
    client = TushareClient(token=token, limiter=limiter)
    w = _writer(cfg)

    # Handle index_daily separately (ts_code-based)
    if "index_daily" in datasets:
        _run_index_daily(cfg, client, catalog, w, end_date=end_date, refresh_days=index_daily_refresh_days)
        datasets = [d for d in datasets if d != "index_daily"]
        if not datasets:
            return

    # Handle fx_daily separately (FX weekday calendar, not SSE trade_cal).
    if "fx_daily" in datasets:
        _run_fx_daily(cfg, client, catalog, w, start_date=start_date, end_date=end_date)
        datasets = [d for d in datasets if d != "fx_daily"]
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
    dailyish = {"daily", "adj_factor", "daily_basic", "stk_limit", "suspend_d", "etf_daily", "moneyflow"}
    # Note: etf_daily is NOT in must_nonempty because ETFs didn't exist before 2004
    must_nonempty = {"daily", "adj_factor", "daily_basic", "moneyflow", "weekly", "monthly"}

    def _min_row_count_for(ds: str) -> int | None:
        # In incremental update mode, treat 0-row partitions as incomplete for etf_daily.
        # This prevents empty files (often caused by throttling/partial failures) from
        # blocking future updates.
        if ds in must_nonempty:
            return 1
        if start_date is None and ds == "etf_daily":
            return 1
        return None

    if start_date is None:
        # incremental: start from the last completed partition per-dataset
        for ds in datasets:
            if ds not in dailyish:
                continue
            min_rows = _min_row_count_for(ds)
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
                min_rows = _min_row_count_for(ds)
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
        dailyish = {"daily", "adj_factor", "daily_basic", "stk_limit", "suspend_d", "moneyflow"}
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
            elif dataset == "moneyflow":
                df = client.query("moneyflow", trade_date=trade_date)
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

            # If we're querying for *today* and the upstream hasn't published the data yet,
            # Tushare often returns an empty frame. Treat this as a normal skip:
            # - no retries (don't fail the whole run)
            # - don't write empty parquet partitions
            # - don't mark as completed (so a later update can pick it up)
            today = _dt.date.today().strftime("%Y%m%d")
            if trade_date == today and (getattr(df, "empty", False) or len(getattr(df, "columns", [])) == 0):
                catalog.set_state(
                    dataset=dataset,
                    partition_key=key,
                    status="skipped",
                    row_count=0,
                    error="empty response for today; likely not published yet",
                )
                logger.info("market: skipped empty today dataset=%s trade_date=%s", dataset, trade_date)
                return 0

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
