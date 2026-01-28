from __future__ import annotations

import contextlib
import glob
import logging
import os
from dataclasses import dataclass

from stock_data.jobs.derived import ensure_derived_views
from stock_data.rate_limit import RateLimiter
from stock_data.retry import RateLimitError, TransientError
from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.tushare_client import TushareClient


logger = logging.getLogger(__name__)


def _has_any_parquet(glob_path: str) -> bool:
    return bool(glob.glob(glob_path, recursive=True))


def _is_duckdb_lock_error(e: Exception) -> bool:
    msg = str(e)
    return "Could not set lock on file" in msg or "Conflicting lock" in msg


def _table_exists(con, name: str) -> bool:
    row = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?;", [name]).fetchone()
    return bool(row and row[0] > 0)


def _view_exists(con, name: str) -> bool:
    row = con.execute("SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?;", [name]).fetchone()
    return bool(row and row[0] > 0)


def _latest_end_date_with_min_rows(con, view: str, *, min_rows: int, lookback_periods: int = 16) -> tuple[str | None, int | None]:
    """Return (end_date, rowcount) for the latest end_date whose rowcount >= min_rows.

    Finance datasets are often partially populated for the newest report period due to
    reporting lags (e.g., annual reports right after 12/31). We validate the latest
    *well-populated* period instead of blindly using MAX(end_date).
    """
    rows = con.execute(
        f"""
        SELECT end_date, COUNT(*) AS c
        FROM {view}
        WHERE end_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT {int(lookback_periods)};
        """
    ).fetchall()
    for end_date, cnt in rows:
        if cnt is not None and int(cnt) >= int(min_rows):
            return str(end_date), int(cnt)
    if rows:
        end_date, cnt = rows[0]
        return (str(end_date) if end_date is not None else None, int(cnt) if cnt is not None else None)
    return None, None


def validate(cfg: RunConfig, token: str | None = None) -> None:
    logger.info("validate: start")
    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    use_ephemeral_duckdb = False
    try:
        # These operations write to the on-disk DuckDB (schema + persistent views).
        # If another process (e.g., update/backfill) holds an exclusive lock,
        # fall back to an in-memory DuckDB and validate from parquet directly.
        cat.ensure_schema()
        ensure_derived_views(cat)
    except Exception as e:  # noqa: BLE001
        if _is_duckdb_lock_error(e):
            use_ephemeral_duckdb = True
            logger.warning(
                "validate: DuckDB is locked by another process; running in read-only parquet mode (skipping persistent views): %s",
                e,
            )
        else:
            raise

    if use_ephemeral_duckdb:
        # Use an in-memory DuckDB connection so we don't touch/lock market.duckdb.
        # Create temporary parquet-backed views for validation queries.
        import duckdb

        con = duckdb.connect(":memory:")
        base_datasets = ["daily", "adj_factor", "daily_basic"]
        finance_datasets = [
            "income",
            "balancesheet",
            "cashflow",
            "forecast",
            "express",
            "dividend",
            "fina_indicator",
            "fina_audit",
            "fina_mainbz",
        ]
        for ds in base_datasets + finance_datasets:
            g = cat.parquet_glob(ds)
            if not _has_any_parquet(g):
                continue
            con.execute(f"CREATE OR REPLACE TEMP VIEW v_{ds} AS SELECT * FROM read_parquet('{g}', union_by_name=true);")
    else:
        con = None

    # Choose connection source.
    if con is None:
        cm = cat.connect()
    else:
        cm = contextlib.closing(con)

    with cm as con:
        # 1) Key uniqueness checks
        logger.info("validate: uniqueness checks")
        for view in ["v_daily", "v_adj_factor", "v_daily_basic"]:
            if not _view_exists(con, view):
                continue
            dup = con.execute(
                f"""
                SELECT ts_code, trade_date, COUNT(*) AS c
                FROM {view}
                GROUP BY 1,2
                HAVING c > 1
                LIMIT 5;
                """
            ).fetchall()
            if dup:
                raise RuntimeError(f"Found duplicates in {view} (ts_code, trade_date). Example: {dup[0]}")
        
        # 1b) Finance dataset uniqueness checks (ts_code, end_date)
        # Note: TuShare financial data can have duplicates due to amended reports, etc.
        # We'll warn but not fail on this.
        finance_views = ["v_income", "v_balancesheet", "v_cashflow", "v_forecast", "v_express", "v_dividend", "v_fina_indicator", "v_fina_audit", "v_fina_mainbz"]
        for view in finance_views:
            if not _view_exists(con, view):
                continue
            dup = con.execute(
                f"""
                SELECT ts_code, end_date, COUNT(*) AS c
                FROM {view}
                GROUP BY 1,2
                HAVING c > 1
                LIMIT 5;
                """
            ).fetchall()
            if dup:
                logger.warning(f"Found duplicates in {view} (ts_code, end_date). Example: {dup[0]} - this is normal for amended reports")

        # 2) Recent-day sanity: daily rowcount should be "not tiny"
        if _view_exists(con, "v_daily"):
            logger.info("validate: recent-day sanity")
            row = con.execute("SELECT MAX(trade_date) FROM v_daily;").fetchone()
            latest = row[0] if row else None
            if latest:
                cnt = con.execute("SELECT COUNT(*) FROM v_daily WHERE trade_date = ?;", [latest]).fetchone()[0]
                if cnt < 1000:
                    raise RuntimeError(f"daily rowcount too small for {latest}: {cnt}")
        
        # 2b) Recent-quarter sanity for finance datasets
        finance_sanity_views = ["v_income", "v_balancesheet", "v_cashflow"]
        for view in finance_sanity_views:
            if not _view_exists(con, view):
                continue
            logger.info("validate: recent-quarter sanity (%s)", view)

            max_end = con.execute(f"SELECT MAX(end_date) FROM {view};").fetchone()
            max_end_date = str(max_end[0]) if max_end and max_end[0] is not None else None
            max_cnt = None
            if max_end_date:
                max_cnt = int(con.execute(f"SELECT COUNT(*) FROM {view} WHERE end_date = ?;", [max_end_date]).fetchone()[0])

            picked_end_date, picked_cnt = _latest_end_date_with_min_rows(con, view, min_rows=1000)
            if not picked_end_date or picked_cnt is None:
                raise RuntimeError(f"{view} has no data")

            if max_end_date and max_cnt is not None and max_end_date != picked_end_date and max_cnt < 1000:
                logger.warning(
                    "%s latest end_date %s has only %d rows; validating previous well-populated period %s (%d rows)",
                    view,
                    max_end_date,
                    max_cnt,
                    picked_end_date,
                    picked_cnt,
                )

            if picked_cnt < 1000:
                # Guardrail: If even the best recent period is tiny, something is wrong.
                raise RuntimeError(f"{view} rowcount too small for period {picked_end_date}: {picked_cnt}")

        # 3) Derived view exists when inputs exist
        if (not use_ephemeral_duckdb) and _view_exists(con, "v_daily") and _view_exists(con, "v_adj_factor"):
            logger.info("validate: derived view presence")
            if not _view_exists(con, "v_daily_adj"):
                raise RuntimeError("Expected v_daily_adj view to exist")

    # 4) Optional spot-check vs pro_bar (qfq) on latest range
    if use_ephemeral_duckdb:
        logger.info("validate: done (parquet-only mode; skip remote spot-check)")
        return
    if not token:
        logger.info("validate: done (no remote spot-check)")
        return

    limiter = RateLimiter(rpm=min(cfg.rpm, 120))  # keep validation gentle
    client = TushareClient(token=token, limiter=limiter)

    with cat.connect() as con:
        if not _view_exists(con, "v_daily_adj"):
            logger.info("validate: done (no v_daily_adj)")
            return
        latest = con.execute("SELECT MAX(trade_date) FROM v_daily;").fetchone()[0]
        if not latest:
            logger.info("validate: done (no v_daily)")
            return
        # Pick a few liquid-looking symbols (first 3) from stock_basic if present, else from daily.
        ts_codes = []
        stock_basic_path = os.path.join(cfg.parquet_dir, "stock_basic", "latest.parquet")
        if os.path.exists(stock_basic_path):
            ts_codes = [r[0] for r in con.execute(f"SELECT ts_code FROM read_parquet('{stock_basic_path}') LIMIT 3;").fetchall()]
        if not ts_codes:
            ts_codes = [r[0] for r in con.execute("SELECT DISTINCT ts_code FROM v_daily LIMIT 3;").fetchall()]

        # Compare last ~30 trading days.
        start = con.execute(
            """
            SELECT MIN(trade_date) FROM (
              SELECT DISTINCT trade_date
              FROM v_daily
              WHERE trade_date <= ?
              ORDER BY trade_date DESC
              LIMIT 30
            );
            """,
            [latest],
        ).fetchone()[0]
        if not start:
            logger.info("validate: done (not enough days)")
            return

        logger.info("validate: remote spot-check (%d symbols)", len(ts_codes))
        for ts_code in ts_codes:
            local = con.execute(
                """
                SELECT trade_date, qfq_close
                FROM v_daily_adj
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date;
                """,
                [ts_code, start, latest],
            ).fetchdf()

            try:
                remote = client.pro_bar(ts_code=ts_code, adj="qfq", start_date=start, end_date=latest)
            except Exception as e:  # noqa: BLE001
                # Optional check: don't fail validation if upstream is throttling/flaky
                # or if the SDK itself errors (we've seen `OSError: ERROR.` from pro_bar).
                if isinstance(e, (RateLimitError, TransientError)):
                    logger.warning("validate: skip remote spot-check for %s due to %s", ts_code, e)
                else:
                    logger.warning("validate: skip remote spot-check for %s due to %r", ts_code, e)
                continue
            if remote is None or remote.empty or local.empty:
                continue
            remote = remote[["trade_date", "close"]].rename(columns={"close": "qfq_close_remote"})
            merged = local.merge(remote, on="trade_date", how="inner")
            if merged.empty:
                continue
            # allow tiny floating rounding error
            max_abs = (merged["qfq_close"] - merged["qfq_close_remote"]).abs().max()
            if max_abs and max_abs > 1e-4:
                raise RuntimeError(f"qfq spot-check failed for {ts_code}: max_abs_diff={max_abs}")

    logger.info("validate: done")


