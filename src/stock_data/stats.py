from __future__ import annotations

import os
from dataclasses import dataclass
import re

from stock_data.datasets import ALL_DATASET_NAMES
from stock_data.runner import RunConfig


def _parse_datasets(datasets: str) -> list[str]:
    if datasets.strip().lower() in {"all", "*"}:
        return list(ALL_DATASET_NAMES)
    return [d.strip() for d in datasets.split(",") if d.strip()]


@dataclass(frozen=True)
class DatasetStat:
    dataset: str
    completed: int | None
    failed: int | None
    running: int | None
    min_partition: str | None
    max_partition: str | None
    total_rows: int | None
    parquet_files: int
    parquet_bytes: int


def _dir_size_bytes(path: str) -> int:
    total = 0
    if not os.path.exists(path):
        return 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            fp = os.path.join(root, name)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


_TRADE_DATE_RE = re.compile(r"trade_date=(\d{8})\.parquet$")
_END_DATE_RE = re.compile(r"end_date=(\d{8})\.parquet$")


def _parquet_file_count_and_date_range(dataset_dir: str) -> tuple[int, str | None, str | None]:
    """Return (file_count, min_date, max_date) using filenames.

    This does not open Parquet files, so it works even while ingestion is running
    or if DuckDB is locked by another process.
    
    Recognizes both trade_date= and end_date= patterns.
    """
    file_count = 0
    min_d = None
    max_d = None
    if not os.path.exists(dataset_dir):
        return 0, None, None

    for root, _dirs, files in os.walk(dataset_dir):
        for name in files:
            if not name.endswith(".parquet"):
                continue
            file_count += 1
            # Try trade_date pattern first
            m = _TRADE_DATE_RE.search(name)
            if not m:
                # Try end_date pattern (for finance datasets)
                m = _END_DATE_RE.search(name)
            if not m:
                continue
            d = m.group(1)
            if min_d is None or d < min_d:
                min_d = d
            if max_d is None or d > max_d:
                max_d = d

    return file_count, min_d, max_d


def _fmt_bytes(n: int) -> str:
    unit = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    i = 0
    while size >= 1024.0 and i < len(unit) - 1:
        size /= 1024.0
        i += 1
    if i == 0:
        return f"{int(size)} {unit[i]}"
    return f"{size:.2f} {unit[i]}"


def _fetch_duckdb_stats_readonly(duckdb_path: str, dataset: str) -> tuple[int, int, int, str | None, str | None, int]:
    """Best-effort read-only stats from ingestion_state.

    Raises on lock issues or missing tables.
    """
    import duckdb

    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        counts = con.execute(
            """
            SELECT status, COUNT(*)
            FROM ingestion_state
            WHERE dataset = ?
            GROUP BY status;
            """,
            [dataset],
        ).fetchall()
        by_status = {s: int(n) for (s, n) in counts}

        row = con.execute(
            """
            SELECT
              MIN(CASE WHEN status='completed' THEN partition_key END) AS min_p,
              MAX(CASE WHEN status='completed' THEN partition_key END) AS max_p,
              SUM(CASE WHEN status='completed' THEN COALESCE(row_count,0) ELSE 0 END) AS total_rows
            FROM ingestion_state
            WHERE dataset = ?;
            """,
            [dataset],
        ).fetchone()

        min_p, max_p, total_rows = row
        return (
            by_status.get("completed", 0),
            by_status.get("failed", 0),
            by_status.get("running", 0),
            str(min_p) if min_p is not None else None,
            str(max_p) if max_p is not None else None,
            int(total_rows or 0),
        )
    finally:
        con.close()


def _fetch_stats(cfg: RunConfig, dataset: str, *, try_duckdb: bool) -> DatasetStat:
    parquet_path = os.path.join(cfg.parquet_dir, dataset)
    parquet_bytes = _dir_size_bytes(parquet_path)
    parquet_files, min_d, max_d = _parquet_file_count_and_date_range(parquet_path)

    completed = failed = running = None
    min_p = max_p = None
    total_rows = None

    if try_duckdb and os.path.exists(cfg.duckdb_path):
        try:
            completed, failed, running, min_p, max_p, total_rows = _fetch_duckdb_stats_readonly(cfg.duckdb_path, dataset)
        except Exception:
            # DuckDB may be locked by another process; fall back to filesystem-only stats.
            completed = failed = running = None
            min_p = max_p = None
            total_rows = None

    # If ingestion_state isn't available, derive min/max from parquet filenames.
    if min_p is None and max_p is None and (min_d or max_d):
        min_p, max_p = min_d, max_d

    return DatasetStat(
        dataset=dataset,
        completed=completed,
        failed=failed,
        running=running,
        min_partition=min_p,
        max_partition=max_p,
        total_rows=total_rows,
        parquet_files=parquet_files,
        parquet_bytes=int(parquet_bytes),
    )


def print_stats(cfg: RunConfig, *, datasets: str = "all") -> None:
    selected = _parse_datasets(datasets)

    # Try to read ingestion_state from DuckDB (read-only). If DuckDB is locked,
    # we still print filesystem-only stats from Parquet.
    try_duckdb = True
    stats = [_fetch_stats(cfg, ds, try_duckdb=try_duckdb) for ds in selected]

    # Print a compact table.
    header = (
        f"{'dataset':<14} {'min':<10} {'max':<10} {'rows':>12} {'files':>7} "
        f"{'completed':>9} {'failed':>6} {'running':>7} {'parquet':>10}"
    )
    print(header)
    print("-" * len(header))

    total_bytes = 0
    for s in stats:
        total_bytes += s.parquet_bytes

        rows = "-" if s.total_rows is None else str(s.total_rows)
        completed = "-" if s.completed is None else str(s.completed)
        failed = "-" if s.failed is None else str(s.failed)
        running = "-" if s.running is None else str(s.running)
        print(
            f"{s.dataset:<14} "
            f"{(s.min_partition or '-'): <10} "
            f"{(s.max_partition or '-'): <10} "
            f"{rows:>12} "
            f"{s.parquet_files:>7} "
            f"{completed:>9} "
            f"{failed:>6} "
            f"{running:>7} "
            f"{_fmt_bytes(s.parquet_bytes):>10}"
        )

    print("")
    print(f"Parquet total: {_fmt_bytes(total_bytes)}")
