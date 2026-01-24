from __future__ import annotations

import contextlib
import os
import threading
from typing import Optional
from dataclasses import dataclass


@dataclass
class DuckDBCatalog:
    duckdb_path: str
    parquet_root: str

    def __post_init__(self) -> None:
        # DuckDB file handles can conflict under heavy concurrent connect/close.
        # Reuse a single connection and serialize access with a lock.
        self._lock = threading.RLock()
        self._con = None

    def ensure_schema(self) -> None:
        os.makedirs(os.path.dirname(self.duckdb_path), exist_ok=True)
        with self.connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_state (
                  dataset VARCHAR,
                  partition_key VARCHAR,
                  status VARCHAR,
                  updated_at TIMESTAMP,
                  row_count BIGINT,
                  error VARCHAR
                );
                """
            )
            # A unique index makes upserts deterministic.
            con.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ingestion_state_pk
                ON ingestion_state(dataset, partition_key);
                """
            )

    @contextlib.contextmanager
    def connect(self):
        # Import lazily.
        import duckdb

        with self._lock:
            if self._con is None:
                self._con = duckdb.connect(self.duckdb_path)
            yield self._con

    def close(self) -> None:
        """Close the underlying DuckDB connection (optional)."""
        with self._lock:
            if self._con is not None:
                try:
                    self._con.close()
                finally:
                    self._con = None

    def set_state(self, *, dataset: str, partition_key: str, status: str, row_count: int | None = None, error: str | None = None) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT INTO ingestion_state(dataset, partition_key, status, updated_at, row_count, error)
                VALUES (?, ?, ?, NOW(), ?, ?)
                ON CONFLICT (dataset, partition_key)
                DO UPDATE SET
                  status = excluded.status,
                  updated_at = excluded.updated_at,
                  row_count = excluded.row_count,
                  error = excluded.error;
                """,
                [dataset, partition_key, status, row_count, error],
            )

    def completed_partitions(self, dataset: str, *, min_row_count: int | None = None) -> set[str]:
        with self.connect() as con:
            if min_row_count is None:
                rows = con.execute(
                    "SELECT partition_key FROM ingestion_state WHERE dataset = ? AND status = 'completed';",
                    [dataset],
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT partition_key
                    FROM ingestion_state
                    WHERE dataset = ?
                      AND status = 'completed'
                      AND COALESCE(row_count, 0) >= ?;
                    """,
                    [dataset, int(min_row_count)],
                ).fetchall()
        return {r[0] for r in rows}

    def last_completed_partition(self, dataset: str, *, min_row_count: int | None = None) -> str | None:
        with self.connect() as con:
            if min_row_count is None:
                row = con.execute(
                    """
                    SELECT partition_key
                    FROM ingestion_state
                    WHERE dataset = ? AND status = 'completed'
                    ORDER BY partition_key DESC
                    LIMIT 1;
                    """,
                    [dataset],
                ).fetchone()
            else:
                row = con.execute(
                    """
                    SELECT partition_key
                    FROM ingestion_state
                    WHERE dataset = ?
                      AND status = 'completed'
                      AND COALESCE(row_count, 0) >= ?
                    ORDER BY partition_key DESC
                    LIMIT 1;
                    """,
                    [dataset, int(min_row_count)],
                ).fetchone()
        return row[0] if row else None

    def parquet_glob(self, dataset: str) -> str:
        # DuckDB read_parquet supports globbing.
        return os.path.join(self.parquet_root, dataset, "**", "*.parquet")

    def fail_running_partitions(self, *, reason: str = "stale running (previous run interrupted)", older_than_seconds: int | None = 300) -> int:
        """Mark leftover `running` partitions as `failed`.

        This prevents interrupted runs from leaving confusing `running` state.

        Args:
            reason: Error message written into `ingestion_state.error`.
            older_than_seconds: Only mark rows whose `updated_at` is older than this.
                Pass None to mark all `running` rows.

        Returns:
            Number of rows updated.
        """
        with self.connect() as con:
            if older_than_seconds is None:
                res = con.execute(
                    """
                    UPDATE ingestion_state
                    SET status = 'failed', updated_at = NOW(), error = ?
                    WHERE status = 'running';
                    """,
                    [reason],
                )
            else:
                res = con.execute(
                    """
                    UPDATE ingestion_state
                    SET status = 'failed', updated_at = NOW(), error = ?
                    WHERE status = 'running'
                      AND updated_at < (NOW() - (? * INTERVAL '1 second'));
                    """,
                    [reason, int(older_than_seconds)],
                )

            try:
                n = int(res.rowcount)
                return n if n > 0 else 0
            except Exception:
                # DuckDB's rowcount support may vary by version.
                return 0

