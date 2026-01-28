# Stock Data Storage Survey & Best Practices

Date: 2026-01-28

This document surveys the current on-disk storage design (Parquet + DuckDB metadata) and gives best practices for common quantitative stock-analysis workloads.

## 1) Current Storage Architecture (as implemented)

### 1.1 Canonical data store: Parquet
The canonical data lives under `store/parquet/<dataset>/...`.

Datasets are stored in a few patterns (see `ParquetWriter`):

- **Snapshot datasets** (single file):
  - Path pattern: `store/parquet/<dataset>/latest.parquet` (or `trade_cal/<exchange>_latest.parquet`)
  - Examples: `stock_basic`, `trade_cal`, `index_basic`, `fund_basic`, `disclosure_date`

- **Trade-date partitioned datasets** (many small files):
  - Path pattern: `store/parquet/<dataset>/year=YYYY/month=MM/trade_date=YYYYMMDD.parquet`
  - Examples: `daily`, `daily_basic`, `adj_factor`, `weekly`, `monthly`, `stk_limit`, `suspend_d`, `etf_daily`

- **Report-period (end_date) partitioned datasets** (finance):
  - Path pattern: `store/parquet/<dataset>/year=YYYY/quarter=Q/end_date=YYYYMMDD.parquet`
  - Examples: `income`, `balancesheet`, `cashflow`, `forecast`, `express`, `fina_indicator`, `fina_mainbz`

- **ts_code partitioned datasets** (one file per code, containing full history):
  - Path pattern: `store/parquet/<dataset>/ts_code=<CODE>.parquet` (dots replaced with underscores)
  - Examples: `index_daily`, `dividend`, `fina_audit`, `fund_nav`, `fund_share`, `fund_div`

### 1.2 DuckDB usage: metadata + convenience
DuckDB is stored at `store/duckdb/market.duckdb`.

DuckDB is used for three concrete things in this project:

1) **Ingestion state / bookkeeping**
- The ingestion pipeline writes and updates a small table `ingestion_state`:
  - key: `(dataset, partition_key)`
  - fields: `status` (`running`/`completed`/`failed`), `updated_at`, `row_count`, `error`
- This enables:
  - incremental runs (only fetch missing partitions)
  - visibility into failures
  - periodic refresh policies for “ts_code full-history” datasets

2) **Convenience SQL views over Parquet (optional, derived)**
- The pipeline can create `v_<dataset>` views that are backed by Parquet globs.
- It also creates a derived `v_daily_adj` view that joins `daily` + `adj_factor` and computes qfq/hfq columns.
- These views are a convenience layer; the system is designed to keep working even if views are missing.

3) **Query/validation tooling**
- `stock-data query` runs ad-hoc SQL using DuckDB.
- `stock-data stat` will *try* to read `ingestion_state` in **read-only** mode for status/row counts, and falls back to filesystem-only stats if DuckDB is locked.
- `stock-data validate` may build/check views; if DuckDB is locked, it can validate directly from Parquet in an in-memory DuckDB.

Important: in this design, **DuckDB is not the canonical store**; Parquet is. DuckDB is metadata + a query engine.

Practical implication:
- If you sync/replace Parquet files without updating DuckDB, your data files may be newer than DuckDB metadata (state/views). Use `stock-data validate` and/or refresh derived views when needed.

## 2) What we measured in this workspace

From local inspection:

- Total Parquet size: ~4.0 GB
- Total Parquet files: ~74,845

Key daily-like datasets:

- `daily`: 8,572 files, ~866 MB, ~17.4M rows
- `daily_basic`: 8,572 files, ~1.95 GB, ~17.3M rows
- `adj_factor`: 8,573 files, ~214 MB, ~18.2M rows

Daily file size distribution (`daily`):

- count: 8,572 files
- median (p50): ~77 KB
- p90: ~252 KB
- max: ~305 KB

Interpretation:
- These partitions are **market-wide per day** (many stocks per file), not “one stock per file”.
- File count is moderate for day-partitioning (~8.6k trading days since 1990).
- File sizes are small; overhead is mostly “open many files + parse metadata”, not raw IO.

## 3) Performance model for common query patterns

### 3.1 Pattern A: a few ts_code over a date range (single query)
Example: “3 stocks, last 1 year”.

With day-partitioning, the store typically reads ~N trading-day files (≈240 for 1 year) and filters ts_code in SQL.

Approximate work (for `daily` in this store):

- Files opened: ~240
- Data scanned: ~240 × ~100 KB ≈ 24 MB (plus parquet/duckdb overhead)
- Rows scanned: ~240 × (~2,000 rows/day) ≈ 0.5M rows

This is usually acceptable on a laptop.

### 3.2 Pattern B: many stocks over a date range (panel extraction)
Example: “CSI300 / all A-shares for 5 years”.

Day-partitioning is a strong match:
- One query reads each day file once.
- Good for factor research, cross-sectional signals, backtests.

### 3.3 Pattern C: many per-stock queries in a loop (anti-pattern)
Example: “for each of 300 stocks: query last 5 years”.

This is where performance collapses:
- Each per-stock query will re-open and re-scan the same day files.
- You pay the day-file overhead *hundreds of times*.

Rule of thumb:
- If you are looping `store.daily(ts_code=...)` for many codes, you should switch to a single panel query.

## 4) How the current reader behaves (important details)

### 4.1 Partition pruning happens when you provide a date range
For trade-date datasets, when `start_date` and `end_date` are provided, the reader tries to:

- compute trading days from `trade_cal`
- construct explicit partition file paths
- pass a list of file paths to DuckDB `read_parquet([...])`

This avoids scanning the entire dataset.

If anything goes wrong (missing calendar, bad schema, etc.), it falls back to a glob of `**/*.parquet`, which can be much slower.

### 4.2 Finance reads now prune by expected quarters when possible
For end_date-partitioned finance datasets, when `start_period` and `end_period` are provided, the reader prefers enumerating expected quarter partition files and only reading those.

### 4.3 `trade_cal` dtype normalization
`is_open` is normalized to int (0/1) on read so that trading-day logic stays stable even if it was ingested as strings.

## 5) Best practices for stock analysis

### 5.1 Prefer “panel queries” over per-stock loops
Instead of:
- loop 500 times: `daily(ts_code=..., start_date=..., end_date=...)`

Prefer:
- one query: `read('daily', start_date=..., end_date=..., where={'ts_code': [..many..]})`
- then filter/split in pandas

If you need an API helper for this pattern, add a method such as:
- `daily_panel(ts_codes: list[str], start_date, end_date, columns=None)`

### 5.2 Cache strategically
If you repeatedly pull the same slices:
- use the store’s in-memory TTL cache (already present)
- call `refresh_version()` after updating/syncing the store

### 5.3 Keep queries narrow
- Select only the columns you need
- Limit date ranges during exploration
- Avoid requesting full-history unless required

### 5.4 Make sure partition pruning stays enabled
- Always provide `start_date` and `end_date` when possible
- Keep `trade_cal` present and valid
- Validate after ingestion (`stock-data validate`) to avoid silent fallbacks

### 5.5 When to introduce a second layout (mirror/compaction)
You will likely want an additional, query-optimized layout when:

- You frequently do deep per-stock reads (few ts_code, very long history) *and*
  the same date range is revisited often.
- You need very fast interactive charts for a single stock across decades.
- You run large-scale strategy code that repeatedly “walks” each stock independently.

Recommended options:

**Option 1: By-code mirror (Parquet)**
- Write a compacted mirror dataset like `daily_by_code/ts_code=.../year=YYYY.parquet`
- Update it from the canonical day-partitioned store
- Benefits: fast per-stock reads, limited file count per stock
- Costs: extra storage + maintenance

**Option 2: DuckDB accelerator table**
- Create a persistent DuckDB table (or materialized view) containing `daily` data
- Sort/index logically by `(ts_code, trade_date)`
- Benefits: very fast repeated queries; fewer file-open overheads
- Costs: ingestion complexity, storage duplication inside DuckDB

**Option 3: Coarser partitions (monthly)**
- Store trade-date datasets as monthly files instead of daily
- Benefits: reduces file count by ~20×
- Costs: larger rewrite units; incremental updates rewrite a bigger file

For most quant research, day-partitioned canonical store + panel queries is usually the best baseline.

## 6) Operational notes

### 6.1 ts_code datasets must be refreshed periodically
Some ts_code-partitioned datasets grow over time (ETF nav/share/div, dividend, audit, index_daily).

Best practice:
- Refresh these partitions periodically (e.g. every 7 days)

Control:
- `STOCK_DATA_TS_CODE_REFRESH_DAYS=7` (default)
- Set `<= 0` to disable periodic refresh (not recommended unless you do manual refresh)

### 6.2 Avoid "silent schema drift" surprises
Reads use `union_by_name=true`, which tolerates missing/new columns.

Best practice:
- Run validation after ingestion
- For production research pipelines, consider storing schema versions/expected columns per dataset.

## 7) Recommended next improvements (future work)

If/when performance becomes a problem, prioritize:

1. Add a first-class “panel” API for multi-ts_code queries (discourage per-stock loops)
2. Add optional compaction/mirror job (by-code or monthly)
3. Add a lightweight manifest/metadata for cache invalidation (avoid relying only on DuckDB mtime)
4. Add schema contracts per dataset to detect upstream API changes early

---

Appendix: Useful commands

- Show dataset stats:
  - `stock-data stat --datasets daily,daily_basic,adj_factor`

- Validate local store:
  - `stock-data validate`

- Update store (token required):
  - `uv run stock-data update`
