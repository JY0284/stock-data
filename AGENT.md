# Agent Notes (stock_data)

This repo is a local “data lake” for Tushare datasets:
- Ingestion jobs download from Tushare and write partitioned Parquet under `store/parquet/`.
- DuckDB is used for (a) ingestion progress/state tracking and (b) SQL query views over Parquet.
- `StockStore` provides a stable Python API; the CLI and web service build on it.

## Quick commands

- Install: `uv sync`
- Token: `export TUSHARE_TOKEN=...`
- Backfill: `uv run stock-data backfill --start-date 20100101 --end-date 20231231`
- Update (incremental): `uv run stock-data update`
- Stats: `uv run stock-data stat`
- Query: `uv run stock-data query --sql "SELECT MAX(trade_date) FROM v_daily"`
- Serve: `uv run stock-data serve --store store --host 127.0.0.1 --port 8000`
- Tests: `uv run python -m pytest -q`

## Where things live

- Dataset registry: `src/stock_data/datasets.py`
- Orchestration: `src/stock_data/pipeline.py`
- Ingestion jobs: `src/stock_data/jobs/*.py`
- Storage layer:
	- Parquet writes: `src/stock_data/storage/parquet_writer.py`
	- DuckDB state: `src/stock_data/storage/duckdb_catalog.py`
	- DuckDB parquet views: `src/stock_data/storage/parquet_views.py`
- Read API: `src/stock_data/store.py` (`open_store`, dataset-specific helpers)
- Web service: `src/stock_data/web_service.py`
- Agent-friendly wrappers: `src/stock_data/agent_tools.py`

## Storage layout conventions

This project relies on consistent naming so readers and view registration can “discover” data.

- Snapshot datasets (universe-like):
	- Parquet: `store/parquet/<dataset>/latest.parquet`
	- Ingestion state keys: typically `asof=YYYYMMDD` (any stable string is fine)
- Trade-date partitioned datasets (market “dailyish”):
	- Parquet: `store/parquet/<dataset>/year=YYYY/month=MM/trade_date=YYYYMMDD.parquet`
	- Ingestion state keys: **must be `YYYYMMDD`**
- ts_code partitioned datasets (per-symbol files):
	- Parquet: `store/parquet/<dataset>/ts_code=<TS_CODE>.parquet` (pattern varies by dataset)
	- Update logic typically uses `start_date`/`end_date` and merges+dedupes into the same file

## Ingestion state conventions (important)

- For **trade_date-partitioned** datasets (e.g. `daily`, `weekly`, `us_daily`), the DuckDB catalog `partition_key` **must be the plain date string**: `YYYYMMDD`.
- Do **not** store keys like `trade_date=YYYYMMDD` for trade-date partitions.

Why this matters:
- Update scheduling uses date comparisons (sorting, `bisect`, building `[start, end]` windows) and assumes the partition key is directly comparable to `YYYYMMDD`.
- If a non-normalized key is used, updates may silently schedule nothing because the key is not a valid date for upstream APIs.

If you ever need to change a key format:
- Add a small normalization helper so older keys keep working.
- Add a regression test that exercises the normalization.

## Adding a new dataset (checklist)

1) Registry
- Add dataset metadata to `src/stock_data/datasets.py` (name, category, partitioning, descriptions).

2) Ingestion
- Add/extend a job module under `src/stock_data/jobs/`.
- Use `DuckDBCatalog.set_state(...)` consistently (`running` → `completed|skipped|failed`).
- For “today might be empty” APIs, prefer `skipped` (not `completed`) so a later `update` can pick it up.
- Write Parquet via `ParquetWriter` helpers to keep directory layout consistent.

3) Pipeline wiring
- Ensure `src/stock_data/pipeline.py` includes the dataset in the correct category and routes to the right job.

4) Store API
- Update dataset type mappings in `src/stock_data/store.py` (snapshot vs trade_date vs ts_code).
- Add a small, typed helper method if the dataset is commonly used (it also documents intended filters).

5) DuckDB views
- Add the dataset to `src/stock_data/storage/parquet_views.py` so `v_<dataset>` can be queried via DuckDB.

6) Web + agent tools (optional but recommended)
- If it’s a “major” dataset, add:
	- a `GET /<category>/...` convenience endpoint in `src/stock_data/web_service.py`
	- a JSON-friendly wrapper in `src/stock_data/agent_tools.py`

7) Tests
- Add a tiny parquet fixture to an existing test store generator (or create a new one).
- Cover:
	- `StockStore` read path
	- agent tools wrapper (pagination/sorting)
	- web endpoint behavior (status code + format)

## Common operational pitfalls

- DuckDB lock contention: `serve` / long queries can lock the DB; many commands fall back to filesystem stats when locked.
- AppleDouble `._*` files (macOS zip/unzip): these can break DuckDB `read_parquet` globbing; use `stock-data clean-store` or `dot_clean` before archiving.
- Rate limiting: keep `cfg.rpm` conservative; Tushare returns transient errors under load—prefer retryable exceptions and idempotent writes.

## Debugging tips

- Check ingestion state:
	- `uv run stock-data query --sql "SELECT * FROM ingestion_state WHERE dataset='us_daily' ORDER BY partition_key DESC LIMIT 20"`
- Check view coverage:
	- `uv run stock-data query --sql "SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM v_us_daily"`
