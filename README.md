# Stock Data Lake (Tushare)

Local ingestion tool for Tushare A-share data (stock list, daily prices, adj_factors, basic indicators), stored as **DuckDB + partitioned Parquet**.

Designed for robustness:
- **Rate limiting**: Enforces ~500 calls/min (safe for 5000-point Tushare accounts).
- **Concurrency**: Parallel downloads for daily partitions.
- **Resumable**: Tracks progress in DuckDB; safe to interrupt and restart.
- **Idempotent**: Overwrites parquet partitions cleanly.

## Setup

1. **Install dependencies** (assuming `uv` or standard pip):
   ```bash
   # If using uv (recommended):
   uv sync
   source .venv/bin/activate
   
   # Or standard pip:
   pip install -e .
   ```

2. **Set your Tushare Token**:
   ```bash
   export TUSHARE_TOKEN="your_token_here"
   ```

## Usage

The CLI `stock-data` is the main entry point.

### 1. Backfill Historical Data
Download history (e.g., from 2010 to 2023). This runs concurrent workers and may take a while.
```bash
stock-data backfill --start-date 20100101 --end-date 20231231
```

### 2. Daily Update
Run this daily (e.g., via cron) to fetch new data since the last successful run.
```bash
stock-data update
```

By default, `update` downloads up to **today** (based on local date). You can pin the end date:

```bash
stock-data update --end-date 20260123
```

### 3. Validate Data
Checks for row counts, uniqueness, and (optionally) spot-checks computed prices against Tushare's `pro_bar` interface.
```bash
stock-data validate
```

If you copied `store/` from macOS using zip/unzip, you may have AppleDouble files named like `._*.parquet`.
DuckDB's `read_parquet()` globbing can pick these up and fail with errors like:
`Invalid Input Error: No magic bytes found ... '.../._something.parquet'`.

Cleanup on the target machine:

- Linux:
   - `find store -name '._*' -type f -delete`
   - `find store -name '.DS_Store' -type f -delete`
- macOS (before archiving):
   - `dot_clean -m store`

Safer transfer options:

- `rsync -a --exclude='._*' --exclude='.DS_Store' store/ user@host:/path/to/store/`
- `COPYFILE_DISABLE=1 tar -czf store.tgz store` (then unpack on Linux)

You can also clean an existing store directory via CLI:

```bash
# Preview
stock-data clean-store --store store --dry-run

# Actually delete the files
stock-data clean-store --store store
```

### 4. Query Data (SQL)
You can run SQL directly against the DuckDB catalog. Views like `v_daily`, `v_adj_factor`, and **`v_daily_adj`** (computed qfq/hfq prices) are auto-created.

```bash
# Check latest available date
stock-data query --sql "SELECT MAX(trade_date) FROM v_daily"

# Check computed QFQ prices for a specific stock
stock-data query --sql "SELECT trade_date, open, close, qfq_close FROM v_daily_adj WHERE ts_code='000001.SZ' ORDER BY trade_date DESC LIMIT 5"
```

### 5. Stats (range / size)

Print local dataset coverage and storage size.

```bash
# All datasets
stock-data stat

# Only selected datasets
stock-data stat --datasets daily,adj_factor,daily_basic
```

Notes:
- When DuckDB is available, this includes `ingestion_state` stats (completed/failed/running and total rows).
- If DuckDB is locked by another process, it falls back to filesystem-only stats from the Parquet layout (date range from filenames, file counts, and sizes).

### 6. List Datasets (descriptions)

List all supported datasets with user-friendly descriptions.

```bash
# bilingual (default)
stock-data datasets

# English only
stock-data datasets --lang en

# 中文 only
stock-data datasets --lang zh
```

### 7. Sync Store from a Remote Service

If you already have a machine that has a complete/updated `store/`, you can run the HTTP service there and sync your local store from it.

On the **remote** machine (server):

```bash
stock-data serve --store store --host 0.0.0.0 --port 8000
```

On the **local** machine (client):

```bash
stock-data sync --store store --remote http://1.2.3.4:8000
```

If the remote uses default ports, you can omit the port:

```bash
stock-data sync --store store --remote http://stock-data.example.com
```

Notes:
- This sync only reads/writes files under `store/duckdb/` and `store/parquet/`.
- By default it skips files based on file size (fast; robust across machines with different mtimes).
- Use `--hash` for sha256 verification (slower, strictest).
- Use `--dry-run` to preview changes.
- `--delete` removes local files not present on remote (dangerous; use with care).

## Python API (Recommended)

The `StockStore` class provides a clean, high-level API for querying local data. It handles DuckDB connections, Parquet paths, and caching automatically.

```python
from stock_data.store import open_store

# Open the store (caching enabled by default)
store = open_store("store")

# Resolve symbol to ts_code
resolved = store.resolve("300888")
# -> ResolvedSymbol(symbol='300888', ts_code='300888.SZ', list_date='20200917')

# Get daily prices
df = store.daily("300888.SZ", start_date="20240101", end_date="20240131")

# Get adjusted prices (qfq = forward-adjusted)
df_adj = store.daily_adj("300888.SZ", start_date="20240101", how="qfq")

# Calendar helpers
trading_days = store.trading_days("20240101", "20240131")
is_open = store.is_trading_day("20240115")

# Universe / stock list
universe = store.universe(list_status="L", market="创业板")

# IPO info
ipo = store.new_share(year=2024)

# Generic query (escape hatch)
df = store.read("daily_basic", start_date="20240101", end_date="20240105")
```

Features:
- **Partition pruning**: Date-range queries only read relevant Parquet files.
- **Caching**: Repeated queries are served from memory (~1GB budget by default).
- **No views required**: Works even if DuckDB views are missing.

For a full demo, run:
```bash
python demos/use_store_api.py 300888 store
```

## Use the Data (DataFrame)

For lower-level access, you can query DuckDB views or Parquet files directly.

### Option A: DuckDB -> pandas

DuckDB reads the partitioned Parquet efficiently and can push down filters (dates, columns, symbols).

```python
import duckdb

con = duckdb.connect("store/duckdb/market.duckdb")

# 1) Daily OHLC for a date range
df_daily = con.execute(
   """
   SELECT ts_code, trade_date, open, high, low, close, vol, amount
   FROM v_daily
   WHERE trade_date BETWEEN '20231225' AND '20231229'
   """
).fetchdf()

# 2) QFQ/HFQ adjusted prices (derived view)
df_adj = con.execute(
   """
   SELECT ts_code, trade_date, close, adj_factor, qfq_close, hfq_close
   FROM v_daily_adj
   WHERE ts_code = '000001.SZ'
   ORDER BY trade_date
   """
).fetchdf()
```

If you don’t want to rely on views, you can query Parquet directly:

```python
import duckdb

con = duckdb.connect()

df = con.execute(
   """
   SELECT *
   FROM read_parquet('store/parquet/daily/**/[!.]*.parquet', union_by_name=true)
   WHERE trade_date = '20231226'
   LIMIT 10
   """
).fetchdf()
```

### Option B: Parquet -> pandas (simple)

Read a single date partition:

```python
import pandas as pd

df = pd.read_parquet("store/parquet/daily/year=2023/month=12/trade_date=20231226.parquet")
```

Read and concatenate multiple partitions:

```python
import glob
import pandas as pd

files = sorted(glob.glob("store/parquet/daily/year=2023/month=12/trade_date=202312*.parquet"))
df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
```

## Demo: One Stock, All Datasets

To quickly see how to consume the local store, run the demo script that resolves a symbol (e.g. `300888`) to `ts_code` and prints all available rows across our datasets:

```bash
python demos/print_stock_300888.py 300888 store
```

## Data Layout

Data is stored in `./store/` (default):

- `store/duckdb/market.duckdb`: Catalog and ingestion state.
- `store/parquet/<dataset>/`: Parquet files.
  - `daily`, `adj_factor`, etc.: partitioned by `year=YYYY/month=MM/trade_date=YYYYMMDD.parquet`.
  - `stock_basic`, `stock_company`: snapshots (`latest.parquet`).

## FAQ / Concepts

For the upstream interface overview, see Tushare docs:
- Basic data: https://tushare.pro/document/2?doc_id=24
- Market data: https://tushare.pro/document/2?doc_id=15

### 1) What is the data granularity (粒度)?

- **Market datasets** are stored at **trade-date** granularity (one partition per trading day):
   - `daily`, `adj_factor`, `daily_basic`, `stk_limit`, `suspend_d`: `trade_date=YYYYMMDD.parquet`
   - `weekly` / `monthly`: also partitioned by `trade_date` (week-end / month-end trading day)
- **Snapshot datasets** are not per-day partitions:
   - `stock_basic`, `stock_company`, `trade_cal`: snapshots like `latest.parquet`
   - `new_share`, `namechange`: windowed snapshots (e.g. by year)
- **DuckDB role**: DuckDB mainly stores `ingestion_state` (progress tracking). The actual market data lives in Parquet and is queried through DuckDB views like `v_daily`.

### 2) Can I read/query while backfill is running?

Yes, with a few notes:

- **Reading Parquet is safe and recommended** during ingestion.
   - Partitions are written using an atomic replace, so readers generally see either the old file or the new file.
- **Reading DuckDB `ingestion_state` is also possible**, but may occasionally wait briefly during writes.
- Practical recommendation: for analysis during ingestion, prefer querying Parquet-backed views (`v_daily`, `v_daily_adj`, etc.) or `read_parquet(...)`.

### 3) What do `backfill`, `update`, and `validate` do?

- **`backfill --start-date A --end-date B`**
   - Builds tasks for **every selected dataset** and every open trade date in **[A, B]**.
   - Re-downloads and **overwrites Parquet partitions** (idempotent overwrite).
   - Use when you want a full refresh of a historical window.

- **`update --end-date B`**
   - Incremental mode: defaults `B` to **today**.
   - For each dataset, starts scheduling from that dataset’s **last completed partition** (tracked in DuckDB `ingestion_state`) and fetches forward to `B`.
   - If a dataset has **no completed partition yet**, `update` does a minimal bootstrap: it fetches only the latest partition(s) up to `B` (it does not backfill full history).
   - Use for daily operations / catch-up.

- **`validate`**
   - Runs data sanity checks (readability, basic consistency) and optional spot-checks.
   - Does not ingest new data.

### 4) If A→B has already been backfilled once, what’s the difference between running `backfill A→B` again vs `update` to B?

- Running **`backfill A→B` again** will re-download and overwrite *all* partitions in that range (expensive but “force refresh”).
- Running **`update --end-date B`** will do almost nothing if everything is already completed; otherwise it only fills the missing/incomplete partitions (cheap and incremental).

## Datasets & Schemas

This project stores datasets under `store/parquet/<dataset>/` and uses DuckDB views (e.g. `v_daily`, `v_daily_adj`) for convenient querying.

Tip: you can always list supported datasets and their high-level meaning via:

```bash
stock-data datasets
```

Below are the **stored columns** for each dataset (as written to Parquet by this repo).

### Market (trade-date partitioned)

Files live at `store/parquet/<dataset>/year=YYYY/month=MM/trade_date=YYYYMMDD.parquet`.

#### `daily` (tushare: `daily`)

Columns:
- `ts_code`: Tushare security code (e.g. `000001.SZ`)
- `trade_date`: Trading date (`YYYYMMDD`)
- `open`, `high`, `low`, `close`: Prices
- `pre_close`: Previous close
- `change`: Price change vs previous close
- `pct_chg`: Percent change
- `vol`: Volume
- `amount`: Turnover amount

#### `adj_factor` (tushare: `adj_factor`)

Columns:
- `ts_code`
- `trade_date`
- `adj_factor`: Adjustment factor used for qfq/hfq calculations

#### `daily_basic` (tushare: `daily_basic`)

Columns:
- `ts_code`
- `trade_date`
- `close`: Close price (used as reference)
- `turnover_rate`, `turnover_rate_f`: Turnover rates
- `volume_ratio`: Volume ratio
- `pe`, `pe_ttm`: Price/Earnings
- `pb`: Price/Book
- `ps`, `ps_ttm`: Price/Sales
- `dv_ratio`, `dv_ttm`: Dividend yield metrics
- `total_share`, `float_share`, `free_share`: Share counts
- `total_mv`, `circ_mv`: Market capitalization (total / circulating)

#### `weekly` (tushare: `weekly`)

Partition key is the **week-end trading day**.

Columns:
- `ts_code`, `trade_date`
- `open`, `high`, `low`, `close`
- `pre_close`, `change`, `pct_chg`
- `vol`, `amount`

#### `monthly` (tushare: `monthly`)

Partition key is the **month-end trading day**.

Columns:
- `ts_code`, `trade_date`
- `open`, `high`, `low`, `close`
- `pre_close`, `change`, `pct_chg`
- `vol`, `amount`

#### `stk_limit` (tushare: `stk_limit`)

Columns:
- `trade_date`
- `ts_code`
- `up_limit`: Limit-up price
- `down_limit`: Limit-down price

#### `suspend_d` (tushare: `suspend_d`)

We fetch both `suspend_type=S` and `suspend_type=R` and concatenate.

Columns:
- `ts_code`
- `trade_date`
- `suspend_timing`: Suspension timing
- `suspend_type`: Suspension type

### Basic (snapshots / windows)

#### `stock_basic` (tushare: `stock_basic`)

File: `store/parquet/stock_basic/latest.parquet`

Columns:
- `ts_code`, `symbol`, `name`
- `area`, `industry`, `fullname`, `enname`, `cnspell`
- `market`, `exchange`, `curr_type`
- `list_status`, `list_date`, `delist_date`
- `is_hs`, `act_name`, `act_ent_type`

#### `trade_cal` (tushare: `trade_cal`)

File: `store/parquet/trade_cal/SSE_latest.parquet`

Columns:
- `exchange`: Exchange code (we use `SSE` for scheduling)
- `cal_date`: Calendar date (`YYYYMMDD`)
- `is_open`: Whether the exchange is open
- `pretrade_date`: Previous trading day

#### `stock_company` (tushare: `stock_company`)

File: `store/parquet/stock_company/latest.parquet`

Columns:
- `ts_code`, `com_name`, `com_id`, `exchange`
- `chairman`, `manager`, `secretary`
- `reg_capital`, `setup_date`
- `province`, `city`
- `introduction`, `website`, `email`, `office`
- `employees`, `main_business`, `business_scope`

#### `new_share` (tushare: `new_share`)

Files: `store/parquet/new_share/year=YYYY.parquet`

Columns:
- `ts_code`, `sub_code`, `name`
- `ipo_date`, `issue_date`
- `amount`, `market_amount`
- `price`, `pe`
- `limit_amount`, `funds`, `ballot`

#### `namechange` (tushare: `namechange`)

Files: `store/parquet/namechange/year=YYYY.parquet`

Columns:
- `ts_code`, `name`
- `start_date`, `end_date`
- `ann_date`
- `change_reason`

## Development

- `src/stock_data/`: Source code.
- `pyproject.toml`: Dependencies and configuration.
