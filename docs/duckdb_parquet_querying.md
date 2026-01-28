# How DuckDB Queries Our Parquet Store (File & Column Pruning)

This document explains how DuckDB decides **which Parquet files** to read and **which columns** to load when you run SQL in this repo.

It uses our `StockStore` implementation as the concrete example.

## TL;DR

- DuckDB can query Parquet *directly* via the table function `read_parquet(...)`.
- In our project, the **application (Python)** often decides the *file list* to scan (partition pruning) and passes it to DuckDB.
- DuckDB decides the *column set* to read by analyzing the SQL (projection pushdown / column pruning).
- DuckDB may also skip work using Parquet metadata (row-group pruning via statistics), but for trade-date partitioned data the biggest win is usually **file pruning**.

---

## 1) Where this happens in our code

### 1.1 Trade-date datasets (daily-like)

The key pieces are:

- `StockStore._dataset_relation_expr(...)`
  - returns an expression like `read_parquet(?, union_by_name=true)` plus parameters (`params`) that are either:
    - a **list of file paths** for the requested date range, or
    - a **glob** like `store/parquet/daily/**/*.parquet` (fallback)

- `StockStore._read_trade_date_dataset(...)`
  - builds SQL like:
    - `SELECT <cols> FROM read_parquet(?) WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?`
  - runs it via DuckDB: `con.execute(sql, params).fetchdf()`

### 1.2 End-date datasets (finance)

Finance datasets are `end_date` partitioned. When you provide a period range, the store attempts to enumerate expected quarter files and pass an explicit file list.

### 1.3 ts_code datasets

For `fund_nav`, `dividend`, etc., the store reads **one file per code** directly:

- `read_parquet('.../ts_code=XXXXXX_XX.parquet')`

---

## 2) How DuckDB knows which files to read

DuckDB can discover files in two ways:

### 2.1 You pass an explicit file list (recommended)

If Python passes a list like:

- `['.../trade_date=20260102.parquet', '.../trade_date=20260103.parquet', ...]`

then DuckDB will only open those files.

In our project, this is the normal path when you call `daily(...)` with both `start_date` and `end_date`.

Why it’s fast:
- it avoids scanning decades of history when you only want a short range
- the “partition pruning” happens before DuckDB even starts execution

### 2.2 You pass a glob path (fallback)

If the store can’t build a file list (missing calendar, errors, no date range), it falls back to a glob like:

- `store/parquet/daily/**/[!.]*.parquet`

DuckDB will expand this glob and scan potentially many files.

---

## 3) How DuckDB knows which columns to read

DuckDB chooses columns from the SQL plan.

### 3.1 Projection pushdown (column pruning)

If your SQL is:

- `SELECT trade_date, close FROM ... WHERE ts_code = '300888.SZ'`

DuckDB only needs to read:
- `trade_date` (selected)
- `close` (selected)
- `ts_code` (used in `WHERE`)

It does **not** need to load other columns like `high`, `low`, `amount`, etc.

If you write `SELECT *`, DuckDB must load all columns.

**Best practice**: always pass `columns=[...]` when you can.

### 3.2 Ordering and grouping require extra columns

If you add:
- `ORDER BY trade_date`

DuckDB must read `trade_date` even if you didn’t select it.

Same idea for `GROUP BY`, `JOIN`, window functions, etc.

---

## 4) How DuckDB can skip reading data inside files

Even after choosing files and columns, DuckDB may skip work using Parquet metadata.

### 4.1 Row-group pruning via Parquet statistics (when possible)

Parquet stores per-row-group min/max statistics for many columns.

If DuckDB sees a filter like:
- `trade_date >= '20250101'`

it may skip row groups whose min/max prove they can’t match.

In our trade-date partitioned layout, each file already represents a single date, so **file pruning** is usually the bigger win than row-group pruning.

### 4.2 Predicate pushdown (filter pushdown)

DuckDB tries to push filters down to scans so it filters early.

However, for `ts_code = ?` inside a market-wide daily file, DuckDB still must scan the row groups containing many codes. It can’t skip the whole file unless the file was partitioned by `ts_code`.

---

## 5) `union_by_name=true` and schema drift

We frequently call:

- `read_parquet(..., union_by_name=true)`

This means:
- when scanning multiple files, DuckDB aligns columns by name
- if a file is missing a column, DuckDB fills NULL for that file

Why we use it:
- ingestion can create placeholder files
- upstream API columns can vary

Tradeoff:
- it can hide schema drift; validation is important

---

## 6) What this means for performance (common patterns)

### 6.1 Fast pattern: one panel query

Good:
- one query for many codes + a date range
- scan each day file once

Example (conceptual):

- `SELECT ts_code, trade_date, close FROM read_parquet(<date_files>) WHERE ts_code IN (...)`

### 6.2 Slow pattern: looping per stock

Bad:
- loop 500 times: `daily(ts_code=..., start_date=..., end_date=...)`
- each loop re-opens and scans the same day files

Solution:
- do one panel query, then split in pandas

---

## 7) How to “see” what DuckDB will do

DuckDB supports `EXPLAIN`.

You can run this via the CLI:

- `stock-data query --sql "EXPLAIN SELECT close FROM read_parquet('store/parquet/daily/year=2026/month=01/*.parquet', union_by_name=true) WHERE ts_code='300888.SZ'"`

This prints the logical/physical plan, showing scan operators, filters, and projections.

---

## 8) Practical checklist

When writing analysis code:

- Prefer passing `start_date` + `end_date` to enable file pruning
- Prefer `columns=[...]` (avoid `SELECT *`)
- Prefer one “panel query” for many codes over loops
- Use `stock-data query --sql "EXPLAIN ..."` when performance is surprising

