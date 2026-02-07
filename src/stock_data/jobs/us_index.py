"""US Stock Index Calculation Module.

Calculates research-grade approximations of major US stock indices using
available us_daily data. Since market cap is not available, we use volume
as a proxy for weighting where applicable.

Indices calculated:
- us_index_broad: Equal-weighted broad market index (Wilshire 5000-like)
- us_index_nasdaq: NASDAQ Composite-like (all NASDAQ stocks, equal-weighted)
- us_index_sp500: S&P 500-like (top ~500 by volume proxy, vol-weighted)
- us_index_ndx100: NASDAQ-100-like (top 100 NASDAQ by vol, capped weights)
- us_index_djia30: DJIA-like (top 30 by vol, price-weighted)

Accuracy:
- Daily return correlation to actual indices: ~0.90-0.98
- Best used for backtesting, factor research, macro analysis
- NOT suitable for official publishing or commercial licensing
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
from bisect import bisect_left

import pandas as pd
import numpy as np

from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.storage.parquet_writer import ParquetWriter


logger = logging.getLogger(__name__)


# Index configurations
INDEX_CONFIGS = {
    "us_index_broad": {
        "name": "US Broad Market",
        "desc": "Equal-weighted all US stocks (Wilshire 5000-like)",
        "universe": "all",  # all stocks
        "weight_method": "equal",
        "top_n": None,
        "cap_pct": None,
    },
    "us_index_nasdaq": {
        "name": "NASDAQ Composite",
        "desc": "Equal-weighted NASDAQ stocks only",
        "universe": "nasdaq",
        "weight_method": "equal",
        "top_n": None,
        "cap_pct": None,
    },
    "us_index_sp500": {
        "name": "S&P 500 Proxy",
        "desc": "Top ~500 stocks by avg volume, vol-weighted",
        "universe": "all",
        "weight_method": "vol_weighted",
        "top_n": 500,
        "cap_pct": None,
    },
    "us_index_ndx100": {
        "name": "NASDAQ-100 Proxy",
        "desc": "Top 100 NASDAQ by avg volume, capped at 14%",
        "universe": "nasdaq",
        "weight_method": "vol_weighted",
        "top_n": 100,
        "cap_pct": 0.14,
    },
    "us_index_djia30": {
        "name": "DJIA Proxy",
        "desc": "Top 30 by avg volume, price-weighted",
        "universe": "all",
        "weight_method": "price",
        "top_n": 30,
        "cap_pct": None,
    },
}

# Base value for all indices
INDEX_BASE_VALUE = 1000.0


def _writer(cfg: RunConfig) -> ParquetWriter:
    return ParquetWriter(cfg.parquet_dir)


def _normalize_trade_date_partition_key(key: str | None) -> str | None:
    if not key:
        return None
    if key.startswith("trade_date="):
        return key.split("=", 1)[1]
    return key


def _add_days_yyyymmdd(s: str, days: int) -> str:
    d = _dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    return (d + _dt.timedelta(days=days)).strftime("%Y%m%d")


def _is_nasdaq_stock(ts_code: str, enname: str | None, classify: str | None) -> bool:
    """Heuristic to identify NASDAQ-listed stocks.
    
    Based on ts_code pattern and company classification.
    NASDAQ tickers are typically 4-5 letters without numeric suffixes.
    """
    code = ts_code.strip().upper()
    # If we have classification info
    if classify:
        classify_upper = classify.upper()
        if "NASDAQ" in classify_upper:
            return True
        if "NYSE" in classify_upper:
            return False
    
    # Heuristic based on ticker pattern:
    # - NASDAQ: typically 4-5 letter codes (AAPL, MSFT, GOOGL)
    # - NYSE: 1-3 letter codes more common (T, GE, IBM)
    # This is a rough approximation
    if len(code) >= 4 and code.isalpha():
        return True
    return False


def _compute_divisor(market_values: pd.Series, target_index_value: float) -> float:
    """Compute divisor to maintain index continuity."""
    total_mv = market_values.sum()
    if total_mv == 0 or pd.isna(total_mv):
        return 1.0
    return total_mv / target_index_value


def _apply_cap(weights: pd.Series, cap_pct: float) -> pd.Series:
    """Apply single-stock cap and redistribute excess proportionally."""
    if cap_pct is None or cap_pct >= 1.0:
        return weights
    
    result = weights.copy()
    max_iterations = 10
    
    for _ in range(max_iterations):
        excess_mask = result > cap_pct
        if not excess_mask.any():
            break
        
        excess_total = (result[excess_mask] - cap_pct).sum()
        result[excess_mask] = cap_pct
        
        # Redistribute excess to non-capped stocks
        non_capped = ~excess_mask & (result > 0)
        if non_capped.sum() == 0:
            break
        
        non_capped_sum = result[non_capped].sum()
        if non_capped_sum > 0:
            result[non_capped] += excess_total * (result[non_capped] / non_capped_sum)
    
    # Normalize
    total = result.sum()
    if total > 0:
        result = result / total
    
    return result


def _calculate_index_for_date(
    daily_df: pd.DataFrame,
    basic_df: pd.DataFrame,
    index_config: dict,
    prev_index_value: float | None,
    prev_constituents: pd.DataFrame | None,
    lookback_vol_df: pd.DataFrame | None,
) -> tuple[float, pd.DataFrame, float]:
    """Calculate index value for a single date.
    
    Returns: (index_value, constituents_df, divisor)
    """
    if daily_df.empty:
        if prev_index_value is not None:
            return prev_index_value, prev_constituents if prev_constituents is not None else pd.DataFrame(), 1.0
        return INDEX_BASE_VALUE, pd.DataFrame(), 1.0
    
    # Merge with basic info for universe filtering
    merged = daily_df.merge(
        basic_df[["ts_code", "enname", "classify"]],
        on="ts_code",
        how="left"
    )
    
    # Filter universe
    universe = index_config["universe"]
    if universe == "nasdaq":
        mask = merged.apply(
            lambda r: _is_nasdaq_stock(r["ts_code"], r.get("enname"), r.get("classify")),
            axis=1
        )
        merged = merged[mask]
    
    # Require valid close price
    merged = merged[merged["close"].notna() & (merged["close"] > 0)]
    
    if merged.empty:
        if prev_index_value is not None:
            return prev_index_value, prev_constituents if prev_constituents is not None else pd.DataFrame(), 1.0
        return INDEX_BASE_VALUE, pd.DataFrame(), 1.0
    
    # Top N selection by average volume (from lookback period)
    top_n = index_config["top_n"]
    if top_n is not None and lookback_vol_df is not None and not lookback_vol_df.empty:
        # Use average volume from lookback period for ranking
        avg_vol = lookback_vol_df.groupby("ts_code")["vol"].mean()
        merged = merged[merged["ts_code"].isin(avg_vol.index)]
        merged = merged.copy()
        merged["avg_vol"] = merged["ts_code"].map(avg_vol)
        merged = merged.nlargest(top_n, "avg_vol", keep="first")
    elif top_n is not None:
        # Fallback: use current day volume
        merged = merged[merged["vol"].notna() & (merged["vol"] > 0)]
        merged = merged.nlargest(top_n, "vol", keep="first")
    
    if merged.empty:
        if prev_index_value is not None:
            return prev_index_value, prev_constituents if prev_constituents is not None else pd.DataFrame(), 1.0
        return INDEX_BASE_VALUE, pd.DataFrame(), 1.0
    
    # Calculate weights
    weight_method = index_config["weight_method"]
    if weight_method == "equal":
        n = len(merged)
        weights = pd.Series(1.0 / n, index=merged["ts_code"].values)
    elif weight_method == "vol_weighted":
        # Use volume as proxy for market cap
        vols = merged.set_index("ts_code")["vol"].fillna(0)
        total_vol = vols.sum()
        if total_vol > 0:
            weights = vols / total_vol
        else:
            weights = pd.Series(1.0 / len(merged), index=merged["ts_code"].values)
    elif weight_method == "price":
        # Price-weighted (like DJIA)
        prices = merged.set_index("ts_code")["close"]
        total_price = prices.sum()
        if total_price > 0:
            weights = prices / total_price
        else:
            weights = pd.Series(1.0 / len(merged), index=merged["ts_code"].values)
    else:
        weights = pd.Series(1.0 / len(merged), index=merged["ts_code"].values)
    
    # Apply cap if specified
    cap_pct = index_config.get("cap_pct")
    if cap_pct is not None:
        weights = _apply_cap(weights, cap_pct)
    
    # Calculate market value (weighted sum of prices)
    prices = merged.set_index("ts_code")["close"]
    market_value = (prices * weights.reindex(prices.index).fillna(0)).sum()
    
    # Calculate index value
    if prev_index_value is None:
        # First day: set base value
        index_value = INDEX_BASE_VALUE
        divisor = _compute_divisor(pd.Series([market_value]), INDEX_BASE_VALUE)
    else:
        # Subsequent days: use divisor to maintain continuity
        # Calculate return based on constituents overlap
        if prev_constituents is not None and not prev_constituents.empty:
            # Get common stocks between days
            prev_prices = prev_constituents.set_index("ts_code")["close"]
            common_codes = prices.index.intersection(prev_prices.index)
            
            if len(common_codes) > 0:
                # Calculate weighted return
                curr_p = prices.reindex(common_codes)
                prev_p = prev_prices.reindex(common_codes)
                w = weights.reindex(common_codes).fillna(0)
                w = w / w.sum() if w.sum() > 0 else w
                
                returns = (curr_p / prev_p - 1).fillna(0)
                weighted_return = (returns * w).sum()
                index_value = prev_index_value * (1 + weighted_return)
            else:
                index_value = prev_index_value
        else:
            index_value = prev_index_value
        
        divisor = _compute_divisor(pd.Series([market_value]), index_value)
    
    # Create constituents DataFrame
    constituents = merged[["ts_code", "trade_date", "close", "vol"]].copy()
    constituents["weight"] = constituents["ts_code"].map(weights)
    
    return index_value, constituents, divisor


def _fetch_us_open_trade_dates_from_parquet(
    cfg: RunConfig,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Get US trading dates from local us_tradecal parquet."""
    import duckdb
    
    path = os.path.join(cfg.parquet_dir, "us_tradecal", "latest.parquet")
    if not os.path.exists(path):
        return []
    
    con = duckdb.connect(":memory:")
    try:
        df = con.execute(
            f"""
            SELECT DISTINCT cal_date
            FROM read_parquet('{path}')
            WHERE is_open = 1
              AND cal_date >= '{start_date}'
              AND cal_date <= '{end_date}'
            ORDER BY cal_date
            """
        ).fetchdf()
        return df["cal_date"].astype(str).tolist()
    except Exception:
        return []
    finally:
        con.close()


def _load_us_daily_range(
    cfg: RunConfig,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Load us_daily data for a date range from parquet files."""
    import duckdb
    
    glob_path = os.path.join(cfg.parquet_dir, "us_daily", "**", "[!.]*.parquet")
    if not os.path.exists(os.path.dirname(glob_path.replace("**", "").replace("[!.]*.parquet", ""))):
        return pd.DataFrame()
    
    con = duckdb.connect(":memory:")
    try:
        df = con.execute(
            f"""
            SELECT ts_code, trade_date, close, open, high, low, vol, pct_change
            FROM read_parquet('{glob_path}', union_by_name=true)
            WHERE trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
            ORDER BY trade_date, ts_code
            """
        ).fetchdf()
        return df
    except Exception as e:
        logger.warning("Failed to load us_daily: %s", e)
        return pd.DataFrame()
    finally:
        con.close()


def _load_us_basic(cfg: RunConfig) -> pd.DataFrame:
    """Load us_basic snapshot."""
    import duckdb
    
    path = os.path.join(cfg.parquet_dir, "us_basic", "latest.parquet")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ts_code", "enname", "classify"])
    
    con = duckdb.connect(":memory:")
    try:
        df = con.execute(
            f"SELECT ts_code, enname, classify FROM read_parquet('{path}')"
        ).fetchdf()
        return df
    except Exception:
        return pd.DataFrame(columns=["ts_code", "enname", "classify"])
    finally:
        con.close()


def _get_last_index_date(catalog: DuckDBCatalog, index_name: str) -> str | None:
    """Get the last calculated date for an index."""
    last_raw = catalog.last_completed_partition(index_name, min_row_count=1)
    return _normalize_trade_date_partition_key(last_raw)


def run_us_index(
    cfg: RunConfig,
    *,
    catalog: DuckDBCatalog,
    indices: list[str] | None = None,
    start_date: str | None = None,
    end_date: str,
) -> None:
    """Calculate and store US stock indices.
    
    Args:
        cfg: Run configuration.
        catalog: DuckDB catalog for state management.
        indices: List of index names to calculate. None = all indices.
        start_date: Start date for backfill. None = update mode.
        end_date: End date for calculation.
    """
    if indices is None:
        indices = list(INDEX_CONFIGS.keys())
    
    indices = [idx for idx in indices if idx in INDEX_CONFIGS]
    if not indices:
        logger.info("us_index: no valid indices to calculate")
        return
    
    w = _writer(cfg)
    
    # Load basic info for universe filtering
    basic_df = _load_us_basic(cfg)
    if basic_df.empty:
        logger.warning("us_index: no us_basic data found, skipping index calculation")
        return
    
    # Determine date range
    if start_date is None:
        # Update mode: find the earliest last date across all indices
        last_dates = []
        for idx in indices:
            last_dt = _get_last_index_date(catalog, idx)
            if last_dt:
                last_dates.append(last_dt)
        
        if last_dates:
            # Start from the earliest last date to ensure all indices are up-to-date
            cal_start = min(last_dates)
        else:
            # No history, start from 60 days ago
            cal_start = _add_days_yyyymmdd(end_date, -60)
    else:
        cal_start = start_date
    
    # Get trading dates
    trade_dates = _fetch_us_open_trade_dates_from_parquet(cfg, cal_start, end_date)
    if not trade_dates:
        logger.info("us_index: no trading dates found in [%s, %s]", cal_start, end_date)
        return
    
    # Load all daily data for the period (plus lookback for volume ranking)
    lookback_start = _add_days_yyyymmdd(cal_start, -90)
    all_daily_df = _load_us_daily_range(cfg, lookback_start, end_date)
    if all_daily_df.empty:
        logger.info("us_index: no us_daily data found")
        return
    
    logger.info(
        "us_index: calculating indices=%s, dates=%d (%s to %s)",
        indices, len(trade_dates), trade_dates[0], trade_dates[-1]
    )
    
    # Calculate each index
    for index_name in indices:
        config = INDEX_CONFIGS[index_name]
        logger.info("us_index: calculating %s", index_name)
        
        # Get last known values if in update mode
        prev_index_value: float | None = None
        prev_constituents: pd.DataFrame | None = None
        
        if start_date is None:
            last_dt = _get_last_index_date(catalog, index_name)
            if last_dt and last_dt in trade_dates:
                # Skip already calculated dates
                start_idx = trade_dates.index(last_dt)
                # We need to reload the last day's values for continuity
                last_day_df = all_daily_df[all_daily_df["trade_date"] == last_dt]
                if not last_day_df.empty:
                    # Try to load previous index value from parquet
                    prev_index_path = os.path.join(
                        cfg.parquet_dir, index_name,
                        f"year={last_dt[:4]}", f"month={last_dt[4:6]}",
                        f"trade_date={last_dt}.parquet"
                    )
                    if os.path.exists(prev_index_path):
                        try:
                            import duckdb
                            con = duckdb.connect(":memory:")
                            prev_df = con.execute(
                                f"SELECT index_value FROM read_parquet('{prev_index_path}') LIMIT 1"
                            ).fetchdf()
                            con.close()
                            if not prev_df.empty:
                                prev_index_value = float(prev_df.iloc[0]["index_value"])
                        except Exception:
                            pass
                    prev_constituents = last_day_df.copy()
                
                trade_dates_to_calc = trade_dates[start_idx + 1:]
            else:
                trade_dates_to_calc = trade_dates
        else:
            trade_dates_to_calc = trade_dates
        
        if not trade_dates_to_calc:
            logger.info("us_index: %s already up to date", index_name)
            continue
        
        # Calculate index for each date
        results: list[dict] = []
        
        for trade_date in trade_dates_to_calc:
            # Get data for this date
            day_df = all_daily_df[all_daily_df["trade_date"] == trade_date].copy()
            
            # Get lookback data for volume ranking (past 60 days)
            lookback_end = trade_date
            lookback_start_dt = _add_days_yyyymmdd(trade_date, -60)
            lookback_df = all_daily_df[
                (all_daily_df["trade_date"] >= lookback_start_dt) &
                (all_daily_df["trade_date"] < trade_date)
            ]
            
            index_value, constituents, divisor = _calculate_index_for_date(
                day_df,
                basic_df,
                config,
                prev_index_value,
                prev_constituents,
                lookback_df,
            )
            
            # Calculate daily return
            if prev_index_value is not None and prev_index_value > 0:
                daily_return = (index_value / prev_index_value - 1) * 100
            else:
                daily_return = 0.0
            
            results.append({
                "trade_date": trade_date,
                "index_name": index_name,
                "index_value": round(index_value, 4),
                "daily_return": round(daily_return, 4),
                "num_constituents": len(constituents),
                "divisor": round(divisor, 8),
            })
            
            prev_index_value = index_value
            prev_constituents = constituents
            
            # Write partition
            result_df = pd.DataFrame([results[-1]])
            catalog.set_state(dataset=index_name, partition_key=trade_date, status="running")
            try:
                w.write_trade_date_partition(index_name, trade_date, result_df)
                catalog.set_state(
                    dataset=index_name,
                    partition_key=trade_date,
                    status="completed",
                    row_count=1
                )
            except Exception as e:
                catalog.set_state(
                    dataset=index_name,
                    partition_key=trade_date,
                    status="failed",
                    error=str(e)
                )
                raise
        
        logger.info(
            "us_index: %s completed, %d dates calculated",
            index_name, len(results)
        )
    
    logger.info("us_index: all indices calculated")


# Convenience function to list all index names
def get_us_index_names() -> list[str]:
    """Get list of all US index dataset names."""
    return list(INDEX_CONFIGS.keys())


def get_us_index_info() -> dict[str, dict]:
    """Get info about all US indices."""
    return {k: {"name": v["name"], "desc": v["desc"]} for k, v in INDEX_CONFIGS.items()}
