from __future__ import annotations

import datetime as _dt
import logging
import os
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_data.rate_limit import RateLimiter
from stock_data.retry import RateLimitError, TransientError, retry_policy


logger = logging.getLogger(__name__)


_TRANSIENT_PATTERNS = [
    re.compile(r".*timeout.*", re.IGNORECASE),
    re.compile(r".*temporar.*unavailable.*", re.IGNORECASE),
    re.compile(r".*too many.*request.*", re.IGNORECASE),
    re.compile(r".*频次.*限制.*", re.IGNORECASE),
    re.compile(r".*访问.*过于频繁.*", re.IGNORECASE),
]


_RATE_LIMIT_PATTERNS = [
    re.compile(r".*每分钟最多访问.*", re.IGNORECASE),
    re.compile(r".*最多访问.*(次|次).*", re.IGNORECASE),
    re.compile(r".*访问该接口\d+次.*", re.IGNORECASE),
]


def _looks_transient_message(msg: str) -> bool:
    return any(p.search(msg or "") for p in _TRANSIENT_PATTERNS)


def _looks_rate_limited_message(msg: str) -> bool:
    return any(p.search(msg or "") for p in _RATE_LIMIT_PATTERNS)


@dataclass(frozen=True)
class TushareClient:
    token: str
    limiter: RateLimiter

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("token is empty")

        # Set global token used by some SDK helpers like ts.pro_bar.
        import tushare as ts

        ts.set_token(self.token)

    @retry_policy(max_attempts=8)
    def query(self, api_name: str, **params: Any):
        """
        Calls Tushare Pro endpoint using `pro.query(api_name, **params)`.

        Retries transient failures and respects the shared global rate limiter.
        """
        self.limiter.acquire()

        if os.environ.get("STOCK_DATA_LOG_TUSHARE_QUERY", "").strip() in {"1", "true", "True", "yes", "YES"}:
            # Keep this log line short (RichHandler wraps long dicts).
            keys = (
                "ts_code",
                "trade_date",
                "start_date",
                "end_date",
                "start_m",
                "end_m",
                "period",
                "exchange",
                "market",
                "is_open",
                "list_status",
            )
            slim = {k: params.get(k) for k in keys if k in params}

            # Avoid dumping huge strings (e.g. `fields=...`) while still indicating presence.
            if "fields" in params and isinstance(params.get("fields"), str):
                slim["fields_len"] = len(params["fields"])

            msg = " ".join([f"{k}={slim[k]}" for k in sorted(slim.keys())])
            logger.info("tushare: query api=%s %s", api_name, msg)

        import tushare as ts

        try:
            pro = ts.pro_api(self.token)
            df = pro.query(api_name, **params)

            # Some endpoints are expected to return non-empty data for open days.
            # If Tushare returns an empty/columnless frame (sometimes happens under
            # throttling/partial failures), treat it as transient so tenacity retries.
            if isinstance(df, pd.DataFrame) and (len(df.columns) == 0 or df.empty):
                if api_name in {"daily", "adj_factor", "daily_basic", "weekly", "monthly", "fund_daily"}:
                    trade_date = params.get("trade_date")
                    today = _dt.date.today().strftime("%Y%m%d")
                    # For today's trade_date, empty is often normal (data not published yet).
                    # Do not retry in that case; the scheduler should try again in a later run.
                    if not (isinstance(trade_date, str) and trade_date == today):
                        raise TransientError(f"Empty response for api={api_name} params={params}")

            return df
        except Exception as e:  # noqa: BLE001 - we map to transient/non-transient
            msg = str(e)
            if _looks_rate_limited_message(msg):
                self.limiter.note_rate_limited()
                raise RateLimitError(msg) from e
            if _looks_transient_message(msg):
                raise TransientError(msg) from e
            raise

    def query_all(
        self,
        api_name: str,
        **params: Any,
    ) -> pd.DataFrame:
        """Query an endpoint with pagination (offset/limit) and concatenate pages.

        Tushare Pro supports `offset` and `limit` for many endpoints. This helper
        is used for endpoints that can return more than one page (e.g. trade_cal,
        daily bars, VIP quarterly finance datasets).

        Behavior:
        - Uses `STOCK_DATA_TUSHARE_PAGE_SIZE` (default 5000).
        - If the endpoint rejects offset/limit and we're on the first page,
          falls back to a single `query` call without pagination.
        """

        page_size = params.pop("page_size", None)
        try:
            page_size = int(page_size) if page_size is not None else int(os.environ.get("STOCK_DATA_TUSHARE_PAGE_SIZE", "5000"))
        except Exception:
            page_size = 5000
        if page_size <= 0:
            page_size = 5000

        max_pages = params.pop("max_pages", None)
        try:
            max_pages = int(max_pages) if max_pages is not None else 2000
        except Exception:
            max_pages = 2000
        if max_pages <= 0:
            max_pages = 2000

        def _looks_offset_limit_unsupported(msg: str) -> bool:
            m = (msg or "").lower()
            needles = [
                "offset",
                "limit",
                "无效",
                "非法",
                "参数",
                "not support",
                "unsupported",
                "unknown",
                "unexpected",
            ]
            return any(n in m for n in needles)

        pages: list[pd.DataFrame] = []
        offset = 0
        for _ in range(max_pages):
            try:
                df_page = self.query(api_name, **params, limit=page_size, offset=offset)
            except Exception as e:  # noqa: BLE001
                # Some endpoints may not accept offset/limit. Fallback only on first page.
                if offset == 0 and _looks_offset_limit_unsupported(str(e)):
                    df_single = self.query(api_name, **params)
                    return df_single if isinstance(df_single, pd.DataFrame) else pd.DataFrame(df_single)
                raise

            if df_page is None:
                df_page = pd.DataFrame()

            if not isinstance(df_page, pd.DataFrame):
                df_page = pd.DataFrame(df_page)

            if len(df_page.columns) == 0 or df_page.empty:
                break

            pages.append(df_page)
            n = int(len(df_page))
            if n < page_size:
                break
            offset += page_size
        else:
            raise RuntimeError(f"Pagination exceeded max_pages={max_pages} for api={api_name}")

        if not pages:
            return pd.DataFrame()
        if len(pages) == 1:
            return pages[0]
        return pd.concat(pages, ignore_index=True)

    @retry_policy(max_attempts=6)
    def pro_bar(self, **params: Any):
        """
        Calls `ts.pro_bar(...)` (SDK-only integrated endpoint).
        Used only for validation spot checks.
        """
        self.limiter.acquire()
        import tushare as ts

        try:
            return ts.pro_bar(**params)
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if _looks_rate_limited_message(msg):
                self.limiter.note_rate_limited()
                raise RateLimitError(msg) from e
            if _looks_transient_message(msg):
                raise TransientError(msg) from e
            raise

