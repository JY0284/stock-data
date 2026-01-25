from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_data.rate_limit import RateLimiter
from stock_data.retry import RateLimitError, TransientError, retry_policy


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

        import tushare as ts

        try:
            pro = ts.pro_api(self.token)
            df = pro.query(api_name, **params)

            # Some endpoints are expected to return non-empty data for open days.
            # If Tushare returns an empty/columnless frame (sometimes happens under
            # throttling/partial failures), treat it as transient so tenacity retries.
            if isinstance(df, pd.DataFrame) and (len(df.columns) == 0 or df.empty):
                if api_name in {"daily", "adj_factor", "daily_basic", "weekly", "monthly", "fund_daily"}:
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

