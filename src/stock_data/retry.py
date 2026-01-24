from __future__ import annotations

import logging
import random

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)


logger = logging.getLogger(__name__)


class TransientError(RuntimeError):
    """Retryable transient failure (network, throttling, etc)."""


class RateLimitError(TransientError):
    """Retryable throttling failure that typically needs a long wait (>= 60s)."""


def is_transient_exception(exc: BaseException) -> bool:
    return isinstance(exc, (TimeoutError, ConnectionError, TransientError))


def _wait_seconds(retry_state: RetryCallState) -> float:
    """Compute wait time based on exception type.

    - RateLimitError: wait about a minute (Tushare window), add small jitter
    - Other transient: exponential backoff with jitter (tenacity helper)
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, RateLimitError):
        return 61.0 + random.uniform(0.0, 3.0)

    if isinstance(exc, TransientError) and exc is not None and "Empty response" in str(exc):
        # Avoid hot-looping on empty responses; give upstream a little room.
        base = wait_random_exponential(multiplier=1, max=60)
        return max(2.0, float(base(retry_state)))

    # Keep previous behavior for generic transient issues.
    base = wait_random_exponential(multiplier=1, max=60)
    return float(base(retry_state))


def _before_sleep(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    sleep = getattr(retry_state.next_action, "sleep", None)
    fn = getattr(retry_state.fn, "__qualname__", str(retry_state.fn))
    attempt = retry_state.attempt_number
    level = logger.warning
    if isinstance(exc, TransientError) and exc is not None and "Empty response" in str(exc):
        level = logger.debug
    if sleep is None:
        level("Retrying %s (attempt %s) after error: %s", fn, attempt, exc)
    else:
        level(
            "Retrying %s (attempt %s) in %.1fs after error: %s",
            fn,
            attempt,
            float(sleep),
            exc,
        )


def retry_policy(max_attempts: int = 8):
    return retry(
        reraise=True,
        stop=stop_after_attempt(max_attempts),
        wait=_wait_seconds,
        before_sleep=_before_sleep,
        retry=retry_if_exception_type((TimeoutError, ConnectionError, TransientError)),
    )

