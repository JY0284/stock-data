from __future__ import annotations

import collections
import threading
import time


class RateLimiter:
    """
    Simple rolling-window rate limiter (requests per 60 seconds).

    Designed for multi-threaded use: each remote call should call `acquire()`.
    """

    def __init__(self, *, rpm: int):
        if rpm <= 0:
            raise ValueError("rpm must be > 0")
        self._rpm = rpm
        self._lock = threading.Lock()
        self._calls = collections.deque()  # timestamps (monotonic)
        self._cooldown_until = 0.0

    def note_rate_limited(self, *, cooldown_s: float = 65.0) -> None:
        """Trigger a shared cooldown across all threads."""
        now = time.monotonic()
        until = now + max(0.0, float(cooldown_s))
        with self._lock:
            if until > self._cooldown_until:
                self._cooldown_until = until

    def acquire(self) -> None:
        while True:
            sleep_s = 0.0
            now = time.monotonic()
            with self._lock:
                if now < self._cooldown_until:
                    sleep_s = self._cooldown_until - now
                else:
                    cutoff = now - 60.0
                    while self._calls and self._calls[0] < cutoff:
                        self._calls.popleft()

                    if len(self._calls) < self._rpm:
                        self._calls.append(now)
                        return

                    earliest = self._calls[0]
                    sleep_s = max(0.0, 60.0 - (now - earliest))

            # Sleep outside the lock so other threads can progress.
            time.sleep(min(sleep_s, 5.0))

