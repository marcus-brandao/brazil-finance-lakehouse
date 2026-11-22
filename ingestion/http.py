import json
import logging
import random
import time
from collections.abc import Callable
from typing import Self

import httpx

log = logging.getLogger("ingestion.http")

RETRYABLE_STATUS = {429} | set(range(500, 600))


class HttpError(Exception):
    def __init__(
        self, message: str, url: str, status: int | None = None, attempts: int = 1
    ):
        super().__init__(message)
        self.url = url
        self.status = status
        self.attempts = attempts


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        timeout: float = 30,
        max_attempts: int = 5,
        base_delay: float = 1,
        max_delay: float = 60,
        min_interval: float = 0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
        transport: httpx.BaseTransport | None = None,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.min_interval = min_interval
        self._sleep = sleep
        self._clock = clock
        self._jitter = jitter
        self._last_call: float | None = None
        self._client = httpx.Client(
            headers={"User-Agent": user_agent}, timeout=timeout, transport=transport
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self._client.close()

    def get(self, url: str, params: dict | None = None) -> httpx.Response:
        last_error = "no attempts made"
        last_status: int | None = None
        for attempt in range(1, self.max_attempts + 1):
            self._wait_for_slot()
            delay = 0.0
            exc: Exception | None = None
            try:
                resp = self._client.get(url, params=params)
            except httpx.TransportError as e:
                exc = e
                last_status, last_error = None, f"{type(e).__name__}: {e}"
            else:
                if resp.status_code < 400:
                    self._log(attempt, resp.status_code, 0, url)
                    return resp
                last_status = resp.status_code
                last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                if resp.status_code not in RETRYABLE_STATUS:
                    self._log(attempt, resp.status_code, 0, url)
                    raise HttpError(last_error, url, resp.status_code, attempt)

            if attempt < self.max_attempts:
                delay = self._delay(attempt, resp if exc is None else None)
            self._log(attempt, last_status, delay, url)
            if attempt < self.max_attempts:
                self._sleep(delay)

        raise HttpError(
            f"gave up after {self.max_attempts} attempts on {url}: {last_error}",
            url,
            last_status,
            self.max_attempts,
        )

    def _delay(self, attempt: int, resp: httpx.Response | None) -> float:
        # Only the seconds form of Retry-After is read; a date falls back to backoff.
        if resp is not None and resp.status_code == 429:
            try:
                return min(float(resp.headers["Retry-After"]), self.max_delay)
            except (KeyError, ValueError):
                pass
        return (
            min(self.max_delay, self.base_delay * 2 ** (attempt - 1)) * self._jitter()
        )

    def _wait_for_slot(self) -> None:
        now = self._clock()
        if self._last_call is not None:
            wait = self._last_call + self.min_interval - now
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
        self._last_call = now

    def _log(self, attempt: int, status: int | None, delay: float, url: str) -> None:
        log.info(
            json.dumps(
                {"attempt": attempt, "status": status, "delay": delay, "url": url}
            )
        )
