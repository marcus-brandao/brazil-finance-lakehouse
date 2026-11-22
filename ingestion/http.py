import json
import logging
import random
import time
from collections.abc import Callable
from typing import Self

import httpx

log = logging.getLogger("ingestion.http")


def is_retryable(status: int) -> bool:
    return status == 429 or 500 <= status < 600


def parse_retry_after(resp: httpx.Response) -> float | None:
    # Only the seconds form is read; an HTTP date falls back to backoff.
    try:
        return float(resp.headers["Retry-After"])
    except (KeyError, ValueError):
        return None


class HttpError(Exception):
    def __init__(
        self,
        message: str,
        url: str,
        status: int | None = None,
        attempts: int = 1,
        body: str = "",
    ):
        super().__init__(message)
        self.url = url
        self.status = status
        self.attempts = attempts
        self.body = body


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
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
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
        for attempt in range(1, self.max_attempts + 1):
            self._wait_for_slot()
            retry_after = None
            body = ""
            try:
                resp = self._client.get(url, params=params)
            except httpx.TransportError as e:
                status, error = None, f"{type(e).__name__}: {e}"
            else:
                status = resp.status_code
                # Redirects are not followed, so a 3xx is an error, not data.
                if status < 300:
                    self._log(attempt, status, 0, url)
                    return resp
                body = resp.text
                error = f"HTTP {status}: {body[:200]}"
                if not is_retryable(status):
                    self._log(attempt, status, 0, url)
                    raise HttpError(error, url, status, attempt, body)
                if status == 429:
                    retry_after = parse_retry_after(resp)

            if attempt == self.max_attempts:
                self._log(attempt, status, 0, url)
                raise HttpError(
                    f"gave up after {attempt} attempts on {url}: {error}",
                    url,
                    status,
                    attempt,
                    body,
                )
            delay = self._delay(attempt, retry_after)
            self._log(attempt, status, delay, url)
            self._sleep(delay)

    def _delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, self.max_delay)
        backoff = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return backoff * self._jitter()

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
