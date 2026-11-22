import json
import logging

import httpx
import pytest

from ingestion.http import HttpClient, HttpError

URL = "https://example.test/data"


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(handler, ft: FakeTime, **kw) -> HttpClient:
    return HttpClient(
        user_agent="test-agent",
        sleep=ft.sleep,
        clock=ft.clock,
        jitter=lambda: 1.0,
        transport=httpx.MockTransport(handler),
        **kw,
    )


def sequence(*items):
    """Handler that returns each item in turn; exceptions are raised."""
    calls = iter(items)
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = next(calls)
        if isinstance(item, Exception):
            raise item
        return item

    handler.seen = seen
    return handler


def test_success_first_try_sends_user_agent():
    ft = FakeTime()
    h = sequence(httpx.Response(200, json={"ok": True}))
    with make_client(h, ft) as c:
        resp = c.get(URL)
    assert resp.json() == {"ok": True}
    assert len(h.seen) == 1
    assert h.seen[0].headers["User-Agent"] == "test-agent"
    assert ft.sleeps == []


def test_retry_then_success_uses_exponential_backoff():
    ft = FakeTime()
    h = sequence(httpx.Response(503), httpx.Response(500), httpx.Response(200))
    with make_client(h, ft) as c:
        assert c.get(URL).status_code == 200
    assert ft.sleeps == [1, 2]


def test_gives_up_after_cap_and_never_returns():
    ft = FakeTime()
    h = sequence(*[httpx.Response(502, text="bad gateway")] * 3)
    with make_client(h, ft, max_attempts=3) as c, pytest.raises(HttpError) as e:
        c.get(URL)
    assert e.value.attempts == 3
    assert e.value.status == 502
    assert "3 attempts" in str(e.value)
    assert len(h.seen) == 3
    assert ft.sleeps == [1, 2]  # no sleep after the last attempt


def test_4xx_fails_fast_with_status_url_and_body():
    ft = FakeTime()
    h = sequence(httpx.Response(404, text="x" * 500))
    with make_client(h, ft) as c, pytest.raises(HttpError) as e:
        c.get(URL)
    assert e.value.status == 404
    assert e.value.url == URL
    assert e.value.attempts == 1
    assert "404" in str(e.value)
    assert "x" * 200 in str(e.value) and "x" * 201 not in str(e.value)
    assert len(h.seen) == 1


def test_3xx_raises_without_retry():
    ft = FakeTime()
    h = sequence(httpx.Response(307, headers={"Location": "https://example.test/x"}))
    with make_client(h, ft) as c, pytest.raises(HttpError) as e:
        c.get(URL)
    assert e.value.status == 307
    assert e.value.attempts == 1
    assert len(h.seen) == 1
    assert ft.sleeps == []


def test_error_carries_full_response_body():
    ft = FakeTime()
    h = sequence(httpx.Response(404, text="y" * 500))
    with make_client(h, ft) as c, pytest.raises(HttpError) as e:
        c.get(URL)
    assert e.value.body == "y" * 500


def test_error_after_retries_carries_last_body():
    ft = FakeTime()
    h = sequence(httpx.Response(503, text="first"), httpx.Response(503, text="last"))
    with make_client(h, ft, max_attempts=2) as c, pytest.raises(HttpError) as e:
        c.get(URL)
    assert e.value.body == "last"


def test_retry_after_honored_on_429():
    ft = FakeTime()
    h = sequence(httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200))
    with make_client(h, ft) as c:
        c.get(URL)
    assert ft.sleeps == [7]


def test_retry_after_is_capped_and_bad_values_fall_back():
    ft = FakeTime()
    h = sequence(
        httpx.Response(429, headers={"Retry-After": "9999"}),
        httpx.Response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
        httpx.Response(200),
    )
    with make_client(h, ft, max_delay=60) as c:
        c.get(URL)
    assert ft.sleeps == [60, 2]


def test_connection_error_retried_then_succeeds():
    ft = FakeTime()
    h = sequence(httpx.ConnectError("boom"), httpx.Response(200))
    with make_client(h, ft) as c:
        assert c.get(URL).status_code == 200
    assert ft.sleeps == [1]


def test_connection_error_every_attempt_raises_without_status():
    ft = FakeTime()
    h = sequence(*[httpx.ConnectError("boom")] * 2)
    with make_client(h, ft, max_attempts=2) as c, pytest.raises(HttpError) as e:
        c.get(URL)
    assert e.value.status is None
    assert e.value.attempts == 2
    assert "ConnectError" in str(e.value)


def test_min_interval_spaces_calls():
    ft = FakeTime()
    h = sequence(httpx.Response(200), httpx.Response(200), httpx.Response(200))
    with make_client(h, ft, min_interval=2) as c:
        c.get(URL)
        c.get(URL)
        ft.now += 5  # long enough since the last call, no wait needed
        c.get(URL)
    assert ft.sleeps == [2]


def test_one_json_log_line_per_attempt(caplog):
    ft = FakeTime()
    h = sequence(httpx.Response(503), httpx.Response(200))
    with (
        caplog.at_level(logging.INFO, logger="ingestion.http"),
        make_client(h, ft) as c,
    ):
        c.get(URL)
    lines = [json.loads(r.message) for r in caplog.records]
    assert lines == [
        {"attempt": 1, "status": 503, "delay": 1.0, "url": URL},
        {"attempt": 2, "status": 200, "delay": 0, "url": URL},
    ]


def test_max_attempts_must_be_positive():
    with pytest.raises(ValueError):
        HttpClient(user_agent="test-agent", max_attempts=0)
