import json
from datetime import UTC, date, datetime

import httpx
import pytest

from ingestion.http import HttpClient, HttpError
from ingestion.ptax import (
    fetch_window,
    incremental,
    main,
    olinda_date,
    read_watermark,
    row_date,
    sync,
    window_path,
)

NOW = datetime(2026, 11, 23, 9, 17, tzinfo=UTC)


def row(timestamp: str, value: float = 5.5) -> dict:
    return {
        "cotacaoCompra": value,
        "cotacaoVenda": value + 0.0006,
        "dataHoraCotacao": timestamp,
    }


def make_client(handler) -> HttpClient:
    return HttpClient(
        user_agent="test-agent",
        sleep=lambda s: None,
        transport=httpx.MockTransport(handler),
    )


def fake_api(data: dict[int, list[dict]]):
    """Handler serving data[year]; a missing year is a 200 with no rows, like BACEN."""
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        year = int(request.url.params["@dataInicial"].strip("'")[-4:])
        return httpx.Response(200, json={"value": data.get(year, [])})

    handler.requests = requests
    return handler


def saved(out, year: int) -> dict:
    return json.loads((out / f"ptax/year={year}.json").read_text())


def landing_files(out) -> list[str]:
    return sorted(str(p.relative_to(out)) for p in out.rglob("*.json"))


def test_olinda_date_is_month_first_and_quoted():
    assert olinda_date(date(2024, 2, 29)) == "'02-29-2024'"
    assert olinda_date(date(2025, 12, 1)) == "'12-01-2025'"


def test_row_date_reads_the_timestamp_day():
    assert row_date(row("2025-12-31 10:06:24.183")) == date(2025, 12, 31)
    assert row_date(row("1984-12-03 11:29:00.0")) == date(1984, 12, 3)


def test_window_path_is_fixed_by_year():
    assert str(window_path(date(2024, 1, 1))) == "ptax/year=2024.json"


def test_fetch_window_sends_the_documented_query():
    h = fake_api({2024: [row("2024-01-02 13:09:42.489")]})
    with make_client(h) as c:
        fetch_window(c, date(2024, 1, 1), date(2024, 12, 31))
    url = h.requests[0].url
    assert url.scheme == "https"
    assert url.host == "olinda.bcb.gov.br"
    assert url.path.endswith(
        "/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    )
    assert dict(url.params) == {
        "@dataInicial": "'01-01-2024'",
        "@dataFinalCotacao": "'12-31-2024'",
        "$format": "json",
    }


def test_weekend_or_holiday_is_an_empty_list_not_an_error():
    with make_client(fake_api({})) as c:
        assert fetch_window(c, date(2025, 11, 22), date(2025, 11, 23)) == []


def test_server_error_is_retried_then_raised():
    h = lambda request: httpx.Response(500, text='{"codigo": 500}')
    with make_client(h) as c, pytest.raises(HttpError) as e:
        fetch_window(c, date(2025, 1, 1), date(2025, 12, 31))
    assert e.value.status == 500
    assert e.value.attempts == c.max_attempts


def test_redirect_is_an_error():
    h = lambda request: httpx.Response(307, headers={"Location": "https://x"})
    with make_client(h) as c, pytest.raises(HttpError):
        fetch_window(c, date(2025, 1, 1), date(2025, 12, 31))


def test_sync_writes_one_wrapped_file_per_year(tmp_path):
    payload_2024 = [row("2024-12-30 13:06:26.618"), row("2024-12-31 10:04:09.264")]
    payload_2025 = [row("2025-01-02 13:09:42.489")]
    h = fake_api({2024: payload_2024, 2025: payload_2025})
    with make_client(h) as c:
        sync(c, date(2024, 12, 31), date(2025, 1, 2), tmp_path, "run-1", NOW)

    assert landing_files(tmp_path) == [
        "_state/ptax.json",
        "ptax/year=2024.json",
        "ptax/year=2025.json",
    ]
    assert saved(tmp_path, 2024) == {
        "source": "bcb_ptax",
        "window_start": "2024-01-01",
        "window_end": "2024-12-31",
        "ingested_at": NOW.isoformat(),
        "run_id": "run-1",
        "payload": payload_2024,
    }
    assert read_watermark(tmp_path) == "2025-01-02"


def test_sync_skips_empty_years(tmp_path):
    h = fake_api({2025: [row("2025-01-02 13:09:42.489")]})
    with make_client(h) as c:
        sync(c, date(1983, 1, 1), date(2025, 1, 1), tmp_path, "run-1", NOW)
    assert landing_files(tmp_path) == ["_state/ptax.json", "ptax/year=2025.json"]


def test_no_data_at_all_writes_nothing(tmp_path):
    with make_client(fake_api({})) as c:
        sync(c, date(1980, 1, 1), date(1980, 12, 31), tmp_path, "run-1", NOW)
    assert landing_files(tmp_path) == []


def test_same_window_twice_overwrites_the_same_file(tmp_path):
    h = fake_api({2024: [row("2024-01-02 13:09:42.489")]})
    with make_client(h) as c:
        sync(c, date(2024, 1, 1), date(2024, 12, 31), tmp_path, "run-1", NOW)
        first = landing_files(tmp_path)
        sync(c, date(2024, 3, 1), date(2024, 3, 1), tmp_path, "run-2", NOW)
    assert landing_files(tmp_path) == first
    assert saved(tmp_path, 2024)["run_id"] == "run-2"


def test_leap_day_stays_in_its_year(tmp_path):
    h = fake_api({2024: [row("2024-02-29 13:06:00.000")]})
    with make_client(h) as c:
        sync(c, date(2024, 2, 29), date(2024, 2, 29), tmp_path, "run-1", NOW)
    assert [r.url.params["@dataFinalCotacao"] for r in h.requests] == ["'12-31-2024'"]
    assert read_watermark(tmp_path) == "2024-02-29"


def test_watermark_ignores_rows_after_today_and_never_moves_back(tmp_path):
    h = fake_api(
        {
            2026: [row("2026-11-19 13:04:18.325"), row("2026-12-09 13:00:00.000")],
            2020: [row("2020-01-02 13:00:00.000")],
        }
    )
    with make_client(h) as c:
        sync(c, date(2026, 1, 1), date(2026, 12, 31), tmp_path, "run-1", NOW)
        assert read_watermark(tmp_path) == "2026-11-19"
        sync(c, date(2020, 1, 1), date(2020, 12, 31), tmp_path, "run-2", NOW)
    assert read_watermark(tmp_path) == "2026-11-19"


def test_duplicate_rows_for_a_day_are_kept_raw(tmp_path):
    # 2025-04-23 has two closing rows 27 ms apart. Bronze keeps both.
    rows = [row("2025-04-23 13:06:30.416"), row("2025-04-23 13:06:30.443")]
    with make_client(fake_api({2025: rows})) as c:
        sync(c, date(2025, 1, 1), date(2025, 12, 31), tmp_path, "run-1", NOW)
    assert saved(tmp_path, 2025)["payload"] == rows


def test_incremental_starts_at_watermark_minus_lookback(tmp_path):
    h = fake_api({2026: [row("2026-11-19 13:04:18.325")]})
    with make_client(h) as c:
        sync(c, date(2026, 1, 1), date(2026, 12, 31), tmp_path, "run-1", NOW)
        h.requests.clear()
        incremental(c, tmp_path, "run-2", NOW)
    assert len(h.requests) == 1
    assert saved(tmp_path, 2026)["run_id"] == "run-2"


def test_incremental_picks_up_a_revised_value(tmp_path):
    with make_client(fake_api({2026: [row("2026-11-19 13:04:18.325", 5.5)]})) as c:
        sync(c, date(2026, 1, 1), date(2026, 12, 31), tmp_path, "run-1", NOW)
    with make_client(fake_api({2026: [row("2026-11-19 13:04:18.325", 5.6)]})) as c:
        incremental(c, tmp_path, "run-2", NOW)
    assert saved(tmp_path, 2026)["payload"][0]["cotacaoCompra"] == 5.6


def test_incremental_across_new_year_refetches_the_previous_year(tmp_path):
    jan = datetime(2026, 1, 5, tzinfo=UTC)
    h = fake_api(
        {
            2025: [row("2025-12-30 13:00:00.000")],
            2026: [row("2026-01-02 13:00:00.000")],
        }
    )
    with make_client(h) as c:
        sync(c, date(2025, 1, 1), date(2026, 1, 5), tmp_path, "run-1", jan)
        h.requests.clear()
        incremental(c, tmp_path, "run-2", jan)
    # Jan 2 minus 10 days is Dec 23, so both years are fetched.
    assert sorted(r.url.params["@dataInicial"] for r in h.requests) == [
        "'01-01-2025'",
        "'01-01-2026'",
    ]


def test_incremental_without_a_watermark_asks_for_a_backfill(tmp_path):
    with make_client(fake_api({})) as c, pytest.raises(SystemExit) as e:
        incremental(c, tmp_path, "run-1", NOW)
    assert "backfill" in str(e.value)


def test_backfill_keeps_the_sgs_state_file_alone(tmp_path):
    sgs_state = tmp_path / "_state/sgs.json"
    sgs_state.parent.mkdir()
    sgs_state.write_text('{"11": "2026-11-20"}')
    with make_client(fake_api({2024: [row("2024-01-02 13:00:00.000")]})) as c:
        sync(c, date(2024, 1, 1), date(2024, 12, 31), tmp_path, "run-1", NOW)
    assert sgs_state.read_text() == '{"11": "2026-11-20"}'


def test_cli_backfill_then_incremental_against_mocked_http(tmp_path, monkeypatch):
    h = fake_api({2024: [row("2024-12-02 13:00:00.000")]})
    real_client = HttpClient
    monkeypatch.setattr(
        "ingestion.ptax.HttpClient",
        lambda user_agent, **kw: real_client(
            user_agent, transport=httpx.MockTransport(h), sleep=lambda s: None
        ),
    )
    out = ["--out", str(tmp_path)]

    main([*out, "backfill", "--from", "2024-01-01", "--to", "2024-12-31"])
    assert landing_files(tmp_path) == ["_state/ptax.json", "ptax/year=2024.json"]
    assert saved(tmp_path, 2024)["source"] == "bcb_ptax"
    assert read_watermark(tmp_path) == "2024-12-02"

    main([*out, "incremental"])
    assert read_watermark(tmp_path) == "2024-12-02"
