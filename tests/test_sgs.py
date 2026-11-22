import json
from datetime import UTC, date, datetime

import httpx
import pytest

from ingestion.http import HttpClient, HttpError
from ingestion.sgs import (
    backfill,
    fetch_window,
    incremental,
    load_series,
    main,
    parse_date,
    read_state,
    window_path,
    year_windows,
)

NOW = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
EMPTY = {
    "erro": {"statusCode": 404, "detail": "SGSNegocioException: Value(s) not found"}
}
UNKNOWN = {"erro": {"statusCode": 404, "detail": "A serie nao existe!"}}

DAILY = {"code": 11, "name": "daily", "lookback_days": 10}
MONTHLY = {"code": 433, "name": "monthly", "lookback_days": 95}


def row(day: str, value: str = "0.05") -> dict:
    return {"data": day, "valor": value}


def make_client(handler) -> HttpClient:
    return HttpClient(
        user_agent="test-agent",
        sleep=lambda s: None,
        transport=httpx.MockTransport(handler),
    )


def fake_api(data: dict[tuple[int, int], list[dict]]):
    """Handler serving data[(code, year)]; a missing key is an empty window."""
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        code = int(request.url.path.split("bcdata.sgs.")[1].split("/")[0])
        start = request.url.params["dataInicial"]
        year = int(start[-4:])
        if (code, year) not in data:
            return httpx.Response(404, json=EMPTY)
        return httpx.Response(200, json=data[(code, year)])

    handler.requests = requests
    return handler


def saved(out, code: int, year: int) -> dict:
    return json.loads((out / f"sgs/series_code={code}/year={year}.json").read_text())


def landing_files(out) -> list[str]:
    return sorted(str(p.relative_to(out)) for p in out.rglob("*.json"))


def test_year_windows_split_a_range_into_calendar_years():
    assert year_windows(date(2023, 6, 15), date(2025, 2, 1)) == [
        (date(2023, 1, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_year_windows_single_day_is_its_whole_year():
    assert year_windows(date(2024, 5, 10), date(2024, 5, 10)) == [
        (date(2024, 1, 1), date(2024, 12, 31))
    ]


def test_year_windows_leap_day_stays_in_its_year():
    assert year_windows(date(2024, 2, 29), date(2024, 2, 29)) == [
        (date(2024, 1, 1), date(2024, 12, 31))
    ]
    # Dec 31 of a leap year is day 366 and does not spill into the next year.
    assert year_windows(date(2024, 12, 31), date(2024, 12, 31)) == [
        (date(2024, 1, 1), date(2024, 12, 31))
    ]


def test_year_windows_cross_the_year_boundary():
    assert year_windows(date(2024, 12, 31), date(2025, 1, 1)) == [
        (date(2024, 1, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_year_windows_reject_a_backwards_range():
    with pytest.raises(ValueError):
        year_windows(date(2025, 1, 1), date(2024, 12, 31))


def test_parse_date_reads_day_first():
    assert parse_date("29/02/2024") == date(2024, 2, 29)
    assert parse_date("01/12/2025") == date(2025, 12, 1)
    with pytest.raises(ValueError):
        parse_date("29/02/2023")


def test_window_path_is_fixed_by_series_and_year():
    assert (
        str(window_path(432, date(2024, 1, 1))) == "sgs/series_code=432/year=2024.json"
    )


def test_config_lists_the_four_series_with_a_lookback():
    series = load_series()
    assert [s["code"] for s in series] == [432, 11, 12, 433]
    assert all(s["lookback_days"] > 0 for s in series)
    assert next(s for s in series if s["code"] == 433)["lookback_days"] >= 90


def test_fetch_window_sends_the_documented_query():
    h = fake_api({(11, 2024): [row("02/01/2024")]})
    with make_client(h) as c:
        fetch_window(c, 11, date(2024, 1, 1), date(2024, 12, 31))
    url = h.requests[0].url
    assert url.scheme == "https"
    assert url.host == "api.bcb.gov.br"
    assert url.path == "/dados/serie/bcdata.sgs.11/dados"
    assert dict(url.params) == {
        "formato": "json",
        "dataInicial": "01/01/2024",
        "dataFinal": "31/12/2024",
    }


def test_empty_window_is_none_not_an_error():
    h = fake_api({})
    with make_client(h) as c:
        assert fetch_window(c, 11, date(1990, 1, 1), date(1990, 12, 31)) is None


def test_unknown_series_raises():
    h = lambda request: httpx.Response(404, json=UNKNOWN)
    with make_client(h) as c, pytest.raises(HttpError) as e:
        fetch_window(c, 999999, date(2024, 1, 1), date(2024, 12, 31))
    assert e.value.status == 404
    assert "A serie nao existe!" in e.value.body


def test_window_over_the_cap_raises():
    h = lambda request: httpx.Response(406, text="periodo maior que 10 anos")
    with make_client(h) as c, pytest.raises(HttpError) as e:
        fetch_window(c, 11, date(2000, 1, 1), date(2024, 12, 31))
    assert e.value.status == 406


def test_backfill_writes_one_wrapped_file_per_series_and_year(tmp_path):
    payload_2024 = [row("02/01/2024", "0.043739"), row("29/02/2024", "0.041957")]
    payload_2025 = [row("02/01/2025", "0.045513")]
    h = fake_api({(11, 2024): payload_2024, (11, 2025): payload_2025})
    with make_client(h) as c:
        backfill(
            c, [DAILY], date(2024, 2, 29), date(2025, 1, 1), tmp_path, "run-1", NOW
        )

    assert landing_files(tmp_path) == [
        "_state/sgs.json",
        "sgs/series_code=11/year=2024.json",
        "sgs/series_code=11/year=2025.json",
    ]
    assert saved(tmp_path, 11, 2024) == {
        "source": "bcb_sgs",
        "series_code": 11,
        "window_start": "2024-01-01",
        "window_end": "2024-12-31",
        "ingested_at": NOW.isoformat(),
        "run_id": "run-1",
        "payload": payload_2024,
    }
    assert saved(tmp_path, 11, 2025)["payload"] == payload_2025
    assert read_state(tmp_path) == {"11": "2025-01-02"}


def test_backfill_skips_empty_years_without_failing(tmp_path):
    h = fake_api({(11, 2025): [row("02/01/2025")]})
    with make_client(h) as c:
        backfill(c, [DAILY], date(2024, 1, 1), date(2025, 1, 1), tmp_path, "run-1", NOW)
    assert landing_files(tmp_path) == [
        "_state/sgs.json",
        "sgs/series_code=11/year=2025.json",
    ]


def test_backfill_with_no_data_at_all_writes_nothing(tmp_path):
    with make_client(fake_api({})) as c:
        backfill(
            c, [DAILY], date(1990, 1, 1), date(1990, 12, 31), tmp_path, "run-1", NOW
        )
    assert landing_files(tmp_path) == []


def test_unknown_series_stops_the_backfill(tmp_path):
    h = lambda request: httpx.Response(404, json=UNKNOWN)
    with make_client(h) as c, pytest.raises(HttpError):
        backfill(
            c, [DAILY], date(2024, 1, 1), date(2024, 12, 31), tmp_path, "run-1", NOW
        )
    assert landing_files(tmp_path) == []


def test_same_window_twice_overwrites_the_same_file(tmp_path):
    h = fake_api({(11, 2024): [row("02/01/2024")]})
    with make_client(h) as c:
        backfill(
            c, [DAILY], date(2024, 1, 1), date(2024, 12, 31), tmp_path, "run-1", NOW
        )
        first = landing_files(tmp_path)
        backfill(c, [DAILY], date(2024, 3, 1), date(2024, 3, 1), tmp_path, "run-2", NOW)
    assert landing_files(tmp_path) == first
    assert saved(tmp_path, 11, 2024)["run_id"] == "run-2"


def test_watermark_ignores_rows_after_today_and_never_moves_back(tmp_path):
    # Series 432 has rows for days that have not happened yet.
    h = fake_api(
        {
            (11, 2026): [row("20/11/2026"), row("09/12/2026")],
            (11, 2020): [row("02/01/2020")],
        }
    )
    with make_client(h) as c:
        backfill(
            c, [DAILY], date(2026, 1, 1), date(2026, 12, 31), tmp_path, "run-1", NOW
        )
        assert read_state(tmp_path) == {"11": "2026-11-20"}
        backfill(
            c, [DAILY], date(2020, 1, 1), date(2020, 12, 31), tmp_path, "run-2", NOW
        )
    assert read_state(tmp_path) == {"11": "2026-11-20"}


def test_incremental_starts_at_watermark_minus_lookback(tmp_path):
    h = fake_api({(11, 2026): [row("20/11/2026")]})
    with make_client(h) as c:
        backfill(
            c, [DAILY], date(2026, 1, 1), date(2026, 12, 31), tmp_path, "run-1", NOW
        )
        h.requests.clear()
        incremental(c, [DAILY], tmp_path, "run-2", NOW)
    assert len(h.requests) == 1  # Nov 10 is still 2026, so one window
    assert h.requests[0].url.params["dataInicial"] == "01/01/2026"
    assert saved(tmp_path, 11, 2026)["run_id"] == "run-2"


def test_incremental_picks_up_revised_values_through_the_lookback(tmp_path):
    first = {(433, 2026): [row("01/09/2026", "0.82"), row("01/10/2026", "0.14")]}
    with make_client(fake_api(first)) as c:
        backfill(
            c, [MONTHLY], date(2026, 1, 1), date(2026, 12, 31), tmp_path, "run-1", NOW
        )

    revised = {(433, 2026): [row("01/09/2026", "0.85"), row("01/10/2026", "0.14")]}
    with make_client(fake_api(revised)) as c:
        incremental(c, [MONTHLY], tmp_path, "run-2", NOW)

    values = {r["data"]: r["valor"] for r in saved(tmp_path, 433, 2026)["payload"]}
    assert values["01/09/2026"] == "0.85"
    assert landing_files(tmp_path) == [
        "_state/sgs.json",
        "sgs/series_code=433/year=2026.json",
    ]


def test_incremental_lookback_across_new_year_refetches_the_previous_year(tmp_path):
    jan = datetime(2026, 1, 5, tzinfo=UTC)
    h = fake_api({(11, 2025): [row("30/12/2025")], (11, 2026): [row("02/01/2026")]})
    with make_client(h) as c:
        backfill(c, [DAILY], date(2025, 1, 1), date(2026, 1, 5), tmp_path, "run-1", jan)
        h.requests.clear()
        incremental(c, [DAILY], tmp_path, "run-2", jan)
    # Watermark Jan 2 minus 10 days is Dec 23, so both years are fetched.
    assert sorted(r.url.params["dataInicial"] for r in h.requests) == [
        "01/01/2025",
        "01/01/2026",
    ]


def test_incremental_without_a_watermark_asks_for_a_backfill(tmp_path):
    with make_client(fake_api({})) as c, pytest.raises(SystemExit) as e:
        incremental(c, [DAILY], tmp_path, "run-1", NOW)
    assert "backfill" in str(e.value)


def test_main_runs_a_backfill_from_the_command_line(tmp_path, monkeypatch):
    h = fake_api({(11, 2024): [row("02/01/2024")]})
    real_client = HttpClient
    monkeypatch.setattr(
        "ingestion.sgs.HttpClient",
        lambda user_agent, **kw: real_client(
            user_agent, transport=httpx.MockTransport(h), sleep=lambda s: None
        ),
    )
    monkeypatch.setattr("ingestion.sgs.load_series", lambda: [DAILY])
    main(
        [
            "--out",
            str(tmp_path),
            "backfill",
            "--from",
            "2024-01-01",
            "--to",
            "2024-12-31",
        ]
    )
    assert "sgs/series_code=11/year=2024.json" in landing_files(tmp_path)
