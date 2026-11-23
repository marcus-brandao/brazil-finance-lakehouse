import io
import zipfile
from datetime import UTC, date, datetime

import httpx
import pytest

from ingestion.cvm import (
    STATE_FILE,
    add_months,
    fetch_month,
    incremental,
    main,
    month_windows,
    sync,
    window_path,
)
from ingestion.http import HttpClient, HttpError
from ingestion.state import read_state

NOW = datetime(2026, 11, 23, 10, 47, tzinfo=UTC)

# Headers as CVM publishes them, see docs/ingest.md for where each one is used.
HEADER_B = "TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST"
HEADER_C = (
    "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;"
    "VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST"
)


def csv(header: str, *rows: str) -> bytes:
    return "\r\n".join([header, *rows, ""]).encode()


def zipped(month: date, body: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"inf_diario_fi_{month:%Y%m}.csv", body)
    return buf.getvalue()


def make_client(handler) -> HttpClient:
    return HttpClient(
        user_agent="test-agent",
        sleep=lambda s: None,
        transport=httpx.MockTransport(handler),
    )


def fake_portal(files: dict[str, bytes]):
    """Serves files["YYYYMM"] zipped; a missing month is a 404, like the portal."""
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        yyyymm = request.url.path.removesuffix(".zip")[-6:]
        if yyyymm not in files:
            return httpx.Response(404, text="Not Found")
        month = date(int(yyyymm[:4]), int(yyyymm[4:]), 1)
        return httpx.Response(200, content=zipped(month, files[yyyymm]))

    handler.requests = requests
    return handler


def landing_files(out) -> list[str]:
    return sorted(str(p.relative_to(out)) for p in out.rglob("*") if p.is_file())


def requested_months(handler) -> list[str]:
    return [r.url.path.removesuffix(".zip")[-6:] for r in handler.requests]


def test_add_months_crosses_year_ends():
    assert add_months(date(2026, 12, 1), 1) == date(2027, 1, 1)
    assert add_months(date(2026, 1, 1), -1) == date(2025, 12, 1)
    assert add_months(date(2024, 2, 1), 12) == date(2025, 2, 1)


def test_month_windows_cover_whole_months():
    assert month_windows(date(2025, 11, 30), date(2026, 2, 1)) == [
        date(2025, 11, 1),
        date(2025, 12, 1),
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]
    assert month_windows(date(2024, 2, 29), date(2024, 2, 1)) == [date(2024, 2, 1)]


def test_month_windows_reject_bad_ranges():
    with pytest.raises(ValueError):
        month_windows(date(2026, 2, 1), date(2026, 1, 31))
    with pytest.raises(SystemExit, match="2021-01"):
        month_windows(date(2020, 12, 1), date(2021, 1, 31))


def test_window_path_is_fixed_by_month():
    assert str(window_path(date(2026, 11, 1))) == "cvm_inf_diario/month=2026-11.csv"


def test_fetch_month_asks_for_the_monthly_zip_and_returns_the_csv():
    body = csv(HEADER_C, "CLASSES - FIF;00.017.024/0001-53;;2026-11-03;1;1;1;0;0;1")
    h = fake_portal({"202611": body})
    with make_client(h) as c:
        assert fetch_month(c, date(2026, 11, 1)) == body
    url = h.requests[0].url
    assert str(url) == (
        "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202611.zip"
    )


def test_fetch_month_returns_none_on_404():
    with make_client(fake_portal({})) as c:
        assert fetch_month(c, date(2026, 12, 1)) is None


def test_server_error_is_retried_then_raised():
    h = lambda request: httpx.Response(503, text="busy")
    with make_client(h) as c, pytest.raises(HttpError) as e:
        fetch_month(c, date(2026, 11, 1))
    assert e.value.attempts == c.max_attempts


def test_old_and_new_layouts_land_byte_for_byte(tmp_path):
    old = csv(HEADER_B, "FI;00.017.024/0001-53;2023-11-01;1;1;1;0;0;1")
    new = csv(HEADER_C, "FI;00.017.024/0001-53;;2023-12-01;1;1;1;0;0;1")
    with make_client(fake_portal({"202311": old, "202312": new})) as c:
        sync(c, date(2023, 11, 1), date(2023, 12, 31), tmp_path, NOW)

    assert landing_files(tmp_path) == [
        "_state/cvm.json",
        "cvm_inf_diario/month=2023-11.csv",
        "cvm_inf_diario/month=2023-12.csv",
    ]
    assert (tmp_path / "cvm_inf_diario/month=2023-11.csv").read_bytes() == old
    assert (tmp_path / "cvm_inf_diario/month=2023-12.csv").read_bytes() == new
    assert read_state(tmp_path, STATE_FILE) == {"inf_diario": "2023-12"}


def test_header_only_file_still_lands(tmp_path):
    with make_client(fake_portal({"202611": csv(HEADER_C)})) as c:
        sync(c, date(2026, 11, 1), date(2026, 11, 23), tmp_path, NOW)
    assert (tmp_path / "cvm_inf_diario/month=2026-11.csv").read_bytes() == csv(HEADER_C)


def test_current_month_not_published_yet_is_skipped(tmp_path):
    dec_1 = datetime(2026, 12, 1, 10, 47, tzinfo=UTC)
    h = fake_portal({"202611": csv(HEADER_C)})
    with make_client(h) as c:
        sync(c, date(2026, 11, 1), dec_1.date(), tmp_path, dec_1)
    assert requested_months(h) == ["202611", "202612"]
    assert read_state(tmp_path, STATE_FILE) == {"inf_diario": "2026-11"}


def test_a_missing_past_month_fails(tmp_path):
    with make_client(fake_portal({})) as c, pytest.raises(SystemExit, match="2026-10"):
        sync(c, date(2026, 10, 1), date(2026, 10, 31), tmp_path, NOW)
    assert landing_files(tmp_path) == []


def test_same_month_twice_overwrites_the_same_file(tmp_path):
    with make_client(fake_portal({"202611": csv(HEADER_C, "a")})) as c:
        sync(c, date(2026, 11, 1), date(2026, 11, 23), tmp_path, NOW)
    first = landing_files(tmp_path)
    with make_client(fake_portal({"202611": csv(HEADER_C, "a", "b")})) as c:
        sync(c, date(2026, 11, 1), date(2026, 11, 23), tmp_path, NOW)
    assert landing_files(tmp_path) == first
    assert (tmp_path / "cvm_inf_diario/month=2026-11.csv").read_bytes() == csv(
        HEADER_C, "a", "b"
    )


def test_watermark_never_moves_back(tmp_path):
    files = {"202611": csv(HEADER_C), "202301": csv(HEADER_B)}
    with make_client(fake_portal(files)) as c:
        sync(c, date(2026, 11, 1), date(2026, 11, 23), tmp_path, NOW)
        sync(c, date(2023, 1, 1), date(2023, 1, 31), tmp_path, NOW)
    assert read_state(tmp_path, STATE_FILE) == {"inf_diario": "2026-11"}


def test_incremental_refetches_the_month_before_the_watermark(tmp_path):
    files = {"202610": csv(HEADER_C), "202611": csv(HEADER_C)}
    h = fake_portal(files)
    with make_client(h) as c:
        sync(c, date(2026, 11, 1), date(2026, 11, 23), tmp_path, NOW)
        h.requests.clear()
        incremental(c, tmp_path, NOW)
    assert requested_months(h) == ["202610", "202611"]


def test_incremental_across_new_year(tmp_path):
    jan_4 = datetime(2027, 1, 4, 10, 47, tzinfo=UTC)
    files = {"202611": csv(HEADER_C), "202612": csv(HEADER_C), "202701": csv(HEADER_C)}
    h = fake_portal(files)
    with make_client(h) as c:
        sync(c, date(2026, 12, 1), date(2026, 12, 31), tmp_path, NOW)
        h.requests.clear()
        incremental(c, tmp_path, jan_4)
    assert requested_months(h) == ["202611", "202612", "202701"]
    assert read_state(tmp_path, STATE_FILE) == {"inf_diario": "2027-01"}


def test_incremental_without_a_watermark_asks_for_a_backfill(tmp_path):
    with make_client(fake_portal({})) as c, pytest.raises(SystemExit) as e:
        incremental(c, tmp_path, NOW)
    assert "backfill" in str(e.value)


def test_backfill_keeps_the_other_state_files_alone(tmp_path):
    sgs_state = tmp_path / "_state/sgs.json"
    sgs_state.parent.mkdir()
    sgs_state.write_text('{"11": "2026-11-20"}')
    with make_client(fake_portal({"202611": csv(HEADER_C)})) as c:
        sync(c, date(2026, 11, 1), date(2026, 11, 23), tmp_path, NOW)
    assert sgs_state.read_text() == '{"11": "2026-11-20"}'


def test_cli_backfill_then_incremental_against_mocked_http(tmp_path, monkeypatch):
    files = {"202310": csv(HEADER_B), "202311": csv(HEADER_B), "202312": csv(HEADER_C)}
    h = fake_portal(files)
    real_client = HttpClient
    monkeypatch.setattr(
        "ingestion.cvm.HttpClient",
        lambda user_agent, **kw: real_client(
            user_agent, transport=httpx.MockTransport(h), sleep=lambda s: None
        ),
    )
    out = ["--out", str(tmp_path)]

    main([*out, "backfill", "--from", "2023-11-15", "--to", "2023-12-02"])
    assert requested_months(h) == ["202311", "202312"]
    assert read_state(tmp_path, STATE_FILE) == {"inf_diario": "2023-12"}
