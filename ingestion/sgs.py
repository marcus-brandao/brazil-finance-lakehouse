import argparse
import json
import logging
import tomllib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from ingestion.http import HttpClient, HttpError

# Always https: the http:// URL answers 307 with an empty body.
URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
CONFIG = Path(__file__).parent / "config" / "sgs.toml"
USER_AGENT = "brazil-finance-lakehouse/0.1 (sgs connector)"
STATE_FILE = "_state/sgs.json"


def load_series(path: Path = CONFIG) -> list[dict]:
    with path.open("rb") as f:
        return tomllib.load(f)["series"]


def parse_date(text: str) -> date:
    return datetime.strptime(text, "%d/%m/%Y").date()


def year_windows(start: date, end: date) -> list[tuple[date, date]]:
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return [(date(y, 1, 1), date(y, 12, 31)) for y in range(start.year, end.year + 1)]


def window_path(code: int, window_start: date) -> Path:
    return Path(f"sgs/series_code={code}/year={window_start.year}.json")


def fetch_range(
    client: HttpClient, code: int, start: date, end: date
) -> list[dict] | None:
    params = {
        "formato": "json",
        "dataInicial": start.strftime("%d/%m/%Y"),
        "dataFinal": end.strftime("%d/%m/%Y"),
    }
    try:
        return client.get(URL.format(code=code), params=params).json()
    except HttpError as e:
        # An empty window is a 404 too, so only this body means "no data".
        # Any other 404 is an unknown series and must fail.
        if e.status == 404 and "Value(s) not found" in e.body:
            return None
        raise


def fetch_window(
    client: HttpClient, code: int, window_start: date, window_end: date
) -> list[dict] | None:
    try:
        return fetch_range(client, code, window_start, window_end)
    except HttpError as e:
        if e.status != 304:
            raise
    # BCB answers some full-year requests with a 304 and no body, although we
    # send no conditional headers (432 and 1 for 2026). The two half years
    # answer 200. A 304 on a half raises. See ADR-0002.
    mid_year = date(window_start.year, 6, 30)
    first = fetch_range(client, code, window_start, mid_year)
    second = fetch_range(client, code, mid_year + timedelta(days=1), window_end)
    rows = (first or []) + (second or [])
    return rows or None


def write_window(
    out: Path,
    code: int,
    window: tuple[date, date],
    payload: list[dict],
    run_id: str,
    now: datetime,
) -> None:
    envelope = {
        "source": "bcb_sgs",
        "series_code": code,
        "window_start": window[0].isoformat(),
        "window_end": window[1].isoformat(),
        "ingested_at": now.isoformat(),
        "run_id": run_id,
        "payload": payload,
    }
    path = out / window_path(code, window[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope))


def read_state(out: Path) -> dict[str, str]:
    path = out / STATE_FILE
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def write_state(out: Path, state: dict[str, str]) -> None:
    path = out / STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def sync_series(
    client: HttpClient,
    series: dict,
    start: date,
    end: date,
    out: Path,
    run_id: str,
    now: datetime,
) -> None:
    code = series["code"]
    today = now.date()
    seen = []
    for window in year_windows(start, end):
        payload = fetch_window(client, code, *window)
        if payload is None:
            continue
        write_window(out, code, window, payload, run_id, now)
        # Series 432 carries rows for days after today (the target stays in force),
        # so the watermark ignores them.
        seen += [d for row in payload if (d := parse_date(row["data"])) <= today]

    if seen:
        state = read_state(out)
        state[str(code)] = max(max(seen).isoformat(), state.get(str(code), ""))
        write_state(out, state)


def backfill(
    client: HttpClient,
    series_list: list[dict],
    start: date,
    end: date,
    out: Path,
    run_id: str,
    now: datetime,
) -> None:
    for series in series_list:
        sync_series(client, series, start, end, out, run_id, now)


def incremental(
    client: HttpClient,
    series_list: list[dict],
    out: Path,
    run_id: str,
    now: datetime,
) -> None:
    state = read_state(out)
    for series in series_list:
        watermark = state.get(str(series["code"]))
        if watermark is None:
            raise SystemExit(f"no watermark for series {series['code']}, run backfill")
        start = date.fromisoformat(watermark) - timedelta(days=series["lookback_days"])
        sync_series(client, series, start, now.date(), out, run_id, now)


def select_series(series_list: list[dict], code: int | None) -> list[dict]:
    if code is None:
        return series_list
    chosen = [s for s in series_list if s["code"] == code]
    if not chosen:
        known = ", ".join(str(s["code"]) for s in series_list)
        raise SystemExit(f"series {code} is not in the config (known: {known})")
    return chosen


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m ingestion.sgs")
    parser.add_argument("--out", type=Path, default=Path("out"))
    parser.add_argument("--series", type=int, help="only this series code")
    modes = parser.add_subparsers(dest="mode", required=True)
    backfill_parser = modes.add_parser("backfill")
    backfill_parser.add_argument(
        "--from", dest="start", type=date.fromisoformat, required=True
    )
    backfill_parser.add_argument("--to", dest="end", type=date.fromisoformat)
    modes.add_parser("incremental")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    now = datetime.now(UTC)
    run_id = uuid4().hex
    series_list = select_series(load_series(), args.series)
    with HttpClient(USER_AGENT, min_interval=0.5) as client:
        if args.mode == "backfill":
            end = args.end or now.date()
            backfill(client, series_list, args.start, end, args.out, run_id, now)
        else:
            incremental(client, series_list, args.out, run_id, now)


if __name__ == "__main__":
    main()
