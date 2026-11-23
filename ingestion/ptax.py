import argparse
import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from ingestion.http import HttpClient
from ingestion.sgs import year_windows

# Closing PTAX for USD, one row per business day. Always https: http:// gives a 404 page.
URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
)
USER_AGENT = "brazil-finance-lakehouse/0.1 (ptax connector)"
STATE_FILE = "_state/ptax.json"
STATE_KEY = "usd"
LOOKBACK_DAYS = 10


def olinda_date(day: date) -> str:
    # The API wants MM-DD-YYYY in single quotes. An ISO date is not an error, it
    # returns 200 with no rows, so a wrong format would look like "no data".
    return f"'{day.strftime('%m-%d-%Y')}'"


def row_date(row: dict) -> date:
    # dataHoraCotacao looks like "2025-12-31 10:06:24.183".
    return date.fromisoformat(row["dataHoraCotacao"][:10])


def window_path(window_start: date) -> Path:
    return Path(f"ptax/year={window_start.year}.json")


def fetch_window(client: HttpClient, start: date, end: date) -> list[dict]:
    params = {
        "@dataInicial": olinda_date(start),
        "@dataFinalCotacao": olinda_date(end),
        "$format": "json",
    }
    # No data (weekend, holiday, before 1984) is a 200 with an empty list.
    return client.get(URL, params=params).json()["value"]


def write_window(
    out: Path,
    window: tuple[date, date],
    payload: list[dict],
    run_id: str,
    now: datetime,
) -> None:
    envelope = {
        "source": "bcb_ptax",
        "window_start": window[0].isoformat(),
        "window_end": window[1].isoformat(),
        "ingested_at": now.isoformat(),
        "run_id": run_id,
        "payload": payload,
    }
    path = out / window_path(window[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope))


def read_watermark(out: Path) -> str | None:
    path = out / STATE_FILE
    if not path.exists():
        return None
    return json.loads(path.read_text()).get(STATE_KEY)


def write_watermark(out: Path, watermark: str) -> None:
    path = out / STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({STATE_KEY: watermark}, indent=2))


def sync(
    client: HttpClient, start: date, end: date, out: Path, run_id: str, now: datetime
) -> None:
    today = now.date()
    seen = []
    for window in year_windows(start, end):
        payload = fetch_window(client, *window)
        if not payload:
            continue
        write_window(out, window, payload, run_id, now)
        seen += [d for row in payload if (d := row_date(row)) <= today]

    if seen:
        # A backfill of old years never moves the watermark back.
        watermark = max(max(seen).isoformat(), read_watermark(out) or "")
        write_watermark(out, watermark)


def incremental(client: HttpClient, out: Path, run_id: str, now: datetime) -> None:
    watermark = read_watermark(out)
    if watermark is None:
        raise SystemExit("no PTAX watermark, run backfill")
    start = date.fromisoformat(watermark) - timedelta(days=LOOKBACK_DAYS)
    sync(client, start, now.date(), out, run_id, now)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m ingestion.ptax")
    parser.add_argument("--out", type=Path, default=Path("out"))
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
    with HttpClient(USER_AGENT, min_interval=0.5) as client:
        if args.mode == "backfill":
            sync(client, args.start, args.end or now.date(), args.out, run_id, now)
        else:
            incremental(client, args.out, run_id, now)


if __name__ == "__main__":
    main()
