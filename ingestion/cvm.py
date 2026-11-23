import argparse
import io
import logging
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

from ingestion.http import HttpClient, HttpError
from ingestion.state import read_state, write_state

# Daily fund report (informe diario), one zip with one CSV per month.
URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{month:%Y%m}.zip"
# Older months are only in DADOS/HIST, in yearly zips that are not all split by
# month (2000 is one yearly CSV). Not supported, see docs/tech-debt.md.
FIRST_MONTH = date(2021, 1, 1)
USER_AGENT = "brazil-finance-lakehouse/0.1 (cvm connector)"
STATE_FILE = "_state/cvm.json"
STATE_KEY = "inf_diario"
# CVM rewrites the current month and the one before it every day, and M-2 to
# M-11 once a week. One month back picks up the last revisions of a month that
# just closed. Later ones are missed, see ADR-0008.
LOOKBACK_MONTHS = 1

log = logging.getLogger("ingestion.cvm")


def add_months(month: date, n: int) -> date:
    index = month.year * 12 + month.month - 1 + n
    return date(index // 12, index % 12 + 1, 1)


def month_windows(start: date, end: date) -> list[date]:
    first, last = start.replace(day=1), end.replace(day=1)
    if first > last:
        raise ValueError(f"start {start} is after end {end}")
    if first < FIRST_MONTH:
        raise SystemExit(f"months before {FIRST_MONTH:%Y-%m} are not supported")
    months = []
    while first <= last:
        months.append(first)
        first = add_months(first, 1)
    return months


def window_path(month: date) -> Path:
    return Path(f"cvm_inf_diario/month={month:%Y-%m}.csv")


def fetch_month(client: HttpClient, month: date) -> bytes | None:
    try:
        content = client.get(URL.format(month=month)).content
    except HttpError as e:
        if e.status == 404:
            return None
        raise
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        return z.read(f"inf_diario_fi_{month:%Y%m}.csv")


def sync(client: HttpClient, start: date, end: date, out: Path, now: datetime) -> None:
    current_month = now.date().replace(day=1)
    written = []
    for month in month_windows(start, end):
        csv = fetch_month(client, month)
        if csv is None:
            # The current month's file appears a day or two after the 1st.
            # Any other missing month is unexpected.
            if month == current_month:
                log.info(f"{month:%Y-%m} is not published yet")
                continue
            raise SystemExit(f"CVM has no file for {month:%Y-%m}")
        # The CSV lands byte for byte: bronze reads it with its header.
        path = out / window_path(month)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(csv)
        written.append(f"{month:%Y-%m}")

    if written:
        # A backfill of old months never moves the watermark back.
        state = read_state(out, STATE_FILE)
        state[STATE_KEY] = max(max(written), state.get(STATE_KEY, ""))
        write_state(out, STATE_FILE, state)


def incremental(client: HttpClient, out: Path, now: datetime) -> None:
    watermark = read_state(out, STATE_FILE).get(STATE_KEY)
    if watermark is None:
        raise SystemExit("no CVM watermark, run backfill")
    start = add_months(date.fromisoformat(f"{watermark}-01"), -LOOKBACK_MONTHS)
    sync(client, start, now.date(), out, now)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m ingestion.cvm")
    parser.add_argument("--out", type=Path, default=Path("out"))
    modes = parser.add_subparsers(dest="mode", required=True)
    backfill_parser = modes.add_parser("backfill", help="whole months, by day given")
    backfill_parser.add_argument(
        "--from", dest="start", type=date.fromisoformat, required=True
    )
    backfill_parser.add_argument("--to", dest="end", type=date.fromisoformat)
    modes.add_parser("incremental")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    now = datetime.now(UTC)
    # A monthly zip is about 10 MB, so the timeout is longer than the default.
    with HttpClient(USER_AGENT, timeout=120, min_interval=0.5) as client:
        if args.mode == "backfill":
            sync(client, args.start, args.end or now.date(), args.out, now)
        else:
            incremental(client, args.out, now)


if __name__ == "__main__":
    main()
