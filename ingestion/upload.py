import argparse
import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from ingestion.sgs import STATE_FILE

log = logging.getLogger("ingestion.upload")

# Same Volume that databricks/setup_workspace.py creates (bronze.landing).
VOLUME = "/Volumes/{catalog}/bronze/landing"


def volume_root() -> str:
    return VOLUME.format(catalog=os.environ.get("DATABRICKS_CATALOG", "workspace"))


def pull_state(w: WorkspaceClient, out: Path) -> None:
    # A first run has no state file on the Volume yet. incremental then fails
    # with its own "run backfill first" message.
    try:
        response = w.files.download(f"{volume_root()}/{STATE_FILE}")
    except NotFound:
        log.info("no state file on the Volume yet")
        return
    path = out / STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.contents.read())


def push(w: WorkspaceClient | None, out: Path) -> None:
    # The state file goes last, so a failed upload never moves the watermark
    # past data that did not land. Errors are not caught.
    files = sorted(p for p in out.rglob("*") if p.is_file())
    files.sort(key=lambda p: p.relative_to(out).as_posix() == STATE_FILE)
    for path in files:
        target = f"{volume_root()}/{path.relative_to(out).as_posix()}"
        if w is None:
            log.info(f"dry run: would upload {path} to {target}")
            continue
        with path.open("rb") as f:
            w.files.upload(target, f, overwrite=True)
        log.info(f"uploaded {target}")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(prog="python -m ingestion.upload")
    parser.add_argument("command", choices=["pull", "push"])
    parser.add_argument("--out", type=Path, default=Path("out"))
    # Dry run needs no credentials: out/ stands in for the Volume.
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.dry_run:
        if args.command == "push":
            push(None, args.out)
        return
    w = WorkspaceClient()
    if args.command == "pull":
        pull_state(w, args.out)
    else:
        push(w, args.out)


if __name__ == "__main__":
    main()
