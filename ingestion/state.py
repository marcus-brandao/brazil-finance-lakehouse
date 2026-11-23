import json
from pathlib import Path


def read_state(out: Path, state_file: str) -> dict[str, str]:
    path = out / state_file
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def write_state(out: Path, state_file: str, state: dict[str, str]) -> None:
    path = out / state_file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))
