import io
import logging
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied

from ingestion import upload


def make_out(tmp_path):
    out = tmp_path / "out"
    (out / "sgs/series_code=432").mkdir(parents=True)
    (out / "_state").mkdir()
    (out / "sgs/series_code=432/year=2025.json").write_text("{}")
    (out / "_state/sgs.json").write_text('{"432": "2025-06-30"}')
    return out


def test_same_path_is_overwritten(tmp_path):
    out = make_out(tmp_path)
    w = MagicMock()
    upload.push(w, out)
    upload.push(w, out)
    targets = [c.args[0] for c in w.files.upload.call_args_list]
    assert len(targets) == 4
    assert len(set(targets)) == 2
    assert all(c.kwargs["overwrite"] is True for c in w.files.upload.call_args_list)
    assert (
        targets[0]
        == "/Volumes/workspace/bronze/landing/sgs/series_code=432/year=2025.json"
    )


def test_state_file_is_uploaded_last(tmp_path):
    out = make_out(tmp_path)
    w = MagicMock()
    upload.push(w, out)
    assert w.files.upload.call_args_list[-1].args[0].endswith("_state/sgs.json")


def test_catalog_comes_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABRICKS_CATALOG", "lakehouse")
    w = MagicMock()
    upload.push(w, make_out(tmp_path))
    assert w.files.upload.call_args.args[0].startswith("/Volumes/lakehouse/bronze/")


def test_dry_run_needs_no_credentials(tmp_path, monkeypatch, caplog):
    for name in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    out = make_out(tmp_path)
    with (
        patch.object(upload, "WorkspaceClient", side_effect=AssertionError),
        caplog.at_level(logging.INFO, logger="ingestion.upload"),
    ):
        upload.main(["push", "--dry-run", "--out", str(out)])
        upload.main(["pull", "--dry-run", "--out", str(out)])
    assert caplog.text.count("dry run: would upload") == 2


def test_state_round_trip(tmp_path):
    out = make_out(tmp_path)
    volume = {}
    w = MagicMock()
    w.files.upload.side_effect = lambda path, f, overwrite: volume.update(
        {path: f.read()}
    )

    def download(path):
        if path not in volume:
            raise NotFound("missing")
        return MagicMock(contents=io.BytesIO(volume[path]))

    w.files.download.side_effect = download
    upload.push(w, out)

    fresh = tmp_path / "fresh"
    upload.pull_state(w, fresh)
    assert (fresh / "_state/sgs.json").read_text() == '{"432": "2025-06-30"}'


def test_missing_state_on_first_run(tmp_path):
    w = MagicMock()
    w.files.download.side_effect = NotFound("missing")
    upload.pull_state(w, tmp_path)
    assert not (tmp_path / "_state/sgs.json").exists()


def test_upload_error_is_raised(tmp_path):
    w = MagicMock()
    w.files.upload.side_effect = PermissionDenied("no")
    with pytest.raises(PermissionDenied):
        upload.push(w, make_out(tmp_path))


def test_both_state_files_are_uploaded_after_all_data(tmp_path):
    out = make_out(tmp_path)
    (out / "ptax").mkdir()
    (out / "ptax/year=2025.json").write_text("{}")
    (out / "_state/ptax.json").write_text('{"usd": "2025-12-31"}')
    w = MagicMock()
    upload.push(w, out)
    targets = [c.args[0] for c in w.files.upload.call_args_list]
    assert all("_state/" not in t for t in targets[:2])
    assert sorted(t.rsplit("/", 1)[1] for t in targets[2:]) == ["ptax.json", "sgs.json"]


def test_pull_reads_both_state_files_and_survives_one_missing(tmp_path):
    w = MagicMock()

    def download(path):
        if path.endswith("ptax.json"):
            raise NotFound("missing")
        return MagicMock(contents=io.BytesIO(b'{"432": "2025-06-30"}'))

    w.files.download.side_effect = download
    upload.pull_state(w, tmp_path)
    assert (tmp_path / "_state/sgs.json").exists()
    assert not (tmp_path / "_state/ptax.json").exists()
