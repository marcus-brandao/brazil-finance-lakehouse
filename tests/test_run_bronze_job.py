from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import run_bronze_job as rbj
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.jobs import RunResultState

URL = "https://example.cloud.databricks.com/jobs/7/runs/99"


@pytest.fixture
def summary(tmp_path, monkeypatch):
    path = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(path))
    return path


def make_client(result_state=RunResultState.SUCCESS, job_id=7):
    w = MagicMock()
    w.jobs.list.return_value = [MagicMock(job_id=job_id)] if job_id else []
    w.jobs.run_now.return_value.run_id = 99
    w.jobs.get_run.return_value.run_page_url = URL
    finished = w.jobs.run_now.return_value.result.return_value
    finished.state.result_state = result_state
    finished.state.state_message = "boom"
    return w


def test_success_writes_the_run_url(summary):
    w = make_client()
    rbj.run_bronze(w)
    w.jobs.run_now.assert_called_once_with(7)
    assert URL in summary.read_text()


def test_failed_run_exits_non_zero_and_keeps_the_url(summary):
    with pytest.raises(SystemExit) as e:
        rbj.run_bronze(make_client(RunResultState.FAILED))
    assert e.value.code == 1
    text = summary.read_text()
    assert URL in text
    assert "boom" in text


def test_skipped_run_is_a_failure(summary):
    with pytest.raises(SystemExit):
        rbj.run_bronze(make_client(result_state=None))


def test_timeout_is_a_failure_with_the_url(summary):
    w = make_client()
    w.jobs.run_now.return_value.result.side_effect = TimeoutError("timed out")
    with pytest.raises(SystemExit):
        rbj.run_bronze(w, timeout=timedelta(seconds=1))
    assert URL in summary.read_text()


def test_internal_error_is_a_failure(summary):
    w = make_client()
    w.jobs.run_now.return_value.result.side_effect = OperationFailed("internal")
    with pytest.raises(SystemExit):
        rbj.run_bronze(w)


def test_missing_job_says_how_to_create_it(summary):
    w = make_client(job_id=None)
    with pytest.raises(SystemExit) as e:
        rbj.run_bronze(w)
    assert "create_bronze_job.py" in str(e.value)
    w.jobs.run_now.assert_not_called()


def test_works_without_a_summary_file(monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    rbj.run_bronze(make_client())
