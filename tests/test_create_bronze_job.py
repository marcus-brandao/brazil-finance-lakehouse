from unittest.mock import MagicMock

import create_bronze_job as cbj
from databricks.sdk.service.workspace import ImportFormat


def make_client(existing_job_id=None):
    w = MagicMock()
    w.current_user.me.return_value.user_name = "me@example.com"
    w.jobs.list.return_value = (
        [MagicMock(job_id=existing_job_id)] if existing_job_id else []
    )
    return w


def test_creates_the_job_when_it_does_not_exist():
    w = make_client()
    w.jobs.create.return_value.job_id = 7
    assert cbj.deploy(w, "lakehouse") == 7
    w.jobs.reset.assert_not_called()
    tasks = w.jobs.create.call_args.kwargs["tasks"]
    assert [t.task_key for t in tasks] == [
        "bronze_sgs",
        "bronze_ptax",
        "bronze_cvm_inf_diario",
    ]
    assert [t.spark_python_task.parameters for t in tasks] == [
        ["--catalog", "lakehouse", "--source", "sgs"],
        ["--catalog", "lakehouse", "--source", "ptax"],
        ["--catalog", "lakehouse", "--source", "cvm_inf_diario"],
    ]
    assert all(t.spark_python_task.python_file.endswith("/bronze.py") for t in tasks)
    assert all(not t.depends_on for t in tasks)
    environment = w.jobs.create.call_args.kwargs["environments"][0]
    assert all(t.environment_key == environment.environment_key for t in tasks)
    assert w.jobs.create.call_args.kwargs["name"] == "bronze"
    assert environment.spec.environment_version


def test_updates_the_job_when_it_exists():
    w = make_client(existing_job_id=42)
    assert cbj.deploy(w, "workspace") == 42
    w.jobs.create.assert_not_called()
    assert w.jobs.reset.call_args.args[0] == 42


def test_script_is_uploaded_as_a_file_and_overwritten():
    w = make_client()
    cbj.deploy(w, "workspace")
    kwargs = w.workspace.upload.call_args.kwargs
    assert kwargs["format"] == ImportFormat.AUTO
    assert kwargs["overwrite"] is True
