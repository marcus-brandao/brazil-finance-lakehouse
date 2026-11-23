# Uploads bronze_sgs.py to the workspace and creates the job, or updates it if
# it already exists. Safe to rerun. Does not start a run.

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, SparkPythonTask, Task
from databricks.sdk.service.workspace import ImportFormat

JOB_NAME = "bronze_sgs"
SCRIPT = Path(__file__).parent / "bronze_sgs.py"
FALLBACK_CATALOG = "workspace"


def job_settings(script_path: str, catalog: str) -> JobSettings:
    # No compute is set: Free Edition only has serverless, which jobs use by default.
    task = Task(
        task_key="bronze_sgs",
        spark_python_task=SparkPythonTask(
            python_file=script_path, parameters=["--catalog", catalog]
        ),
    )
    return JobSettings(name=JOB_NAME, tasks=[task], max_concurrent_runs=1)


def deploy(w: WorkspaceClient, catalog: str) -> int:
    folder = f"/Users/{w.current_user.me().user_name}/brazil-finance-lakehouse"
    w.workspace.mkdirs(folder)
    script_path = f"{folder}/bronze_sgs.py"
    # AUTO keeps a plain .py as a workspace file, SOURCE would turn it into a notebook.
    w.workspace.upload(
        script_path, SCRIPT.read_bytes(), format=ImportFormat.AUTO, overwrite=True
    )

    settings = job_settings(script_path, catalog)
    existing = next(iter(w.jobs.list(name=JOB_NAME)), None)
    if existing is None:
        created = w.jobs.create(
            name=settings.name,
            tasks=settings.tasks,
            max_concurrent_runs=settings.max_concurrent_runs,
        )
        return created.job_id
    w.jobs.reset(existing.job_id, settings)
    return existing.job_id


if __name__ == "__main__":
    catalog = os.environ.get("DATABRICKS_CATALOG", FALLBACK_CATALOG)
    print(f"Job {JOB_NAME} ready, id {deploy(WorkspaceClient(), catalog)}")
