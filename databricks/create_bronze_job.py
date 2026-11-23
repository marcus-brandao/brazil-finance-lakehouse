# Uploads bronze.py to the workspace and creates the job, or updates it if it
# already exists. The job has one task per source. Safe to rerun. Does not start a run.

import os
from pathlib import Path

from bronze import SOURCES
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobEnvironment,
    JobSettings,
    SparkPythonTask,
    Task,
)
from databricks.sdk.service.workspace import ImportFormat

JOB_NAME = "bronze"
SCRIPT = Path(__file__).parent / "bronze.py"
FALLBACK_CATALOG = "workspace"
ENVIRONMENT_KEY = "default"


def job_settings(script_path: str, catalog: str) -> JobSettings:
    # Serverless tasks need an environment. The script has no dependencies, so only
    # the version is set. 6 was the newest on the release notes page (2026-11-22).
    environment = JobEnvironment(
        environment_key=ENVIRONMENT_KEY, spec=Environment(environment_version="6")
    )
    # No dependencies between tasks, so one failing source does not stop the other.
    tasks = [
        Task(
            task_key=f"bronze_{source}",
            environment_key=ENVIRONMENT_KEY,
            spark_python_task=SparkPythonTask(
                python_file=script_path,
                parameters=["--catalog", catalog, "--source", source],
            ),
        )
        for source in SOURCES
    ]
    return JobSettings(
        name=JOB_NAME,
        tasks=tasks,
        environments=[environment],
        max_concurrent_runs=1,
    )


def deploy(w: WorkspaceClient, catalog: str) -> int:
    folder = f"/Users/{w.current_user.me().user_name}/brazil-finance-lakehouse"
    w.workspace.mkdirs(folder)
    script_path = f"{folder}/bronze.py"
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
            environments=settings.environments,
            max_concurrent_runs=settings.max_concurrent_runs,
        )
        return created.job_id
    w.jobs.reset(existing.job_id, settings)
    return existing.job_id


if __name__ == "__main__":
    catalog = os.environ.get("DATABRICKS_CATALOG", FALLBACK_CATALOG)
    print(f"Job {JOB_NAME} ready, id {deploy(WorkspaceClient(), catalog)}")
