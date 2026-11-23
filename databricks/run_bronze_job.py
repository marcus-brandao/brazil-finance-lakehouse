# Starts the bronze_sgs job, waits for it and fails unless it succeeded. The run URL
# goes to the GitHub step summary before the wait, so it is there even if the run fails.

import os
from datetime import timedelta

from create_bronze_job import JOB_NAME
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.jobs import RunResultState

TIMEOUT = timedelta(minutes=30)


def find_job_id(w: WorkspaceClient) -> int:
    job = next(iter(w.jobs.list(name=JOB_NAME)), None)
    if job is None:
        raise SystemExit(
            f"job {JOB_NAME} not found. Run databricks/create_bronze_job.py first."
        )
    return job.job_id


def add_to_summary(line: str) -> None:
    print(line)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(line + "\n\n")


def run_bronze(w: WorkspaceClient, timeout: timedelta = TIMEOUT) -> None:
    waiter = w.jobs.run_now(find_job_id(w))
    url = w.jobs.get_run(waiter.run_id).run_page_url
    add_to_summary(f"Bronze job run: {url}")
    try:
        run = waiter.result(timeout=timeout)
    except (OperationFailed, TimeoutError) as e:
        add_to_summary(f"Bronze job did not finish: {e}")
        raise SystemExit(1) from e
    if run.state.result_state != RunResultState.SUCCESS:
        message = f"{run.state.result_state}: {run.state.state_message}"
        add_to_summary(f"Bronze job failed ({message})")
        raise SystemExit(1)
    add_to_summary("Bronze job succeeded")


if __name__ == "__main__":
    run_bronze(WorkspaceClient())
