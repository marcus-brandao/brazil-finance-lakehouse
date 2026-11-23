`setup_workspace.py` creates the catalog, the `bronze`, `silver` and `gold` schemas and the `bronze.landing` Volume. See [docs/setup.md](../docs/setup.md).

`bronze.py` is the Auto Loader job that fills `bronze.sgs_raw` and `bronze.ptax_raw` (`--source sgs` or `--source ptax`), `create_bronze_job.py` uploads it and creates the serverless job with one task per source, and `run_bronze_job.py` starts a run and waits for it (the ingest workflow calls it). See [docs/bronze.md](../docs/bronze.md).

The Databricks Asset Bundle config is planned for M2.
