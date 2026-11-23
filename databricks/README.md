`setup_workspace.py` creates the catalog, the `bronze`, `silver` and `gold` schemas and the `bronze.landing` Volume. See [docs/setup.md](../docs/setup.md).

`bronze_sgs.py` is the Auto Loader job that fills `bronze.sgs_raw`, and `create_bronze_job.py` uploads it and creates the serverless job. See [docs/bronze.md](../docs/bronze.md).

The Databricks Asset Bundle config is planned for M2.
