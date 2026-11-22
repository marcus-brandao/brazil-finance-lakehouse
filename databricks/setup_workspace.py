# Creates the catalog, schemas and landing Volume. Safe to rerun.

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, DatabricksError
from databricks.sdk.service.catalog import VolumeType

SCHEMAS = ("bronze", "silver", "gold")
# Default catalog of a Free Edition workspace, used when CREATE CATALOG is refused.
FALLBACK_CATALOG = "workspace"


def _ignore_exists(create, *args, **kwargs) -> None:
    try:
        create(*args, **kwargs)
    except AlreadyExists:
        pass


def ensure_catalog(w: WorkspaceClient, name: str) -> str:
    try:
        _ignore_exists(w.catalogs.create, name=name)
    except DatabricksError as e:
        # Fail loudly if the fallback catalog is missing too.
        print(f"Cannot create catalog {name!r} ({e}); using {FALLBACK_CATALOG!r}")
        w.catalogs.get(FALLBACK_CATALOG)
        return FALLBACK_CATALOG
    return name


def setup(w: WorkspaceClient, catalog: str) -> str:
    catalog = ensure_catalog(w, catalog)
    for schema in SCHEMAS:
        _ignore_exists(w.schemas.create, name=schema, catalog_name=catalog)
    _ignore_exists(
        w.volumes.create,
        catalog_name=catalog,
        schema_name="bronze",
        name="landing",
        volume_type=VolumeType.MANAGED,
    )
    return catalog


if __name__ == "__main__":
    used = setup(
        WorkspaceClient(), os.environ.get("DATABRICKS_CATALOG", FALLBACK_CATALOG)
    )
    print(f"Ready: {used}.bronze.landing")
