from unittest.mock import MagicMock

import setup_workspace as sw
from databricks.sdk.errors import AlreadyExists, PermissionDenied


def test_creates_catalog_schemas_and_volume():
    w = MagicMock()
    assert sw.setup(w, "lakehouse") == "lakehouse"
    w.catalogs.create.assert_called_once_with(name="lakehouse")
    assert [c.kwargs["name"] for c in w.schemas.create.call_args_list] == list(
        sw.SCHEMAS
    )
    assert w.volumes.create.call_args.kwargs["name"] == "landing"


def test_falls_back_when_catalog_creation_is_refused():
    w = MagicMock()
    w.catalogs.create.side_effect = PermissionDenied("no")
    assert sw.setup(w, "lakehouse") == sw.FALLBACK_CATALOG
    assert w.schemas.create.call_args.kwargs["catalog_name"] == sw.FALLBACK_CATALOG


def test_rerun_is_a_no_op():
    w = MagicMock()
    for m in (w.catalogs.create, w.schemas.create, w.volumes.create):
        m.side_effect = AlreadyExists("exists")
    assert sw.setup(w, "lakehouse") == "lakehouse"
