from unittest.mock import MagicMock

import pytest
import setup_workspace as sw
from databricks.sdk.errors import AlreadyExists, BadRequest, NotFound, PermissionDenied


def test_creates_catalog_schemas_and_volume():
    w = MagicMock()
    w.catalogs.get.side_effect = NotFound("missing")
    assert sw.setup(w, "lakehouse") == "lakehouse"
    w.catalogs.create.assert_called_once_with(name="lakehouse")
    assert [c.kwargs["name"] for c in w.schemas.create.call_args_list] == list(
        sw.SCHEMAS
    )
    assert w.volumes.create.call_args.kwargs["name"] == "landing"


def test_falls_back_when_catalog_creation_is_refused():
    w = MagicMock()
    w.catalogs.get.side_effect = [NotFound("missing"), MagicMock()]
    w.catalogs.create.side_effect = BadRequest(sw.NO_STORAGE_ROOT)
    assert sw.setup(w, "lakehouse") == sw.FALLBACK_CATALOG
    assert w.schemas.create.call_args.kwargs["catalog_name"] == sw.FALLBACK_CATALOG


def test_rerun_is_a_no_op():
    w = MagicMock()
    for m in (w.catalogs.create, w.schemas.create, w.volumes.create):
        m.side_effect = AlreadyExists("exists")
    assert sw.setup(w, "lakehouse") == "lakehouse"


def test_rerun_handles_bad_request_for_duplicates():
    w = MagicMock()
    w.schemas.create.side_effect = BadRequest("Schema 'bronze' already exists")
    assert sw.setup(w, "lakehouse") == "lakehouse"


def test_other_errors_are_not_swallowed():
    w = MagicMock()
    w.schemas.create.side_effect = BadRequest("invalid name")
    with pytest.raises(BadRequest):
        sw.setup(w, "lakehouse")


def test_unrelated_catalog_errors_are_not_swallowed():
    w = MagicMock()
    w.catalogs.get.side_effect = NotFound("missing")
    w.catalogs.create.side_effect = PermissionDenied("no")
    with pytest.raises(PermissionDenied):
        sw.setup(w, "lakehouse")


def test_existing_catalog_is_not_recreated():
    w = MagicMock()
    assert sw.setup(w, "workspace") == "workspace"
    w.catalogs.create.assert_not_called()
