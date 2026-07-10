"""Parity test pinning ``project.schema.json`` to the ``ProjectConfig`` model.

The packaged JSON Schema for ``lhp.yaml`` is hand-written (it is an editor
hint served to Monaco by the web IDE, not a validation gate), so nothing
mechanically ties it to the Pydantic source of truth. This test closes that
gap: every field a user may write in ``lhp.yaml`` (``ProjectConfig``'s fields,
by alias where one exists) must appear as a top-level property in the schema.

The schema MAY carry extra top-level properties beyond the model (superset is
allowed); it must never lag behind the model.
"""

import json
from importlib.resources import files

import pytest

from lhp.models import ProjectConfig

pytestmark = pytest.mark.unit


def _packaged_project_schema() -> dict:
    resource = files("lhp.schemas") / "project.schema.json"
    return json.loads(resource.read_text(encoding="utf-8"))


def test_project_schema_covers_all_model_fields():
    """Top-level schema ``properties`` ⊇ ``ProjectConfig.model_fields`` (by alias)."""
    schema = _packaged_project_schema()
    schema_properties = set(schema["properties"])

    # Users write the YAML alias when one is declared (e.g. a field named
    # ``schema_`` aliased to ``schema``), so compare against alias-or-name.
    model_keys = {
        field.alias or name for name, field in ProjectConfig.model_fields.items()
    }

    missing = model_keys - schema_properties
    assert not missing, (
        f"project.schema.json is missing top-level properties for ProjectConfig "
        f"fields: {sorted(missing)}. Every lhp.yaml key accepted by the model "
        f"must be described in the schema (it powers the web IDE's Monaco hints)."
    )
