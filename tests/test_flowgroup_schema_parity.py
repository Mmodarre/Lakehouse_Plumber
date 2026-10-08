"""Parity test pinning ``flowgroup.schema.json`` to the ``WriteTarget`` model.

The packaged JSON Schema for a flowgroup is hand-written (it is an editor hint
served to Monaco by the web IDE, not a validation gate), so nothing
mechanically ties its ``WriteTarget`` definition to the Pydantic source of
truth. This test closes that gap: every field a user may write under an
action's ``write_target`` (``WriteTarget``'s fields, by alias where one exists)
must appear as a property of ``definitions.WriteTarget`` in the schema.

The schema MAY carry extra ``WriteTarget`` properties beyond the model (e.g.
``mode``, ``cdc_config``, ``snapshot_cdc_config`` are handled as raw dict keys
on the action, not model fields — superset is allowed); it must never lag
behind the model.
"""

import json
from importlib.resources import files

import pytest

from lhp.models import Action, FlowGroup, WriteTarget

pytestmark = pytest.mark.unit


def _packaged_flowgroup_schema() -> dict:
    resource = files("lhp.schemas") / "flowgroup.schema.json"
    return json.loads(resource.read_text(encoding="utf-8"))


def test_flowgroup_schema_covers_all_write_target_fields():
    """``WriteTarget`` schema ``properties`` ⊇ ``WriteTarget.model_fields`` (by alias)."""
    schema = _packaged_flowgroup_schema()
    schema_properties = set(schema["definitions"]["WriteTarget"]["properties"])

    # Users write the YAML alias when one is declared, so compare against
    # alias-or-name (mirrors tests/test_project_schema_parity.py).
    model_keys = {
        field.alias or name for name, field in WriteTarget.model_fields.items()
    }

    missing = model_keys - schema_properties
    assert not missing, (
        f"flowgroup.schema.json WriteTarget is missing properties for "
        f"WriteTarget model fields: {sorted(missing)}. Every write_target key "
        f"accepted by the model must be described in the schema (it powers the "
        f"web IDE's Monaco hints and the designer field-help tooltips)."
    )


@pytest.mark.parametrize(
    "filename,definition,model",
    [
        ("flowgroup.schema.json", "SingleFlowGroup", FlowGroup),
        ("flowgroup.schema.json", "Action", Action),
        ("template.schema.json", "Action", Action),
        ("template.schema.json", "WriteTarget", WriteTarget),
    ],
)
def test_authoring_schemas_cover_public_model_fields(filename, definition, model):
    """Prevent editor suggestions from losing fields documented for authoring."""
    schema = json.loads((files("lhp.schemas") / filename).read_text())
    documented = set(schema["definitions"][definition]["properties"])
    accepted = {field.alias or name for name, field in model.model_fields.items()}
    assert not (accepted - documented), (
        f"{filename} {definition} omits accepted fields: "
        f"{sorted(accepted - documented)}"
    )


@pytest.mark.parametrize("filename", ["flowgroup.schema.json", "template.schema.json"])
def test_schema_references_resolve_after_shared_contract_updates(filename):
    """Fields copied between authoring contracts must retain resolvable refs."""
    schema = json.loads((files("lhp.schemas") / filename).read_text())

    def walk(value):
        if isinstance(value, dict):
            if "$ref" in value and value["$ref"].startswith("#/"):
                target = schema
                for component in value["$ref"][2:].split("/"):
                    target = target[component.replace("~1", "/").replace("~0", "~")]
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(schema)
