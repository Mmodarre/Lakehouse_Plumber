"""Executable examples and contextual content invariants for shipped field help."""

import json
from pathlib import Path

import pytest
import yaml

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, LoadActionValidator
from lhp.core.validators.action.test import TestActionValidator as ActionTestValidator
from lhp.core.validators.action.transform import TransformActionValidator
from lhp.core.validators.action.write import WriteActionValidator
from lhp.models import Action
from lhp.parsers.schema_transform_parser import SchemaTransformParser

ROOT = Path(__file__).resolve().parents[1]
HELP = ROOT / "src/lhp/schemas/help"
KINDS = ("project", "pipeline_config", "job_config", "flowgroup", "template")
CATALOGS = {kind: json.loads((HELP / f"{kind}.json").read_text()) for kind in KINDS}
ENTRIES = [entry for catalog in CATALOGS.values() for entry in catalog["entries"]]
BY_ID = {entry["id"]: entry for entry in ENTRIES}
EXAMPLES = [
    (entry["id"], example) for entry in ENTRIES for example in entry.get("examples", [])
]


@pytest.mark.parametrize("entry_id,example", EXAMPLES, ids=[x[0] for x in EXAMPLES])
def test_help_examples_are_copyable_yaml(entry_id, example):
    value = yaml.safe_load(example["yaml"])
    assert value is not None, entry_id


@pytest.mark.parametrize(
    "entry_id,example",
    [(key, ex) for key, ex in EXAMPLES if ex["label"] == "Complete action example"],
)
def test_complete_action_examples_match_real_validators(entry_id, example):
    action = Action.model_validate(yaml.safe_load(example["yaml"]))
    registry, fields = ActionRegistry(), ConfigFieldValidator()
    validators = {
        "load": LoadActionValidator(registry, fields),
        "transform": TransformActionValidator(registry, fields),
        "write": WriteActionValidator(registry, fields),
        "test": ActionTestValidator(registry, fields),
    }
    assert not validators[action.type.value].validate(action, entry_id)
    if action.schema_inline:
        assert SchemaTransformParser().parse_inline_schema(action.schema_inline)


def test_shared_schema_field_has_different_contextual_guidance():
    cloud = BY_ID["action.load.cloudfiles.source.schema"]
    delta = BY_ID["action.load.delta.source.schema"]
    assert cloud["summary"] != delta["summary"]
    assert "file" in cloud["summary"]
    assert "Unity Catalog" in delta["summary"]
    assert cloud["bindings"] == [
        {"path": ["source", "schema"], "subtype": "load:cloudfiles"}
    ]
    assert delta["bindings"] == [
        {"path": ["source", "schema"], "subtype": "load:delta"}
    ]


def test_required_default_and_inheritance_copy_preserves_runtime_distinctions():
    required = " ".join(BY_ID["template.parameters.*.required"]["details"])
    assert "before defaults" in required
    assert "must include its key" in required
    packaging = BY_ID["pipeline.packaging"]["unsetBehavior"]
    assert "Inherit project defaults" in packaging
    assert "only when neither level" in packaging


def test_sources_and_related_entries_exist():
    assert len(BY_ID) == len(ENTRIES)
    for entry in ENTRIES:
        assert entry["sources"]
        for source in entry["sources"]:
            assert (ROOT / source["file"]).is_file(), (entry["id"], source)
        for related in entry.get("relatedHelpIds", []):
            assert related in BY_ID
