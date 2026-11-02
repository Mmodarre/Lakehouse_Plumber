"""``expectations_file`` is a materialized-view write-target field.

The model, the field catalog and both authoring schemas declare it; only the
materialized_view field set accepts it.
"""

import json
from importlib.resources import files

import pytest

from lhp.core.validators.field._field_catalog import WRITE_TARGET_FIELDS
from lhp.models import Action, WriteTarget

pytestmark = pytest.mark.unit


def test_model_declares_the_field():
    target = WriteTarget(type="materialized_view", expectations_file="e/rules.yaml")

    assert target.expectations_file == "e/rules.yaml"
    assert WriteTarget(type="materialized_view").expectations_file is None


def test_write_target_path_is_normalised_to_forward_slashes():
    action = Action(
        name="w",
        type="write",
        write_target={
            "type": "materialized_view",
            "table": "t",
            "expectations_file": "expectations\\gold\\rules.yaml",
        },
    )

    assert action.write_target["expectations_file"] == "expectations/gold/rules.yaml"


def test_only_materialized_views_accept_the_field():
    assert "expectations_file" in WRITE_TARGET_FIELDS["materialized_view"]
    assert "expectations_file" not in WRITE_TARGET_FIELDS["streaming_table"]
    assert "expectations_file" not in WRITE_TARGET_FIELDS["sink"]


@pytest.mark.parametrize("filename", ["flowgroup.schema.json", "template.schema.json"])
def test_authoring_schemas_describe_the_field(filename):
    schema = json.loads((files("lhp.schemas") / filename).read_text())
    prop = schema["definitions"]["WriteTarget"]["properties"]["expectations_file"]

    assert prop["type"] == "string"
    assert "materialized views" in prop["description"]
