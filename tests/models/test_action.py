"""``Action`` keeps unknown action-level keys so validation can reject them.

With Pydantic's default ``extra="ignore"`` a misspelt key (``readmode``,
``expectation_file``) vanished at parse time and ``lhp validate`` passed. The
model now keeps it through every rebuild (presets, local variables,
templates) and pickling, and ``ConfigValidator`` raises LHP-CFG-001.
"""

import pickle

import pytest

from lhp.core.loaders import read_sql_file
from lhp.core.processing import TemplateEngine
from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.validators import ConfigValidator
from lhp.errors import LHPConfigError
from lhp.models import Action, FlowGroup

pytestmark = pytest.mark.unit


def _load(**extra) -> dict:
    return {
        "name": "load_src",
        "type": "load",
        "source": {"type": "delta", "catalog": "c", "schema": "s", "table": "t"},
        "target": "v_src",
        **extra,
    }


class TestUnknownKeysAreKept:
    def test_model_keeps_the_key_and_its_value(self):
        action = Action(**_load(readmode="batch"))

        assert action.model_extra == {"readmode": "batch"}
        assert action.model_dump()["readmode"] == "batch"
        assert action.readMode is None

    def test_known_fields_are_not_extras(self):
        assert Action(**_load(readMode="batch")).model_extra == {}

    def test_dump_and_rebuild_keeps_it(self):
        flowgroup = FlowGroup(pipeline="p", flowgroup="f", actions=[_load(bogus=1)])

        rebuilt = FlowGroup(**flowgroup.model_dump())

        assert rebuilt.actions[0].model_extra == {"bogus": 1}

    def test_pickle_keeps_it(self):
        flowgroup = FlowGroup(pipeline="p", flowgroup="f", actions=[_load(bogus=1)])

        restored = pickle.loads(pickle.dumps(flowgroup))

        assert restored.actions[0].model_extra == {"bogus": 1}

    def test_preset_merge_keeps_it(self):
        flowgroup = FlowGroup(pipeline="p", flowgroup="f", actions=[_load(bogus=1)])

        merged = FlowgroupResolutionService().apply_preset_config(flowgroup, {})

        assert merged.actions[0].model_extra == {"bogus": 1}

    def test_template_expansion_keeps_it(self, tmp_path):
        (tmp_path / "templates").mkdir()
        (tmp_path / "templates" / "ingest.yaml").write_text(
            "name: ingest\n"
            "parameters:\n"
            "  - name: table\n"
            "    required: true\n"
            "actions:\n"
            "  - name: 'load_{{ table }}'\n"
            "    type: load\n"
            "    source: {type: delta, catalog: c, schema: s, table: '{{ table }}'}\n"
            "    target: 'v_{{ table }}'\n"
            "    expectation_file: 'expectations/{{ table }}.yaml'\n"
        )
        engine = TemplateEngine.for_project(tmp_path, read_sql_file)

        (action,) = engine.render_template("ingest", {"table": "orders"})

        assert action.model_extra == {"expectation_file": "expectations/orders.yaml"}

    def test_json_schema_allows_additional_properties(self):
        # The persistent parse cache keys on FlowGroup's JSON schema, so this
        # change also invalidates shards written before it.
        assert Action.model_json_schema()["additionalProperties"] is True


class TestUnknownKeysAreRejected:
    def test_config_validator_raises_cfg_001_with_a_suggestion(self):
        with pytest.raises(LHPConfigError) as exc_info:
            ConfigValidator().validate_action(Action(**_load(readmode="batch")), 0)

        error = exc_info.value
        assert error.code == "LHP-CFG-001"
        assert error.title == "Unknown field 'readmode' in action"
        assert error.context["Unknown"] == ["readmode"]
        assert "'readmode' → 'readMode'" in error.example

    def test_valid_action_passes_the_field_check(self):
        assert ConfigValidator().validate_action(Action(**_load()), 0) == []
