"""``write_target.expectations_file`` on a materialized view is checked at validate time.

The file must load, its rules must pass the strict expectation checks (warn,
drop and fail are all allowed on a view) and every rule needs its own name.
Real files on disk, no mocks.
"""

import json

import pytest
import yaml

from lhp.core.registry import ActionRegistry
from lhp.core.validators import (
    ConfigFieldValidator,
    ConfigValidator,
    WriteActionValidator,
)
from lhp.core.validators.action._write_materialized_view import (
    validate_materialized_view,
)
from lhp.errors import LHPConfigError, LHPError
from lhp.generators.registration import register_all
from lhp.models import Action, FlowGroup

pytestmark = pytest.mark.unit

_PREFIX = "Action[0] 'write_gate'"

_GATE_RULES = [
    {"name": "valid_id", "constraint": "id IS NOT NULL", "failureAction": "fail"},
    {"name": "positive_amount", "constraint": "amount > 0", "failureAction": "warn"},
    {"name": "has_region", "constraint": "region IS NOT NULL", "failureAction": "drop"},
]


def _mv(**write_target) -> Action:
    return Action(
        name="write_gate",
        type="write",
        write_target={
            "type": "materialized_view",
            "catalog": "main",
            "schema": "gold",
            "table": "gate",
            "sql": "SELECT * FROM main.silver.orders",
            **write_target,
        },
    )


def _write(project, name: str, content) -> str:
    path = project / "expectations" / name
    path.parent.mkdir(exist_ok=True)
    if name.endswith(".json"):
        path.write_text(json.dumps(content))
    else:
        path.write_text(yaml.safe_dump(content, sort_keys=False))
    return f"expectations/{name}"


def _validate(project, rules, name: str = "gate.yaml") -> list:
    action = _mv(expectations_file=_write(project, name, rules))
    return validate_materialized_view(action, _PREFIX, project)


class TestValidFiles:
    def test_warn_drop_and_fail_rules_are_all_accepted(self, tmp_path):
        assert _validate(tmp_path, _GATE_RULES) == []

    def test_mapping_format_is_accepted(self, tmp_path):
        rules = {
            "id IS NOT NULL": {"action": "fail", "name": "valid_id"},
            "amount > 0": {"action": "warn", "name": "positive_amount"},
            "region IS NOT NULL": {"action": "drop", "name": "has_region"},
        }
        assert _validate(tmp_path, rules, "gate.json") == []

    def test_wrapped_list_is_accepted(self, tmp_path):
        assert _validate(tmp_path, {"expectations": _GATE_RULES}) == []

    def test_no_expectations_file_reads_nothing(self, tmp_path):
        assert validate_materialized_view(_mv(), _PREFIX, tmp_path) == []

    def test_unknown_project_root_skips_the_file(self):
        action = _mv(expectations_file="expectations/absent.yaml")
        assert validate_materialized_view(action, _PREFIX, None) == []


class TestFileErrors:
    def test_missing_file_is_reported_with_its_io_code(self, tmp_path):
        action = _mv(expectations_file="expectations/absent.yaml")

        [error] = validate_materialized_view(action, _PREFIX, tmp_path)

        assert error.startswith(f"{_PREFIX}: Failed to load expectations file: ")
        assert "LHP-IO-001" in error

    def test_scalar_document_is_an_unexpected_format(self, tmp_path):
        assert _validate(tmp_path, "just text") == [
            f"{_PREFIX}: Expectations file has unexpected format"
        ]


class TestRuleErrors:
    def test_unknown_failure_action_is_val_006_with_a_did_you_mean(self, tmp_path):
        rules = [{"name": "valid_id", "constraint": "id > 0", "failureAction": "fial"}]

        [error] = _validate(tmp_path, rules)

        assert isinstance(error, LHPError)
        assert error.code == "LHP-VAL-006"
        assert error.context == {
            "Expectation": "'valid_id'",
            "Field": "failureAction",
            "Provided": "fial",
        }
        assert error.suggestions[0] == "Did you mean 'fail'?"

    def test_unknown_mapping_action_is_val_006(self, tmp_path):
        rules = {"id > 0": {"action": "dorp", "name": "valid_id"}}

        [error] = _validate(tmp_path, rules)

        assert error.code == "LHP-VAL-006"
        assert error.context["Field"] == "action"
        assert error.suggestions[0] == "Did you mean 'drop'?"

    def test_rule_without_a_constraint_is_val_001(self, tmp_path):
        [error] = _validate(tmp_path, [{"name": "checks_nothing"}])

        assert error.code == "LHP-VAL-001"
        assert error.context == {
            "Expectation": "'checks_nothing'",
            "Field": "constraint",
        }

    def test_rule_that_is_not_a_mapping_is_val_008(self, tmp_path):
        [error] = _validate(tmp_path, ["id IS NOT NULL"])

        assert error.code == "LHP-VAL-008"


class TestDuplicateNames:
    def test_same_name_in_one_category_is_rejected(self, tmp_path):
        rules = [
            {
                "name": "valid_id",
                "constraint": "id IS NOT NULL",
                "failureAction": "warn",
            },
            {"name": "valid_id", "constraint": "id > 0", "failureAction": "warn"},
        ]

        [error] = _validate(tmp_path, rules)

        assert isinstance(error, LHPError)
        assert error.code == "LHP-VAL-006"
        assert error.title == "Duplicate expectation name"
        assert error.details == (
            f"{_PREFIX}: 2 expectations in expectations file "
            "'expectations/gate.yaml' are named 'valid_id'. An expectation name "
            "must be unique for a dataset."
        )
        assert error.context == {
            "Expectation": "'valid_id'",
            "Field": "name",
            "Provided": "valid_id",
        }

    def test_same_name_across_categories_is_rejected(self, tmp_path):
        rules = [
            {
                "name": "valid_id",
                "constraint": "id IS NOT NULL",
                "failureAction": "fail",
            },
            {"name": "valid_id", "constraint": "id > 0", "failureAction": "drop"},
        ]

        [error] = _validate(tmp_path, rules)

        assert error.title == "Duplicate expectation name"

    def test_message_names_a_list_rule(self, tmp_path):
        rules = [
            {"name": "a", "message": "shared", "constraint": "x > 0"},
            {"name": "b", "message": "shared", "constraint": "y > 0"},
        ]

        [error] = _validate(tmp_path, rules)

        assert error.context["Provided"] == "shared"

    def test_unnamed_rules_with_the_same_constraint_clash(self, tmp_path):
        rules = [
            {"constraint": "x > 0", "failureAction": "warn"},
            {"constraint": "x > 0", "failureAction": "warn"},
        ]

        [error] = _validate(tmp_path, rules)

        assert error.context["Provided"] == "Constraint failed: x > 0"

    def test_mapping_rules_sharing_a_name_are_rejected(self, tmp_path):
        rules = {
            "id IS NOT NULL": {"action": "fail", "name": "valid_id"},
            "id > 0": {"action": "warn", "name": "valid_id"},
        }

        [error] = _validate(tmp_path, rules, "gate.json")

        assert error.context["Provided"] == "valid_id"

    def test_each_duplicated_name_is_one_error(self, tmp_path):
        rules = [
            {"name": "a", "constraint": "x > 0"},
            {"name": "a", "constraint": "x > 1"},
            {"name": "a", "constraint": "x > 2"},
            {"name": "b", "constraint": "y > 0"},
            {"name": "b", "constraint": "y > 1"},
        ]

        errors = _validate(tmp_path, rules)

        assert [e.context["Provided"] for e in errors] == ["a", "b"]
        assert "3 expectations" in errors[0].details

    def test_names_are_only_checked_once_the_rules_are_valid(self, tmp_path):
        rules = [
            {"name": "a", "constraint": "x > 0", "failureAction": "fial"},
            {"name": "a", "constraint": "x > 1"},
        ]

        assert [e.title for e in _validate(tmp_path, rules)] == [
            "Invalid value for 'failureAction'"
        ]


class TestQuerySourceChecksAreUnchanged:
    def test_no_source_sql_or_sql_path(self, tmp_path):
        action = Action(
            name="write_gate",
            type="write",
            write_target={"type": "materialized_view", "table": "gate"},
        )

        assert validate_materialized_view(action, _PREFIX, tmp_path) == [
            f"{_PREFIX}: Materialized view must have either 'source', 'sql', or "
            "'sql_path' in write_target"
        ]

    def test_file_errors_follow_the_query_source_error(self, tmp_path):
        action = Action(
            name="write_gate",
            type="write",
            write_target={
                "type": "materialized_view",
                "table": "gate",
                "expectations_file": "expectations/absent.yaml",
            },
        )

        errors = validate_materialized_view(action, _PREFIX, tmp_path)

        assert len(errors) == 2
        assert "must have either 'source'" in errors[0]
        assert "LHP-IO-001" in errors[1]


class TestOtherWriteTargets:
    @pytest.mark.parametrize(
        "write_target",
        [
            {"type": "streaming_table", "catalog": "c", "schema": "s", "table": "t"},
            {"type": "sink", "sink_type": "delta", "sink_name": "k"},
        ],
        ids=["streaming_table", "sink"],
    )
    def test_key_is_an_unknown_field_outside_materialized_views(self, write_target):
        with pytest.raises(LHPConfigError) as exc_info:
            ConfigFieldValidator().validate_write_target(
                {**write_target, "expectations_file": "e.yaml"}, "write_x"
            )

        assert exc_info.value.code == "LHP-CFG-001"

    def test_key_is_a_known_field_on_materialized_views(self):
        ConfigFieldValidator().validate_write_target(
            {"type": "materialized_view", "table": "t", "expectations_file": "e.yaml"},
            "write_x",
        )


def _flowgroup(expectations_file: str) -> FlowGroup:
    return FlowGroup(
        pipeline="p_gold",
        flowgroup="f_mv_gate",
        actions=[
            _mv(expectations_file=expectations_file).model_dump(exclude_none=True)
        ],
    )


class TestThroughConfigValidator:
    def test_valid_file_validates_clean(self, tmp_path):
        register_all()
        path = _write(tmp_path, "gate.yaml", _GATE_RULES)

        assert (
            ConfigValidator(project_root=tmp_path).validate_flowgroup(_flowgroup(path))
            == []
        )

    def test_rule_error_reaches_flowgroup_validation(self, tmp_path):
        register_all()
        rules = [{"name": "valid_id", "constraint": "id > 0", "failureAction": "fial"}]
        path = _write(tmp_path, "gate.yaml", rules)

        errors = ConfigValidator(project_root=tmp_path).validate_flowgroup(
            _flowgroup(path)
        )

        assert [e.code for e in errors if isinstance(e, LHPError)] == ["LHP-VAL-006"]

    def test_missing_file_reaches_flowgroup_validation(self, tmp_path):
        register_all()

        errors = ConfigValidator(project_root=tmp_path).validate_flowgroup(
            _flowgroup("expectations/absent.yaml")
        )

        assert len(errors) == 1
        assert "Failed to load expectations file" in str(errors[0])

    def test_write_action_validator_passes_its_project_root(self, tmp_path):
        register_all()
        validator = WriteActionValidator(
            ActionRegistry(), ConfigFieldValidator(), tmp_path
        )

        [error] = validator.validate(
            _mv(expectations_file="expectations/absent.yaml"), _PREFIX
        )

        assert "LHP-IO-001" in error
