"""Per-rule checks of ``validate_expectations_file`` (both modes).

A rule with an unknown action or type used to become a warn expectation (or be
dropped with a log line); a rule with no constraint was skipped. Each is now an
``LHPError``: LHP-VAL-006 for an unknown value (or a list rule that uses the
mapping format's ``action`` key), LHP-VAL-001 for a missing constraint and
LHP-VAL-008 for a rule that is not a mapping. Real files on disk, no mocks.
"""

import json

import pytest
import yaml

from lhp.core.validators.field import validate_expectations_file
from lhp.errors import LHPError

pytestmark = pytest.mark.unit

_PREFIX = "Action[1] 'dq'"


def _errors(tmp_path, document, quarantine=False) -> list:
    (tmp_path / "rules.yaml").write_text(yaml.dump(document, sort_keys=False))
    return validate_expectations_file(
        "rules.yaml", tmp_path, _PREFIX, quarantine=quarantine
    )


def _only(errors: list) -> LHPError:
    assert len(errors) == 1, errors
    assert isinstance(errors[0], LHPError)
    return errors[0]


@pytest.fixture(params=[False, True], ids=["dqe", "quarantine"])
def quarantine(request):
    return request.param


class TestUnknownAction:
    def test_mapping_action_typo(self, tmp_path, quarantine):
        error = _only(
            _errors(
                tmp_path,
                {
                    "reconciled = true": {"action": "fial", "name": "reconciles"},
                    "amount >= 0": {"action": "drop", "name": "non_negative"},
                },
                quarantine,
            )
        )

        assert error.code == "LHP-VAL-006"
        assert error.title == "Invalid value for 'action'"
        assert error.details == (
            f"{_PREFIX}: expectation 'reconciles' in expectations file "
            "'rules.yaml' has invalid value 'fial' for 'action'. "
            "Valid values: drop, fail, warn"
        )
        assert error.suggestions == [
            "Did you mean 'fail'?",
            "Use one of: drop, fail, warn",
        ]
        assert error.context == {
            "Expectation": "'reconciles'",
            "Field": "action",
            "Provided": "fial",
        }

    def test_list_failure_action_typo(self, tmp_path, quarantine):
        error = _only(
            _errors(
                tmp_path,
                [{"name": "id_check", "constraint": "id > 0", "failureAction": "dorp"}],
                quarantine,
            )
        )

        assert error.code == "LHP-VAL-006"
        assert error.context["Field"] == "failureAction"
        assert error.suggestions[0] == "Did you mean 'drop'?"

    def test_list_type_is_checked_when_there_is_no_failure_action(self, tmp_path):
        error = _only(
            _errors(
                tmp_path,
                [
                    {
                        "name": "id_check",
                        "constraint": "id > 0",
                        "type": "expect_or_dorp",
                    }
                ],
            )
        )

        assert error.code == "LHP-VAL-006"
        assert error.context["Field"] == "type"
        assert error.details.endswith(
            "Valid values: expect, expect_or_drop, expect_or_fail"
        )
        assert error.suggestions[0] == "Did you mean 'expect_or_drop'?"

    def test_list_type_is_ignored_when_failure_action_decides(self, tmp_path):
        rule = {"constraint": "id > 0", "failureAction": "drop", "type": "bogus"}

        assert _errors(tmp_path, [rule]) == []

    def test_list_rule_with_action_key_is_rejected(self, tmp_path, quarantine):
        error = _only(
            _errors(
                tmp_path,
                [{"name": "id_check", "constraint": "id > 0", "action": "drop"}],
                quarantine,
            )
        )

        assert error.code == "LHP-VAL-006"
        assert error.title == "Invalid field 'action' in a list-format expectation"
        assert error.details == (
            f"{_PREFIX}: expectation 'id_check' in expectations file 'rules.yaml' "
            "sets 'action', the mapping format's key. A rule in a list sets "
            "'failureAction'."
        )
        assert error.suggestions == ["Use 'failureAction: drop' instead of 'action'"]

    def test_action_key_is_rejected_even_beside_failure_action(self, tmp_path):
        rule = {"constraint": "id > 0", "failureAction": "drop", "action": "drop"}

        assert _only(_errors(tmp_path, [rule])).context["Field"] == "action"

    def test_unnamed_list_rule_is_labelled_by_position(self, tmp_path):
        rules = [
            {"name": "ok", "constraint": "a > 0"},
            {"constraint": "b > 0", "failureAction": "nope"},
        ]

        assert _only(_errors(tmp_path, rules)).context["Expectation"] == "#2"


class TestMissingConstraint:
    def test_list_rule_without_constraint(self, tmp_path):
        error = _only(
            _errors(tmp_path, [{"name": "id_check", "failureAction": "fail"}])
        )

        assert error.code == "LHP-VAL-001"
        assert error.details == (
            f"{_PREFIX}: expectation 'id_check' in expectations file 'rules.yaml' "
            "has no constraint, so it checks nothing."
        )

    def test_mapping_rule_with_empty_constraint(self, tmp_path):
        error = _only(_errors(tmp_path, {"": {"action": "drop", "name": "blank"}}))

        assert error.code == "LHP-VAL-001"
        assert error.context["Expectation"] == "'blank'"

    def test_quarantine_with_one_usable_rule_reports_the_other(self, tmp_path):
        rules = [{"name": "kept", "constraint": "a > 0"}, {"name": "empty"}]

        error = _only(_errors(tmp_path, rules, quarantine=True))

        assert error.code == "LHP-VAL-001"

    def test_quarantine_with_no_usable_rule_reports_only_the_count(self, tmp_path):
        errors = _errors(tmp_path, [{"name": "empty"}], quarantine=True)

        assert len(errors) == 1
        assert "Quarantine mode requires at least one expectation" in errors[0]


class TestRuleShape:
    @pytest.mark.parametrize(
        ("document", "label", "actual_type"),
        [
            (["id > 0"], "#1", "str"),
            ({"id > 0": "drop"}, "'id > 0'", "str"),
            ({"id > 0": None}, "'id > 0'", "NoneType"),
        ],
        ids=["list-of-strings", "mapping-to-string", "mapping-to-null"],
    )
    def test_rule_that_is_not_a_mapping(
        self, tmp_path, quarantine, document, label, actual_type
    ):
        error = _only(_errors(tmp_path, document, quarantine))

        assert error.code == "LHP-VAL-008"
        assert error.context["Expectation"] == label
        assert error.context["Actual Type"] == actual_type


class TestAccepted:
    def test_valid_files_in_every_shape_pass(self, tmp_path):
        documents = [
            [
                {"name": "a", "constraint": "a > 0", "failureAction": "FAIL"},
                {"message": "b", "expression": "b > 0", "type": "expect_or_drop"},
                {"constraint": "c > 0", "failureAction": None},
                {"constraint": "d > 0"},
            ],
            {
                "a > 0": {"action": "Drop", "name": "a"},
                "b > 0": {"action": None},
                "c > 0": {},
            },
            {"version": "1.0", "expectations": [{"name": "a", "constraint": "a > 0"}]},
        ]
        for document in documents:
            assert _errors(tmp_path, document) == []

    def test_json_fixture_shape_passes(self, tmp_path):
        (tmp_path / "rules.json").write_text(
            json.dumps(
                {
                    "version": "1.0",
                    "table": "customer",
                    "expectations": [
                        {
                            "name": "valid_custkey",
                            "expression": "customer_id IS NOT NULL",
                            "failureAction": "fail",
                        },
                        {
                            "name": "valid_phone",
                            "expression": "phone IS NULL",
                            "failureAction": "warn",
                        },
                    ],
                }
            )
        )

        assert validate_expectations_file("rules.json", tmp_path, _PREFIX) == []


class TestWrappedValue:
    @pytest.mark.parametrize("value", [None, "rules"], ids=["null", "string"])
    def test_wrapped_value_that_is_not_a_list_or_mapping(
        self, tmp_path, quarantine, value
    ):
        usage = " for quarantine mode" if quarantine else ""

        assert _errors(tmp_path, {"expectations": value}, quarantine) == [
            f"{_PREFIX}: Expectations file has unexpected format{usage}"
        ]
