"""``write_target.expectations_file`` renders ``@dp.expect_all*`` on a materialized view.

The decorators go below ``@dp.materialized_view(...)`` and above the function,
the order the workspace spike proved keeps the view incremental. Without the
key the output is unchanged. Real files on disk, no mocks.
"""

import json
from pathlib import Path

import pytest
import yaml

from lhp.errors import LHPError
from lhp.generators.write import MaterializedViewWriteGenerator
from lhp.generators.write.materialized_view import assemble_expectations
from lhp.models import Action

pytestmark = pytest.mark.unit

_WARN = {"name": "positive_amount", "constraint": "amount > 0", "failureAction": "warn"}
_DROP = {
    "name": "has_region",
    "constraint": "region IS NOT NULL",
    "failureAction": "drop",
}
_FAIL = {"name": "valid_id", "constraint": "id IS NOT NULL", "failureAction": "fail"}

_EXPECT_ALL = '@dp.expect_all({"positive_amount": "amount > 0"})'
_EXPECT_DROP = '@dp.expect_all_or_drop({"has_region": "region IS NOT NULL"})'
_EXPECT_FAIL = '@dp.expect_all_or_fail({"valid_id": "id IS NOT NULL"})'


def _action(**write_target) -> Action:
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


def _generate(project: Path, rules, name: str = "gate.yaml", **write_target) -> str:
    path = project / "expectations" / name
    path.parent.mkdir(exist_ok=True)
    path.write_text(
        json.dumps(rules) if name.endswith(".json") else yaml.safe_dump(rules)
    )
    action = _action(expectations_file=f"expectations/{name}", **write_target)
    return MaterializedViewWriteGenerator().generate(action, {"project_root": project})


def _decorators(code: str) -> list:
    """The ``@dp.expect*`` lines, checked to sit right above the ``def``."""
    lines = code.splitlines()
    end = next(i for i, line in enumerate(lines) if line.startswith("def "))
    found = [line for line in lines[:end] if line.startswith("@dp.expect")]
    assert lines[0] == "@dp.materialized_view("
    assert lines[end - len(found) : end] == found
    return found


class TestDecorators:
    def test_without_the_key_there_are_no_expectations(self, tmp_path):
        code = MaterializedViewWriteGenerator().generate(
            _action(), {"project_root": tmp_path}
        )

        assert "expect" not in code
        assert _decorators(code) == []

    @pytest.mark.parametrize(
        "rules,expected",
        [
            ([_WARN], [_EXPECT_ALL]),
            ([_DROP], [_EXPECT_DROP]),
            ([_FAIL], [_EXPECT_FAIL]),
            ([_WARN, _FAIL], [_EXPECT_ALL, _EXPECT_FAIL]),
            ([_DROP, _FAIL], [_EXPECT_DROP, _EXPECT_FAIL]),
            ([_FAIL, _DROP, _WARN], [_EXPECT_ALL, _EXPECT_DROP, _EXPECT_FAIL]),
        ],
        ids=["warn", "drop", "fail", "warn+fail", "drop+fail", "all"],
    )
    def test_each_category_gets_its_decorator_in_a_fixed_order(
        self, tmp_path, rules, expected
    ):
        code = _generate(tmp_path, rules)

        assert _decorators(code) == expected
        assert code.startswith("@dp.materialized_view(\n")

    def test_rules_of_one_category_share_a_decorator_in_file_order(self, tmp_path):
        second = {"name": "valid_ts", "constraint": "ts IS NOT NULL", "type": "expect"}

        code = _generate(tmp_path, [_WARN, second])

        assert _decorators(code) == [
            '@dp.expect_all({"positive_amount": "amount > 0", '
            '"valid_ts": "ts IS NOT NULL"})'
        ]

    def test_mapping_format_file(self, tmp_path):
        rules = {
            "id IS NOT NULL": {"action": "fail", "name": "valid_id"},
            "amount > 0": {"action": "warn", "name": "positive_amount"},
            "region IS NOT NULL": {"action": "drop", "name": "has_region"},
        }

        code = _generate(tmp_path, rules, "gate.json")

        assert _decorators(code) == [_EXPECT_ALL, _EXPECT_DROP, _EXPECT_FAIL]

    def test_source_view_mv_reads_in_batch_under_the_decorators(self, tmp_path):
        path = tmp_path / "expectations" / "gate.yaml"
        path.parent.mkdir()
        path.write_text(yaml.safe_dump([_FAIL]))
        action = Action(
            name="write_gate",
            type="write",
            source="v_orders",
            write_target={
                "type": "materialized_view",
                "catalog": "main",
                "schema": "gold",
                "table": "gate",
                "expectations_file": "expectations/gate.yaml",
            },
        )

        code = MaterializedViewWriteGenerator().generate(
            action, {"project_root": tmp_path}
        )

        assert _decorators(code) == [_EXPECT_FAIL]
        assert 'df = spark.read.table("v_orders")' in code

    def test_constraint_text_is_escaped_as_a_python_literal(self, tmp_path):
        rule = {"name": 'quote"d', "constraint": "note <> 'it''s'"}

        code = _generate(tmp_path, [rule])

        assert _decorators(code) == [
            '@dp.expect_all({"quote\\"d": "note <> \'it\'\'s\'"})'
        ]
        compile(code, "<generated>", "exec")


class TestFileResolution:
    def test_backslash_path_is_normalised_by_the_model(self, tmp_path):
        (tmp_path / "expectations").mkdir()
        (tmp_path / "expectations" / "gate.yaml").write_text(yaml.safe_dump([_FAIL]))
        action = _action(expectations_file="expectations\\gate.yaml")

        assert action.write_target["expectations_file"] == "expectations/gate.yaml"
        code = MaterializedViewWriteGenerator().generate(
            action, {"project_root": tmp_path}
        )
        assert _decorators(code) == [_EXPECT_FAIL]

    def test_missing_file_raises_the_loader_error(self, tmp_path):
        action = _action(expectations_file="expectations/absent.yaml")

        with pytest.raises(LHPError) as exc_info:
            MaterializedViewWriteGenerator().generate(
                action, {"project_root": tmp_path}
            )

        assert exc_info.value.code == "LHP-IO-001"

    def test_unvalidated_rule_is_an_internal_error(self, tmp_path):
        bad = {"name": "x", "constraint": "x > 0", "failureAction": "fial"}

        with pytest.raises(AssertionError, match="fial"):
            _generate(tmp_path, [bad])


class TestAssembleExpectations:
    def test_no_sources_means_no_expectations(self):
        assert assemble_expectations([]) == ({}, {}, {})

    def test_one_source_is_split_by_category(self):
        assert assemble_expectations([[_WARN, _DROP, _FAIL]]) == (
            {"positive_amount": "amount > 0"},
            {"has_region": "region IS NOT NULL"},
            {"valid_id": "id IS NOT NULL"},
        )

    def test_sources_merge_in_order_across_formats(self):
        mapping = {"b > 0": {"action": "warn", "name": "b"}}

        warn, drop, fail = assemble_expectations([[_WARN], mapping, [_FAIL]])

        assert list(warn) == ["positive_amount", "b"]
        assert drop == {}
        assert fail == {"valid_id": "id IS NOT NULL"}

    def test_name_shared_across_sources_is_an_internal_error(self):
        clash = {"name": "valid_id", "constraint": "id > 0", "failureAction": "warn"}

        with pytest.raises(AssertionError, match="'valid_id'"):
            assemble_expectations([[_FAIL], [clash]])

    def test_name_shared_across_categories_is_an_internal_error(self):
        clash = {"name": "valid_id", "constraint": "id > 0", "failureAction": "warn"}

        with pytest.raises(AssertionError, match="'valid_id'"):
            assemble_expectations([[_FAIL, clash]])
