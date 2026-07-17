"""Tests for core data models of LakehousePlumber."""

import pytest

from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    LoadSourceType,
    Preset,
    Template,
    TestActionType,
    TransformType,
    ViolationAction,
    WriteTarget,
    WriteTargetType,
)


class TestModels:
    def test_action_type_enum(self):
        assert ActionType.LOAD.value == "load"
        assert ActionType.TRANSFORM.value == "transform"
        assert ActionType.WRITE.value == "write"
        assert ActionType.TEST.value == "test"

    def test_test_type_enum(self):
        assert TestActionType is not None

        assert TestActionType.ROW_COUNT.value == "row_count"
        assert TestActionType.UNIQUENESS.value == "uniqueness"
        assert TestActionType.REFERENTIAL_INTEGRITY.value == "referential_integrity"
        assert TestActionType.COMPLETENESS.value == "completeness"
        assert TestActionType.RANGE.value == "range"
        assert TestActionType.SCHEMA_MATCH.value == "schema_match"
        assert TestActionType.ALL_LOOKUPS_FOUND.value == "all_lookups_found"
        assert TestActionType.CUSTOM_SQL.value == "custom_sql"
        assert TestActionType.CUSTOM_EXPECTATIONS.value == "custom_expectations"

    def test_violation_action_enum(self):
        assert ViolationAction is not None
        assert ViolationAction.FAIL.value == "fail"
        assert ViolationAction.WARN.value == "warn"

    def test_action_model(self):
        action = Action(
            name="test_action",
            type=ActionType.LOAD,
            source={"type": "cloudfiles", "path": "/test/path"},
            target="test_view",
            description="Test action",
        )
        assert action.name == "test_action"
        assert action.type == ActionType.LOAD
        assert action.target == "test_view"

    def test_action_depends_on_defaults_to_none(self):
        action = Action(name="test_action", type=ActionType.TRANSFORM)
        assert action.depends_on is None

    def test_action_depends_on_accepts_list(self):
        action = Action(
            name="test_action",
            type=ActionType.TRANSFORM,
            depends_on=["cat.sch.tbl"],
        )
        assert action.depends_on == ["cat.sch.tbl"]

    def test_write_target_accepts_tags_file(self):
        write_target = WriteTarget(
            type=WriteTargetType.STREAMING_TABLE,
            table="my_table",
            tags_file="tags/orders_tags.yaml",
        )
        assert write_target.tags_file == "tags/orders_tags.yaml"

    def test_write_target_tags_file_path_normalized(self):
        """Backslashes in write_target tags_file are normalized (mirrors table_schema)."""
        action = Action(
            name="write_with_tags_file",
            type=ActionType.WRITE,
            target="my_table",
            write_target={
                "type": "streaming_table",
                "catalog": "c",
                "schema": "s",
                "table": "my_table",
                "tags_file": "tags\\orders_tags.yaml",
            },
        )
        assert isinstance(action.write_target, dict)
        assert action.write_target["tags_file"] == "tags/orders_tags.yaml"

    def test_flowgroup_model(self):
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[
                Action(name="load_data", type=ActionType.LOAD, target="raw_data"),
                Action(
                    name="clean_data",
                    type=ActionType.TRANSFORM,
                    source="raw_data",
                    target="clean_data",
                ),
            ],
        )
        assert flowgroup.pipeline == "test_pipeline"
        assert len(flowgroup.actions) == 2
        assert flowgroup.presets == ["bronze_layer"]

    def test_preset_model(self):
        preset = Preset(
            name="bronze_layer",
            version="1.0",
            extends="base_preset",
            description="Bronze layer preset",
            defaults={"schema_evolution": "addNewColumns"},
        )
        assert preset.name == "bronze_layer"
        assert preset.extends == "base_preset"
        assert preset.defaults.get("schema_evolution") == "addNewColumns"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
