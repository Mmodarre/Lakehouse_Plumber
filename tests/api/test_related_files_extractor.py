"""Unit tests for the related files extractor service.

Tests the pure extraction logic that identifies external file references
from FlowGroup actions across all 14 field paths.
"""

import pytest
from pathlib import Path

from lhp.api.services.related_files_extractor import (
    RelatedFile,
    extract_related_files,
)
from lhp.models.config import Action, ActionType, FlowGroup


pytestmark = pytest.mark.api


def _make_fg(actions: list[Action], name: str = "test_fg") -> FlowGroup:
    """Create a minimal FlowGroup for testing."""
    return FlowGroup(pipeline="test_pipeline", flowgroup=name, actions=actions)


def _make_action(name: str = "test_action", **kwargs) -> Action:
    """Create an Action with given overrides."""
    return Action(name=name, type=ActionType.LOAD, **kwargs)


class TestActionLevelFields:
    """Extracts sql_path, expectations_file, schema_file, module_path."""

    def test_extracts_sql_path(self, tmp_path: Path):
        (tmp_path / "sql" / "query.sql").parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "sql" / "query.sql").write_text("SELECT 1")

        fg = _make_fg([_make_action(sql_path="sql/query.sql")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "sql/query.sql"
        assert result[0].category == "sql"
        assert result[0].field == "sql_path"
        assert result[0].exists is True

    def test_extracts_expectations_file(self, tmp_path: Path):
        (tmp_path / "expectations.json").write_text("{}")

        fg = _make_fg([_make_action(expectations_file="expectations.json")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].category == "expectations"
        assert result[0].field == "expectations_file"

    def test_extracts_schema_file(self, tmp_path: Path):
        fg = _make_fg([_make_action(schema_file="schemas/customer.yaml")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].category == "schema"
        assert result[0].field == "schema_file"
        assert result[0].exists is False  # file doesn't exist

    def test_extracts_module_path(self, tmp_path: Path):
        (tmp_path / "modules").mkdir()
        (tmp_path / "modules" / "transform.py").write_text("def run(): pass")

        fg = _make_fg([_make_action(module_path="modules/transform.py")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].category == "python"
        assert result[0].field == "module_path"
        assert result[0].exists is True

    def test_extracts_multiple_fields_from_one_action(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    sql_path="sql/query.sql",
                    module_path="modules/transform.py",
                    expectations_file="quality/checks.json",
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 3
        categories = {r.category for r in result}
        assert categories == {"sql", "python", "expectations"}


class TestSourceDictFields:
    """Extracts from action.source when it's a dict."""

    def test_extracts_source_sql_path(self, tmp_path: Path):
        fg = _make_fg(
            [_make_action(source={"type": "sql", "sql_path": "sql/source.sql"})]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "source.sql_path"
        assert result[0].category == "sql"

    def test_extracts_source_module_path(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    source={"type": "python", "module_path": "modules/loader.py"}
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "source.module_path"
        assert result[0].category == "python"

    def test_extracts_source_schema_file(self, tmp_path: Path):
        fg = _make_fg(
            [_make_action(source={"schema_file": "schemas/source.yaml"})]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "source.schema_file"
        assert result[0].category == "schema"

    def test_source_schema_file_path_detected(self, tmp_path: Path):
        """source.schema is ambiguous — file path detected by is_file_path."""
        fg = _make_fg(
            [_make_action(source={"schema": "schemas/customer.ddl"})]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "source.schema"
        assert result[0].category == "schema"

    def test_source_schema_inline_ddl_ignored(self, tmp_path: Path):
        """Inline DDL like 'customer_id BIGINT, name STRING' is NOT a file path."""
        fg = _make_fg(
            [_make_action(source={"schema": "customer_id BIGINT, name STRING"})]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 0

    def test_cloudfiles_schema_hints_file_path(self, tmp_path: Path):
        """cloudFiles.schemaHints as file path is extracted."""
        fg = _make_fg(
            [
                _make_action(
                    source={
                        "type": "cloudfiles",
                        "options": {
                            "cloudFiles.schemaHints": "schemas/hints.yaml"
                        },
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "source.options.cloudFiles.schemaHints"
        assert result[0].category == "schema"

    def test_cloudfiles_schema_hints_inline_ignored(self, tmp_path: Path):
        """Inline schemaHints like 'id INT, name STRING' are NOT file paths."""
        fg = _make_fg(
            [
                _make_action(
                    source={
                        "type": "cloudfiles",
                        "options": {
                            "cloudFiles.schemaHints": "id INT, name STRING"
                        },
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 0


class TestWriteTargetFields:
    """Extracts from action.write_target (dict form)."""

    def test_extracts_write_target_table_schema_file(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "streaming_table",
                        "table_schema": "schemas/table.yaml",
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "write_target.table_schema"
        assert result[0].category == "schema"

    def test_write_target_inline_schema_ignored(self, tmp_path: Path):
        """Inline DDL in table_schema is not a file path."""
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "streaming_table",
                        "table_schema": "id BIGINT, name STRING",
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 0

    def test_extracts_write_target_sql_path(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "materialized_view",
                        "sql_path": "sql/mv_query.sql",
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "write_target.sql_path"
        assert result[0].category == "sql"

    def test_extracts_write_target_module_path(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "sink",
                        "module_path": "sinks/custom_sink.py",
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "write_target.module_path"
        assert result[0].category == "python"

    def test_extracts_snapshot_cdc_source_function_file(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "streaming_table",
                        "snapshot_cdc_config": {
                            "source_function": {
                                "file": "cdc/snapshot_loader.py"
                            }
                        },
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == (
            "write_target.snapshot_cdc_config.source_function.file"
        )
        assert result[0].category == "python"


class TestDeduplication:
    """Same file referenced by multiple actions is returned only once."""

    def test_deduplicates_by_path(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(name="action_a", sql_path="sql/shared.sql"),
                _make_action(name="action_b", sql_path="sql/shared.sql"),
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].action_name == "action_a"  # first occurrence wins

    def test_deduplicates_across_field_types(self, tmp_path: Path):
        """Same file referenced from action-level and source dict."""
        fg = _make_fg(
            [
                _make_action(
                    name="load_action",
                    module_path="modules/shared.py",
                    source={"module_path": "modules/shared.py"},
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1


class TestSorting:
    """Results sorted by category order (sql → schema → expectations → python)."""

    def test_sorted_by_category_then_path(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    name="action",
                    module_path="modules/b.py",
                    sql_path="sql/a.sql",
                    expectations_file="quality/checks.json",
                    schema_file="schemas/s.yaml",
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        categories = [r.category for r in result]
        assert categories == ["sql", "schema", "expectations", "python"]


class TestExistence:
    """Existing and non-existing files handled correctly."""

    def test_existing_file_marked_true(self, tmp_path: Path):
        (tmp_path / "sql").mkdir()
        (tmp_path / "sql" / "exists.sql").write_text("SELECT 1")

        fg = _make_fg([_make_action(sql_path="sql/exists.sql")])
        result = extract_related_files(fg, tmp_path)

        assert result[0].exists is True

    def test_missing_file_marked_false(self, tmp_path: Path):
        fg = _make_fg([_make_action(sql_path="sql/missing.sql")])
        result = extract_related_files(fg, tmp_path)

        assert result[0].exists is False


class TestEmptyActions:
    """Flowgroup with no actions returns empty list."""

    def test_empty_actions_returns_empty(self, tmp_path: Path):
        fg = _make_fg([])
        result = extract_related_files(fg, tmp_path)

        assert result == []

    def test_actions_without_file_refs_returns_empty(self, tmp_path: Path):
        fg = _make_fg([_make_action(name="simple", sql="SELECT 1")])
        result = extract_related_files(fg, tmp_path)

        assert result == []


class TestPathNormalization:
    """Backslash paths are normalized to forward slashes."""

    def test_backslash_normalized(self, tmp_path: Path):
        fg = _make_fg([_make_action(sql_path="sql\\query.sql")])
        result = extract_related_files(fg, tmp_path)

        # Action's model_post_init normalizes \\ to /, so the extractor sees /
        assert result[0].path == "sql/query.sql"
