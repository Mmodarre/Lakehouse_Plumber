"""Unit tests for the webapp related-files extractor service.

Tests the pure extraction logic that identifies external file references
from a raw flowgroup YAML dict across the known file-path-bearing fields.
"""

from pathlib import Path

import pytest

from lhp.webapp.services.related_files_extractor import (
    RelatedFile,
    extract_related_files,
)

pytestmark = pytest.mark.webapp


def _make_fg(actions: list[dict], name: str = "test_fg") -> dict:
    """Create a minimal raw flowgroup YAML dict for testing."""
    return {"pipeline": "test_pipeline", "flowgroup": name, "actions": actions}


def _make_action(name: str = "test_action", **kwargs) -> dict:
    """Create a raw action dict with given overrides."""
    action = {"name": name, "type": "load"}
    action.update(kwargs)
    return action


class TestActionLevelFields:
    """Extracts sql_path, expectations_file, schema_file, module_path."""

    def test_extracts_sql_path(self, tmp_path: Path):
        (tmp_path / "sql").mkdir()
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
        assert result[0].exists is True

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
    """Extracts from action.source when it's a dict (load actions)."""

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
        fg = _make_fg([_make_action(source={"schema_file": "schemas/source.yaml"})])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].field == "source.schema_file"
        assert result[0].category == "schema"

    def test_source_schema_file_path_detected(self, tmp_path: Path):
        """source.schema is ambiguous — file path detected by _is_file_path."""
        fg = _make_fg([_make_action(source={"schema": "schemas/customer.ddl"})])
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
                        "options": {"cloudFiles.schemaHints": "schemas/hints.yaml"},
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
                        "options": {"cloudFiles.schemaHints": "id INT, name STRING"},
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 0

    def test_source_as_string_ignored(self, tmp_path: Path):
        """Transform actions use source as a view-name string — not a file ref."""
        fg = _make_fg([_make_action(source="some_input_view")])
        result = extract_related_files(fg, tmp_path)

        assert result == []


class TestBareSchemaNameResolution:
    """Bare schema names resolve to the schemas/<name>.<ext> project layout."""

    def test_bare_schema_file_resolves_to_schemas_yaml(self, tmp_path: Path):
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "nation_schema.yaml").write_text("name: n\ncolumns: []")

        fg = _make_fg([_make_action(schema_file="nation_schema")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "schemas/nation_schema.yaml"
        assert result[0].category == "schema"
        assert result[0].field == "schema_file"
        assert result[0].exists is True

    def test_bare_schema_file_resolves_yml_extension(self, tmp_path: Path):
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "part_schema.yml").write_text("name: p\ncolumns: []")

        fg = _make_fg([_make_action(schema_file="part_schema")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "schemas/part_schema.yml"
        assert result[0].exists is True

    def test_bare_schema_name_missing_still_reports_yaml_candidate(
        self, tmp_path: Path
    ):
        """A bare name with no on-disk file yields the .yaml candidate, exists=False."""
        fg = _make_fg([_make_action(schema_file="ghost_schema")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "schemas/ghost_schema.yaml"
        assert result[0].exists is False

    def test_bare_source_schema_file_resolves(self, tmp_path: Path):
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "orders_schema.yaml").write_text("name: o\ncolumns: []")

        fg = _make_fg([_make_action(source={"schema_file": "orders_schema"})])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "schemas/orders_schema.yaml"
        assert result[0].field == "source.schema_file"
        assert result[0].exists is True

    def test_literal_schema_path_unchanged(self, tmp_path: Path):
        """A value that is already a path keeps literal behaviour (no schemas/ prefix)."""
        (tmp_path / "custom").mkdir()
        (tmp_path / "custom" / "s.yaml").write_text("name: s\ncolumns: []")

        fg = _make_fg([_make_action(schema_file="custom/s.yaml")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "custom/s.yaml"
        assert result[0].exists is True

    def test_schema_token_not_treated_as_bare_name(self, tmp_path: Path):
        """A substitution token is left literal, not rewritten to schemas/${...}.yaml."""
        fg = _make_fg([_make_action(schema_file="${raw_schema}")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "${raw_schema}"
        assert result[0].exists is False


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
                    write_target={"type": "sink", "module_path": "sinks/custom_sink.py"}
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
                            "source_function": {"file": "cdc/snapshot_loader.py"}
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


class TestWriteTargetTagsFile:
    """Extracts write_target.tags_file into the 'tags' category."""

    def test_extracts_write_target_tags_file(self, tmp_path: Path):
        (tmp_path / "uc_tags").mkdir()
        (tmp_path / "uc_tags" / "table_tags.yaml").write_text("owner: data-eng")

        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "streaming_table",
                        "tags_file": "uc_tags/table_tags.yaml",
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "uc_tags/table_tags.yaml"
        assert result[0].category == "tags"
        assert result[0].field == "write_target.tags_file"
        assert result[0].exists is True

    def test_write_target_tags_file_missing_marked_false(self, tmp_path: Path):
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "streaming_table",
                        "tags_file": "uc_tags/absent.yaml",
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "uc_tags/absent.yaml"
        assert result[0].category == "tags"
        assert result[0].field == "write_target.tags_file"
        assert result[0].exists is False

    def test_write_target_inline_tags_ignored(self, tmp_path: Path):
        """An inline tags dict is not a file reference — yields nothing."""
        fg = _make_fg(
            [
                _make_action(
                    write_target={
                        "type": "streaming_table",
                        "tags": {"owner": "data-eng", "layer": "silver"},
                    }
                )
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert result == []


class TestSqlAndPythonTogether:
    """A flowgroup referencing both a SQL file and a python source."""

    def test_sql_file_and_python_source(self, tmp_path: Path):
        (tmp_path / "sql").mkdir()
        (tmp_path / "sql" / "cleanse.sql").write_text("SELECT 1")
        (tmp_path / "py").mkdir()
        (tmp_path / "py" / "loader.py").write_text("def f(): pass")

        fg = _make_fg(
            [
                _make_action(name="bronze", sql_path="sql/cleanse.sql"),
                _make_action(
                    name="load_py",
                    source={"type": "python", "module_path": "py/loader.py"},
                ),
            ]
        )
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 2
        by_cat = {r.category: r for r in result}
        assert by_cat["sql"].path == "sql/cleanse.sql"
        assert by_cat["sql"].exists is True
        assert by_cat["python"].path == "py/loader.py"
        assert by_cat["python"].field == "source.module_path"
        assert by_cat["python"].exists is True


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
    """Results sorted by category order (sql -> schema -> expectations -> python)."""

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

    def test_missing_file_referenced_but_nonexistent(self, tmp_path: Path):
        """A referenced file that does not exist is still returned, exists=False."""
        fg = _make_fg([_make_action(sql_path="sql/missing.sql")])
        result = extract_related_files(fg, tmp_path)

        assert len(result) == 1
        assert result[0].path == "sql/missing.sql"
        assert result[0].exists is False


class TestNoFileRefs:
    """Flowgroups with no external file references return an empty list."""

    def test_empty_actions_returns_empty(self, tmp_path: Path):
        fg = _make_fg([])
        result = extract_related_files(fg, tmp_path)

        assert result == []

    def test_missing_actions_key_returns_empty(self, tmp_path: Path):
        fg = {"pipeline": "p", "flowgroup": "fg"}
        result = extract_related_files(fg, tmp_path)

        assert result == []

    def test_actions_without_file_refs_returns_empty(self, tmp_path: Path):
        fg = _make_fg([_make_action(name="simple", sql="SELECT 1")])
        result = extract_related_files(fg, tmp_path)

        assert result == []


class TestTemplateDriven:
    """use_template flowgroups: template-side refs are NOT resolved in v1."""

    def test_use_template_with_no_inline_actions(self, tmp_path: Path):
        """A template-driven flowgroup with no inline actions yields no refs."""
        fg = {
            "pipeline": "p",
            "flowgroup": "templated_fg",
            "use_template": "ingestion_template",
            "template_parameters": {"table": "customers"},
        }
        result = extract_related_files(fg, tmp_path)

        # v1 limitation: template-side file references are not resolved.
        assert result == []

    def test_use_template_does_not_crash_and_walks_inline_actions(self, tmp_path: Path):
        """If a use_template flowgroup also has inline actions, those are walked."""
        fg = {
            "pipeline": "p",
            "flowgroup": "templated_fg",
            "use_template": "ingestion_template",
            "actions": [_make_action(name="inline", sql_path="sql/inline.sql")],
        }
        result = extract_related_files(fg, tmp_path)

        # Only the inline ref is found; the template's own refs are deferred.
        assert len(result) == 1
        assert result[0].path == "sql/inline.sql"


class TestPathNormalization:
    """Backslash paths are normalized to forward slashes."""

    def test_backslash_normalized(self, tmp_path: Path):
        fg = _make_fg([_make_action(sql_path="sql\\query.sql")])
        result = extract_related_files(fg, tmp_path)

        assert result[0].path == "sql/query.sql"


def test_related_file_record_shape(tmp_path: Path):
    """The record exposes exactly the JSON fields the frontend consumes."""
    fg = _make_fg([_make_action(name="a", sql_path="sql/q.sql")])
    result = extract_related_files(fg, tmp_path)

    rf = result[0]
    assert isinstance(rf, RelatedFile)
    assert rf.path == "sql/q.sql"
    assert rf.category == "sql"
    assert rf.action_name == "a"
    assert rf.field == "sql_path"
    assert rf.exists is False
