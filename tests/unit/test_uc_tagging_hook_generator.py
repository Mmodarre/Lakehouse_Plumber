"""Unit tests for the UC tagging hook generator."""

import pytest

from lhp.core.codegen.uc_tagging import build_uc_tagging_hook_files
from lhp.core.codegen.uc_tagging_hook_generator import HOOK_FILENAME, UCTaggingHookGenerator
from lhp.models import Action, ActionType, FlowGroup, ProjectConfig, UCTaggingConfig


def _config(uc_tagging=None):
    return ProjectConfig(name="test_project", uc_tagging=uc_tagging)


def _flowgroup(actions, pipeline="p1", name="fg1"):
    return FlowGroup(pipeline=pipeline, flowgroup=name, actions=actions)


def _write_action(name="w", write_target=None):
    return Action(
        name=name,
        type=ActionType.WRITE,
        source="v_src",
        write_target=write_target or {},
    )


def _st_target(table="orders", **extra):
    base = {
        "type": "streaming_table",
        "catalog": "prod",
        "schema": "sales",
        "table": table,
        "create_table": True,
    }
    base.update(extra)
    return base


_ENABLED = object()  # sentinel: default to an enabled uc_tagging block


def _build(actions, uc_tagging=_ENABLED, root=None):
    if uc_tagging is _ENABLED:
        uc_tagging = UCTaggingConfig()
    return build_uc_tagging_hook_files(
        pipeline_name="p1",
        flowgroups=[_flowgroup(actions)],
        project_config=_config(uc_tagging),
        project_root=root,
    )


@pytest.mark.unit
class TestUCTaggingHookGenerator:
    def test_disabled_returns_none(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        result = _build([action], uc_tagging=UCTaggingConfig(enabled=False), root=tmp_path)
        assert result is None

    def test_absent_block_enabled_by_default(self, tmp_path):
        # On by default: no uc_tagging block + declared tags → hook IS generated.
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        result = _build([action], uc_tagging=None, root=tmp_path)
        assert result is not None
        assert HOOK_FILENAME in result

    def test_no_tags_returns_none(self, tmp_path):
        action = _write_action(write_target=_st_target())
        assert _build([action], root=tmp_path) is None

    def test_table_tags_embedded_with_key_only(self, tmp_path):
        action = _write_action(
            write_target=_st_target(tags={"team": "data-eng", "pii": None, "n": 1})
        )
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert "'prod.sales.orders'" in content
        assert "'team': 'data-eng'" in content
        assert "'pii': ''" in content  # key-only normalized to empty string
        assert "'n': '1'" in content  # non-string coerced

    def test_create_table_false_excluded(self, tmp_path):
        action = _write_action(
            write_target=_st_target(create_table=False, tags={"team": "x"})
        )
        assert _build([action], root=tmp_path) is None

    def test_temporary_excluded(self, tmp_path):
        action = _write_action(
            write_target=_st_target(temporary=True, tags={"team": "x"})
        )
        assert _build([action], root=tmp_path) is None

    def test_sink_excluded(self, tmp_path):
        action = _write_action(
            write_target={"type": "sink", "sink_type": "delta", "tags": {"a": "b"}}
        )
        assert _build([action], root=tmp_path) is None

    def test_empty_tags_dropped_when_additive(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={}))
        assert _build([action], uc_tagging=UCTaggingConfig(), root=tmp_path) is None

    def test_empty_tags_kept_when_reconciling(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={}))
        result = _build(
            [action],
            uc_tagging=UCTaggingConfig(remove_undeclared_tags=True),
            root=tmp_path,
        )
        assert result is not None
        content = result[HOOK_FILENAME]
        assert "'prod.sales.orders': {}" in content
        assert "_REMOVE_UNDECLARED_TAGS = True" in content

    def test_remove_undeclared_false_by_default(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert "_REMOVE_UNDECLARED_TAGS = False" in content

    def test_hook_structure(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert "@dp.on_event_hook" in content
        # Trigger: tag everything once the pipeline reaches a terminal state.
        assert 'event_type") != "update_progress"' in content
        assert "_TERMINAL_PIPELINE_STATES" in content
        for state in ("COMPLETED", "FAILED", "CANCELED"):
            assert state in content
        assert "json.loads" in content  # details may arrive as a JSON string
        # Existing tag state read once from information_schema (no per-entity LIST),
        # aggregated to one row per entity (tags map) and filtered by table name only.
        assert "system.information_schema.table_tags" in content
        assert "system.information_schema.column_tags" in content
        assert "map_from_entries(collect_list(struct(tag_name, tag_value)))" in content
        assert "GROUP BY entity_type, entity_name" in content
        # Filter the RAW catalog/schema/table columns (prunable), not lower()/concat
        # expressions; no lower(concat_ws(...)) in the WHERE.
        assert "WHERE catalog_name IN (" in content
        assert "AND schema_name IN (" in content
        assert "AND table_name IN (" in content
        assert "lower(concat_ws(" not in content
        # The resolved query text is logged before execution.
        assert "Reading existing tag state from information_schema" in content
        assert "/api/2.1/unity-catalog/entity-tag-assignments" in content
        assert "from databricks.sdk import WorkspaceClient" in content
        # Thread pool sized by tag_update_concurrency (default 16).
        assert "ThreadPoolExecutor(max_workers=16)" in content
        # Failures raise so they surface in the pipeline event log.
        assert "raise RuntimeError" in content
        # An information_schema read failure raises with remediation guidance
        # (no silent degrade to create-only).
        assert "Failed to read existing tag state from" in content
        assert "SELECT on system.information_schema" in content
        # No SQL tag DDL path.
        assert "ALTER TABLE" not in content

    def test_thread_pool_concurrency_default_and_override(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        default = _build([action], uc_tagging=UCTaggingConfig(), root=tmp_path)[HOOK_FILENAME]
        assert "ThreadPoolExecutor(max_workers=16)" in default

        custom = _build(
            [action],
            uc_tagging=UCTaggingConfig(tag_update_concurrency=32),
            root=tmp_path,
        )[HOOK_FILENAME]
        assert "ThreadPoolExecutor(max_workers=32)" in custom

    def test_column_tags_from_yaml_schema(self, tmp_path):
        schema_file = tmp_path / "schemas" / "orders.yaml"
        schema_file.parent.mkdir(parents=True)
        schema_file.write_text(
            "name: orders\n"
            "columns:\n"
            "  - name: id\n"
            "    type: BIGINT\n"
            "  - name: email\n"
            "    type: STRING\n"
            "    tags:\n"
            "      classification: pii\n"
        )
        action = _write_action(
            write_target=_st_target(table_schema="schemas/orders.yaml")
        )
        result = _build([action], root=tmp_path)
        assert result is not None
        content = result[HOOK_FILENAME]
        assert "'prod.sales.orders'" in content
        assert "'email'" in content
        assert "'classification': 'pii'" in content

    def test_column_tags_ignored_for_inline_ddl(self, tmp_path):
        action = _write_action(
            write_target=_st_target(table_schema="id BIGINT, email STRING")
        )
        # Inline DDL carries no column tags, and no table tags here -> nothing to do
        assert _build([action], root=tmp_path) is None
