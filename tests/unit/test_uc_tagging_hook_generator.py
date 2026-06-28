"""Unit tests for the UC tagging hook generator."""

import sys
import types
from contextlib import contextmanager

import pytest

from lhp.core.codegen.uc_tagging import build_uc_tagging_hook_files
from lhp.core.codegen.uc_tagging_hook_generator import (
    HOOK_FILENAME,
    UCTaggingHookGenerator,
)
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
        # Trigger: tag during the run on update_progress RUNNING + terminal states.
        assert 'event_type") != "update_progress"' in content
        assert '"RUNNING"' in content
        assert "_TERMINAL_PIPELINE_STATES" in content
        for state in ("COMPLETED", "FAILED", "CANCELED"):
            assert state in content
        # Per-entity tracking + single terminal pass (no pipeline-level _applied).
        assert "_tagged" in content
        assert "_update_terminated" in content
        assert "_applied" not in content
        assert "_match_fqn" not in content
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
        # Snapshot read at MODULE IMPORT (not in a decorated fn → no collect warning);
        # a read failure is CAUGHT and stashed, then re-raised by the hook as a warning.
        assert "_snapshot_error = str(e)" in content
        assert "_existing_tags = _fetch_existing_tags()" in content
        # The import-time call must NOT be inside _ensure_snapshot_loaded (removed).
        assert "_ensure_snapshot_loaded" not in content
        # "does not exist" during RUNNING is suppressed (table not materialised yet).
        assert "_ABSENT_MARKERS" in content
        assert "does not exist" in content
        # Failures surface as non-blocking event-log warnings during the run.
        assert "raise RuntimeError" in content
        assert "[LHP UC Tagging] WARNING:" in content
        assert "[LHP UC Tagging] ERROR:" in content
        # A fixed, small consecutive-failure budget (one combined RUNNING raise + one
        # terminal raise per run; counter resets each update).
        assert "max_allowable_consecutive_failures=3)" in content
        # New prefix everywhere; old prefix gone.
        assert "[LHP Tagging]" not in content
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


@contextmanager
def _fake_pyspark_and_sdk(collect_result=None, collect_error=None, do_handler=None):
    """Inject fake pyspark.pipelines / pyspark.sql / databricks.sdk so the rendered
    hook can be exec()'d and driven without a real Spark/SDK.

    Yields a state dict: ``hooks`` (registered @dp.on_event_hook fns) and ``calls``
    (recorded api_client.do invocations). ``collect_error`` makes the snapshot read
    raise; ``do_handler(method, path, body)`` may raise to simulate a tag failure.
    """
    state = {"hooks": [], "calls": []}

    pipelines_mod = types.ModuleType("pyspark.pipelines")

    def on_event_hook(max_allowable_consecutive_failures=None):
        def deco(fn):
            state["hooks"].append(fn)
            return fn

        return deco

    pipelines_mod.on_event_hook = on_event_hook

    sql_mod = types.ModuleType("pyspark.sql")

    class _DF:
        def collect(self):
            if collect_error is not None:
                raise collect_error
            return collect_result or []

    class _Builder:
        def getOrCreate(self):
            return self

        def sql(self, _query):
            return _DF()

    class SparkSession:
        builder = _Builder()

    sql_mod.SparkSession = SparkSession

    pyspark_mod = types.ModuleType("pyspark")
    pyspark_mod.pipelines = pipelines_mod
    pyspark_mod.sql = sql_mod

    sdk_mod = types.ModuleType("databricks.sdk")

    class _Api:
        def do(self, method, path, body=None, query=None):
            state["calls"].append({"method": method, "path": path, "body": body})
            if do_handler is not None:
                do_handler(method, path, body)

    class WorkspaceClient:
        def __init__(self):
            self.api_client = _Api()

    sdk_mod.WorkspaceClient = WorkspaceClient
    databricks_mod = types.ModuleType("databricks")
    databricks_mod.sdk = sdk_mod

    keys = ("pyspark", "pyspark.pipelines", "pyspark.sql", "databricks", "databricks.sdk")
    saved = {k: sys.modules.get(k) for k in keys}
    sys.modules.update(
        {
            "pyspark": pyspark_mod,
            "pyspark.pipelines": pipelines_mod,
            "pyspark.sql": sql_mod,
            "databricks": databricks_mod,
            "databricks.sdk": sdk_mod,
        }
    )
    try:
        yield state
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


def _exec_hook(content):
    ns = {}
    exec(compile(content, "_uc_tagging_hook.py", "exec"), ns)
    return ns


def _evt(state):
    return {"event_type": "update_progress", "details": {"update_progress": {"state": state}}}


@pytest.mark.unit
class TestUCTaggingHookRuntime:
    """Exercise the rendered hook's RUNTIME behavior with fake spark + SDK."""

    def test_snapshot_failure_is_nonblocking_then_warns_on_running(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        with _fake_pyspark_and_sdk(collect_error=RuntimeError("no SELECT grant")) as st:
            ns = _exec_hook(content)  # import must NOT raise despite the failed read
            assert ns["_snapshot_error"] is not None
            hook = st["hooks"][0]

            # RUNNING: create-only tagging happens AND the snapshot failure is raised
            # as a during-run WARNING.
            with pytest.raises(RuntimeError) as exc:
                hook(_evt("RUNNING"))
            assert "WARNING" in str(exc.value)
            assert "could not read existing tag state" in str(exc.value)
            assert any(c["method"] == "POST" for c in st["calls"])  # created the tag
            assert "prod.sales.orders" in ns["_tagged"]

            # Second RUNNING: already warned + already tagged -> no new work, no raise.
            before = len(st["calls"])
            hook(_evt("RUNNING"))
            assert len(st["calls"]) == before

    def test_running_suppresses_absent_then_terminal_tags_it(self, tmp_path):
        a1 = _write_action(
            name="w_orders", write_target=_st_target(table="orders", tags={"team": "x"})
        )
        a2 = _write_action(
            name="w_cust",
            write_target=_st_target(table="customers", tags={"team": "y"}),
        )
        content = _build([a1, a2], root=tmp_path)[HOOK_FILENAME]

        materialized = {"customers": False}

        def do_handler(method, path, body):
            target = ((body or {}).get("entity_name", "")) or path
            if "customers" in target and not materialized["customers"]:
                raise RuntimeError(f"Table '{target}' does not exist")

        with _fake_pyspark_and_sdk(collect_result=[], do_handler=do_handler) as st:
            ns = _exec_hook(content)
            assert ns["_snapshot_error"] is None
            hook = st["hooks"][0]

            # RUNNING: orders tagged; customers "does not exist" -> suppressed (no raise).
            hook(_evt("RUNNING"))
            assert "prod.sales.orders" in ns["_tagged"]
            assert "prod.sales.customers" not in ns["_tagged"]

            # customers materializes; first terminal event tags it.
            materialized["customers"] = True
            hook(_evt("COMPLETED"))
            assert "prod.sales.customers" in ns["_tagged"]
            assert ns["_update_terminated"] is True

            # Second terminal event is a no-op (single terminal pass).
            before = len(st["calls"])
            hook(_evt("FAILED"))
            assert len(st["calls"]) == before

    def test_real_error_raises_warning_on_running(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        def do_handler(method, path, body):
            raise RuntimeError("PERMISSION_DENIED: APPLY TAG")

        with _fake_pyspark_and_sdk(collect_result=[], do_handler=do_handler) as st:
            ns = _exec_hook(content)
            hook = st["hooks"][0]
            with pytest.raises(RuntimeError) as exc:
                hook(_evt("RUNNING"))
            assert "tag operation(s) failed" in str(exc.value)
            assert "PERMISSION_DENIED" in str(exc.value)
            # A real (non-absent) error is NOT marked tagged -> retried at terminal.
            assert "prod.sales.orders" not in ns["_tagged"]

    def test_ignores_non_update_progress_and_non_running_states(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        with _fake_pyspark_and_sdk(collect_result=[]) as st:
            _exec_hook(content)
            hook = st["hooks"][0]
            hook({"event_type": "flow_progress", "details": {}})
            hook(_evt("INITIALIZING"))
            hook(_evt("WAITING_FOR_RESOURCES"))
            assert st["calls"] == []  # nothing tagged on these
