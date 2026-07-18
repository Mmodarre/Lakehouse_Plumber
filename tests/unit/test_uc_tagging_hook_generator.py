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
from lhp.errors import LHPError
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


def _build(
    actions, uc_tagging=_ENABLED, root=None, sandbox_active=False, substitution_mgr=None
):
    if uc_tagging is _ENABLED:
        uc_tagging = UCTaggingConfig()
    return build_uc_tagging_hook_files(
        pipeline_name="p1",
        flowgroups=[_flowgroup(actions)],
        project_config=_config(uc_tagging),
        project_root=root,
        sandbox_active=sandbox_active,
        substitution_mgr=substitution_mgr,
    )


_DEFAULT_TAGS = object()  # sentinel: default to {'team': 'data-eng'}


def _write_tags_file(
    tmp_path, rel, *, table="orders", tags=_DEFAULT_TAGS, columns=None, fmt="yaml"
):
    """Write a strict-format tags sidecar under ``tmp_path`` and return ``rel``.

    ``tags`` defaults to ``{'team': 'data-eng'}``; pass ``tags=None`` to OMIT the
    ``tags:`` key entirely (proves absent ≠ empty), or a dict for an explicit
    block. ``columns`` (column-name -> tag map) is omitted when ``None``.
    """
    if tags is _DEFAULT_TAGS:
        tags = {"team": "data-eng"}
    path = tmp_path / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        import json

        data = {"version": "1.0", "table": table}
        if tags is not None:
            data["tags"] = tags
        if columns is not None:
            data["column_tags"] = [{"name": c, "tags": t} for c, t in columns.items()]
        path.write_text(json.dumps(data))
    else:
        lines = ['version: "1.0"', f"table: {table}"]
        if tags is not None:
            lines.append("tags:")
            lines += [f'  {k}: "{v}"' for k, v in tags.items()]
        if columns is not None:
            if not columns:
                lines.append("column_tags: []")
            else:
                lines.append("column_tags:")
                for col, ctags in columns.items():
                    lines.append(f"  - name: {col}")
                    lines.append("    tags:")
                    lines += [f'      {k}: "{v}"' for k, v in ctags.items()]
        path.write_text("\n".join(lines) + "\n")
    return rel


def _table_tags_line(content):
    return next(
        line for line in content.splitlines() if line.startswith("_TABLE_TAGS = ")
    )


@pytest.mark.unit
class TestUCTaggingTagsFile:
    """Task 3: table tags resolved from an external ``tags_file`` sidecar at
    commit time, with a sandbox-aware literal ``table:`` cross-check.
    """

    def test_table_tags_from_yaml_file(self, tmp_path):
        _write_tags_file(tmp_path, "uc_tags/orders.yaml", tags={"team": "data-eng"})
        action = _write_action(write_target=_st_target(tags_file="uc_tags/orders.yaml"))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert "_TABLE_TAGS = {'prod.sales.orders': {'team': 'data-eng'}}" in content

    def test_table_tags_from_json_file(self, tmp_path):
        _write_tags_file(
            tmp_path, "uc_tags/orders.json", tags={"team": "data-eng"}, fmt="json"
        )
        action = _write_action(write_target=_st_target(tags_file="uc_tags/orders.json"))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert "_TABLE_TAGS = {'prod.sales.orders': {'team': 'data-eng'}}" in content

    def test_missing_file_raises_io_001(self, tmp_path):
        action = _write_action(
            write_target=_st_target(tags_file="uc_tags/missing.yaml")
        )
        with pytest.raises(LHPError) as exc_info:
            _build([action], root=tmp_path)
        assert exc_info.value.code == "LHP-IO-001"

    def test_table_mismatch_raises_cfg_067(self, tmp_path):
        _write_tags_file(tmp_path, "uc_tags/orders.yaml", table="other_table")
        action = _write_action(
            write_target=_st_target(table="orders", tags_file="uc_tags/orders.yaml")
        )
        with pytest.raises(LHPError) as exc_info:
            _build([action], root=tmp_path)
        assert exc_info.value.code == "LHP-CFG-067"

    def test_token_table_resolves_to_file_table(self, tmp_path):
        from lhp.core.processing.substitution import EnhancedSubstitutionManager

        _write_tags_file(
            tmp_path, "uc_tags/orders.yaml", table="orders", tags={"team": "data-eng"}
        )
        mgr = EnhancedSubstitutionManager()
        mgr.mappings["tbl"] = "orders"
        action = _write_action(
            write_target=_st_target(table="${tbl}", tags_file="uc_tags/orders.yaml")
        )
        content = _build([action], root=tmp_path, substitution_mgr=mgr)[HOOK_FILENAME]
        assert "_TABLE_TAGS = {'prod.sales.orders': {'team': 'data-eng'}}" in content

    def test_file_and_inline_byte_identical(self, tmp_path):
        tags = {"team": "data-eng", "cost_center": "1234"}
        _write_tags_file(tmp_path, "uc_tags/orders.yaml", tags=tags)
        file_action = _write_action(
            write_target=_st_target(tags_file="uc_tags/orders.yaml")
        )
        inline_action = _write_action(write_target=_st_target(tags=tags))
        file_content = _build([file_action], root=tmp_path)[HOOK_FILENAME]
        inline_content = _build([inline_action], root=tmp_path)[HOOK_FILENAME]
        assert _table_tags_line(file_content) == _table_tags_line(inline_content)

    def test_sandbox_skips_table_check(self, tmp_path):
        # Deliberately non-matching table: under --sandbox the cross-check is
        # skipped and the tags are still emitted.
        _write_tags_file(
            tmp_path,
            "uc_tags/orders.yaml",
            table="renamed_table",
            tags={"team": "data-eng"},
        )
        action = _write_action(
            write_target=_st_target(table="orders", tags_file="uc_tags/orders.yaml")
        )
        content = _build([action], root=tmp_path, sandbox_active=True)[HOOK_FILENAME]
        assert "_TABLE_TAGS = {'prod.sales.orders': {'team': 'data-eng'}}" in content


@pytest.mark.unit
class TestUCTaggingHookGenerator:
    def test_disabled_returns_none(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        result = _build(
            [action], uc_tagging=UCTaggingConfig(enabled=False), root=tmp_path
        )
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
        assert "_processed" in content
        assert "_tagged" not in content  # renamed to _processed
        assert "_update_terminated" in content
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
        # The resolved query text is logged before execution.
        assert "Reading existing tag state from information_schema" in content
        assert "/api/2.1/unity-catalog/entity-tag-assignments" in content
        assert "from databricks.sdk import WorkspaceClient" in content
        # S-1: path segments are URL-encoded (entity_name + key) on both the PATCH and
        # DELETE tag URLs, so FQNs/keys with special chars can't break out of the path.
        assert "import urllib.parse" in content
        assert "urllib.parse.quote(entity_name, safe='')" in content
        assert "urllib.parse.quote(key, safe='')" in content
        # PATCH wraps both segments, DELETE wraps both → 4 quote() calls total.
        assert content.count("quote(") == 4
        # S-2: transient REST errors (429/503/throttle) are retried with backoff via
        # _do_with_retry; reconcile's POST/PATCH/DELETE all route through it, so the
        # only raw api_client.do() call left is the one inside the helper.
        assert "import time" in content
        assert "def _do_with_retry(w, method, path, **kwargs):" in content
        assert "_TRANSIENT_MARKERS" in content
        assert "time.sleep(" in content
        assert content.count("_do_with_retry(") == 4  # 1 def + 3 reconcile call-sites
        assert content.count("w.api_client.do(") == 1  # only inside _do_with_retry
        # Thread pool sized by tag_update_concurrency (default 16).
        assert "ThreadPoolExecutor(max_workers=16)" in content
        # S-3: the ThreadPoolExecutor block body is inside a try, so a pool-level
        # failure degrades to the clean WARNING path instead of escaping as a traceback.
        assert "    try:\n        with ThreadPoolExecutor(" in content
        assert 'errors.append(("<thread-pool>", e))' in content
        # Snapshot read at MODULE IMPORT (not in a decorated fn → no collect warning);
        # a read failure is CAUGHT and stashed, then re-raised by the hook as a warning.
        assert "_snapshot_error = str(e)" in content
        assert "_existing_tags = _fetch_existing_tags()" in content
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
        assert "SELECT on system.information_schema" in content
        # No SQL tag DDL path.
        assert "ALTER TABLE" not in content

    def test_thread_pool_concurrency_default_and_override(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        default = _build([action], uc_tagging=UCTaggingConfig(), root=tmp_path)[
            HOOK_FILENAME
        ]
        assert "ThreadPoolExecutor(max_workers=16)" in default

        custom = _build(
            [action],
            uc_tagging=UCTaggingConfig(tag_update_concurrency=20),
            root=tmp_path,
        )[HOOK_FILENAME]
        assert "ThreadPoolExecutor(max_workers=20)" in custom

    def test_column_tags_from_tags_file(self, tmp_path):
        # Column tags now come from the tags_file ``column_tags:`` list.
        _write_tags_file(
            tmp_path,
            "uc_tags/orders.yaml",
            tags={"team": "data-eng"},
            columns={"email": {"classification": "pii"}},
        )
        action = _write_action(write_target=_st_target(tags_file="uc_tags/orders.yaml"))
        result = _build([action], root=tmp_path)
        assert result is not None
        content = result[HOOK_FILENAME]
        assert "'prod.sales.orders'" in content
        assert "'email'" in content
        assert "'classification': 'pii'" in content

    def test_column_tags_without_table_schema(self, tmp_path):
        # Headline capability: column tags come from tags_file even when the write
        # target declares NO table_schema at all.
        _write_tags_file(
            tmp_path,
            "uc_tags/orders.yaml",
            tags={"team": "data-eng"},
            columns={"email": {"classification": "pii"}},
        )
        action = _write_action(write_target=_st_target(tags_file="uc_tags/orders.yaml"))
        assert "table_schema" not in action.write_target
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert (
            "_COLUMN_TAGS = {'prod.sales.orders': "
            "{'email': {'classification': 'pii'}}}" in content
        )

    def test_table_schema_contributes_no_column_tags(self, tmp_path):
        # table_schema is no longer a tag source: a YAML schema file with per-column
        # tags contributes NOTHING, so with no tags/tags_file there is nothing to do.
        schema_file = tmp_path / "schemas" / "orders.yaml"
        schema_file.parent.mkdir(parents=True)
        schema_file.write_text(
            "name: orders\n"
            "columns:\n"
            "  - name: email\n"
            "    type: STRING\n"
            "    tags:\n"
            "      classification: pii\n"
        )
        yaml_action = _write_action(
            write_target=_st_target(table_schema="schemas/orders.yaml")
        )
        assert _build([yaml_action], root=tmp_path) is None
        # Inline DDL likewise contributes no column tags.
        ddl_action = _write_action(
            write_target=_st_target(table_schema="id BIGINT, email STRING")
        )
        assert _build([ddl_action], root=tmp_path) is None


@pytest.mark.unit
class TestUCTaggingHookContent:
    """Targeted delta coverage of the rendered tag-data literals.

    Full-fidelity coverage of the additive case now lives in the e2e suite
    (tests/e2e/test_uc_tagging_e2e.py, Pipeline A). Reconcile is excluded from
    e2e by scope, so it is kept here. These assert the raw-render single-quoted
    repr() literals (no ruff-format pass, unlike the e2e baselines).
    """

    def test_additive_table_and_column(self, tmp_path):
        # Both table and column tags come from ONE tags_file.
        _write_tags_file(
            tmp_path,
            "uc_tags/orders.yaml",
            tags={"team": "data-eng", "pii": ""},
            columns={"email": {"classification": "pii", "masked": ""}},
        )
        action = _write_action(write_target=_st_target(tags_file="uc_tags/orders.yaml"))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        # Table tags (key-only -> ''), column tags (incl. empty-string value),
        # additive mode. Raw render -> single-quoted repr() literals.
        assert (
            "_TABLE_TAGS = {'prod.sales.orders': {'team': 'data-eng', 'pii': ''}}"
            in content
        )
        assert (
            "_COLUMN_TAGS = {'prod.sales.orders': "
            "{'email': {'classification': 'pii', 'masked': ''}}}" in content
        )
        assert "_REMOVE_UNDECLARED_TAGS = False" in content

    def test_columns_only_tags_file_omits_table(self, tmp_path):
        # A tags_file with a ``column_tags:`` list but NO ``tags:`` key. The absent
        # ``tags:`` (parsed as None, not {}) omits the table from _TABLE_TAGS
        # entirely — even under reconcile it is NOT emitted as a managed empty set
        # that would wipe the table's live tags. Proves absent ≠ empty end to end.
        _write_tags_file(
            tmp_path,
            "uc_tags/orders.yaml",
            tags=None,
            columns={"email": {"classification": "pii"}},
        )
        action = _write_action(write_target=_st_target(tags_file="uc_tags/orders.yaml"))
        content = _build(
            [action],
            uc_tagging=UCTaggingConfig(remove_undeclared_tags=True),
            root=tmp_path,
        )[HOOK_FILENAME]
        assert "_TABLE_TAGS = {}" in content
        assert (
            "_COLUMN_TAGS = {'prod.sales.orders': "
            "{'email': {'classification': 'pii'}}}" in content
        )

    def test_reconcile_with_empty_set(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={}))
        content = _build(
            [action],
            uc_tagging=UCTaggingConfig(remove_undeclared_tags=True),
            root=tmp_path,
        )[HOOK_FILENAME]
        # Reconcile keeps the empty tag dict (additive mode would drop the entity).
        assert "_TABLE_TAGS = {'prod.sales.orders': {}}" in content
        assert "_COLUMN_TAGS = {}" in content
        assert "_REMOVE_UNDECLARED_TAGS = True" in content


@pytest.mark.unit
class TestUCTaggingTagValidation:
    """E-2: the materialized (post-substitution) tag key/value is validated against
    Unity Catalog's charset/length rules at the ``_normalize_tags`` chokepoint; an
    illegal tag raises ``LHP-CFG-066`` at generation time.
    """

    @pytest.mark.parametrize("key", ["cost:center", "cost-center"])
    def test_key_with_prohibited_char_raises(self, tmp_path, key):
        action = _write_action(write_target=_st_target(tags={key: "x"}))
        with pytest.raises(LHPError) as exc_info:
            _build([action], root=tmp_path)
        assert exc_info.value.code == "LHP-CFG-066"

    def test_value_with_leading_space_raises(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"team": " data-eng"}))
        with pytest.raises(LHPError) as exc_info:
            _build([action], root=tmp_path)
        assert exc_info.value.code == "LHP-CFG-066"

    def test_overlong_key_raises(self, tmp_path):
        action = _write_action(write_target=_st_target(tags={"k" * 257: "x"}))
        with pytest.raises(LHPError) as exc_info:
            _build([action], root=tmp_path)
        assert exc_info.value.code == "LHP-CFG-066"

    @pytest.mark.parametrize("key", ["class.name", "Domain/Subdomain"])
    def test_key_with_dot_or_slash_is_allowed(self, tmp_path, key):
        # UC tag keys legitimately contain '.' (system-governed tags) and '/'
        # (subdomain tags); generation must accept them, not raise LHP-CFG-066.
        action = _write_action(write_target=_st_target(tags={key: "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        # The tag survived normalization into the emitted _TABLE_TAGS map.
        assert f"{key!r}: 'x'" in content


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
            state["calls"].append(
                {"method": method, "path": path, "body": body, "query": query}
            )
            if do_handler is not None:
                do_handler(method, path, body)

    class WorkspaceClient:
        def __init__(self):
            self.api_client = _Api()

    sdk_mod.WorkspaceClient = WorkspaceClient
    databricks_mod = types.ModuleType("databricks")
    databricks_mod.sdk = sdk_mod

    keys = (
        "pyspark",
        "pyspark.pipelines",
        "pyspark.sql",
        "databricks",
        "databricks.sdk",
    )
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
    return {
        "event_type": "update_progress",
        "details": {"update_progress": {"state": state}},
    }


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
            assert "prod.sales.orders" in ns["_processed"]

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
            assert "prod.sales.orders" in ns["_processed"]
            assert "prod.sales.customers" not in ns["_processed"]

            # customers materializes; first terminal event tags it.
            materialized["customers"] = True
            hook(_evt("COMPLETED"))
            assert "prod.sales.customers" in ns["_processed"]
            assert ns["_update_terminated"] is True

            # Second terminal event is a no-op (single terminal pass).
            before = len(st["calls"])
            hook(_evt("FAILED"))
            assert len(st["calls"]) == before

    def test_real_error_warns_once_then_terminal_is_silent(self, tmp_path):
        # A real (non-absent) failure must warn ONCE on RUNNING and then be marked
        # _processed, so the terminal pass does not re-attempt it and emit a duplicate
        # event-log warning.
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        def do_handler(method, path, body):
            raise RuntimeError("PERMISSION_DENIED: APPLY TAG")

        with _fake_pyspark_and_sdk(collect_result=[], do_handler=do_handler) as st:
            ns = _exec_hook(content)
            hook = st["hooks"][0]

            # RUNNING: warns once, and the entity is marked processed despite failing.
            with pytest.raises(RuntimeError) as exc:
                hook(_evt("RUNNING"))
            assert "tag operation(s) failed" in str(exc.value)
            assert "PERMISSION_DENIED" in str(exc.value)
            assert "prod.sales.orders" in ns["_processed"]

            # Terminal: the failed-but-processed entity is NOT retried → no REST calls,
            # no second raise (no duplicate warning).
            before = len(st["calls"])
            hook(_evt("COMPLETED"))  # must not raise
            assert len(st["calls"]) == before

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

    def test_transient_error_is_retried_then_succeeds(self, tmp_path):
        # S-2: a transient REST error (429) is retried with backoff and eventually
        # succeeds, so the entity ends up _processed with NO event-log warning. Backoff
        # sleeps are neutralized by swapping the hook's `time` global for a no-op stub.
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        attempts = {"n": 0}

        def do_handler(method, path, body):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("429 Too Many Requests")
            # subsequent calls succeed

        slept = {"n": 0}

        class _SleepStub:
            @staticmethod
            def sleep(_secs):
                slept["n"] += 1

        with _fake_pyspark_and_sdk(collect_result=[], do_handler=do_handler) as st:
            ns = _exec_hook(content)
            # Neutralize backoff: the hook's _do_with_retry calls `time.sleep(...)`;
            # `time` resolves in the exec namespace, so override it there.
            assert "time" in ns  # the rendered hook does `import time`
            ns["time"] = _SleepStub
            hook = st["hooks"][0]

            # RUNNING: the POST 429s once, is retried, and succeeds -> no raise.
            hook(_evt("RUNNING"))  # must NOT raise
            assert "prod.sales.orders" in ns["_processed"]
            # Two recorded calls: the failed attempt + the retried success (the harness
            # records the call BEFORE invoking do_handler, so a retried op = 2 calls).
            assert len(st["calls"]) == 2
            # The backoff sleep was taken exactly once (proving the stub was wired in).
            assert slept["n"] == 1

    def test_nontransient_error_is_not_retried_and_warns_once(self, tmp_path):
        # Companion to the retry test: a NON-transient error (500, no transient marker,
        # no retry_after_secs) is re-raised immediately by _do_with_retry -> NOT retried
        # (exactly one recorded call) and still surfaces as a single WARNING.
        action = _write_action(write_target=_st_target(tags={"team": "x"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        def do_handler(method, path, body):
            raise RuntimeError("500 Internal Server Error")

        slept = {"n": 0}

        class _SleepStub:
            @staticmethod
            def sleep(_secs):
                slept["n"] += 1

        with _fake_pyspark_and_sdk(collect_result=[], do_handler=do_handler) as st:
            ns = _exec_hook(content)
            ns["time"] = _SleepStub
            hook = st["hooks"][0]

            with pytest.raises(RuntimeError) as exc:
                hook(_evt("RUNNING"))
            assert "[LHP UC Tagging] WARNING:" in str(exc.value)
            assert "tag operation(s) failed" in str(exc.value)
            assert "500 Internal Server Error" in str(exc.value)
            # Not retried: exactly one recorded call, and no backoff sleep.
            assert len(st["calls"]) == 1
            assert slept["n"] == 0
            # Marked processed despite failing, so the terminal pass does not re-attempt
            # it and emit a duplicate warning (matches the existing real-error contract).
            assert "prod.sales.orders" in ns["_processed"]
            before = len(st["calls"])
            hook(_evt("COMPLETED"))  # must not raise
            assert len(st["calls"]) == before

    # ── S-5: populated existing-tags snapshot drives the destructive paths ──────
    # Every test above runs with an EMPTY snapshot, so only the create (POST) branch
    # ever fires. These four populate `_existing_tags` (via `collect_result`) so the
    # PATCH (value-change) and DELETE (undeclared) branches of _reconcile_tags run.
    # The snapshot row shape the parser expects (hook._fetch_existing_tags): each row
    # supports row["entity_type"] (singular "table"/"column"), row["entity_name"]
    # (lowercased FQN), row["tags"] (a {tag_name: tag_value} map). A plain dict row
    # satisfies that subscript access, so collect_result is a list of such dicts.

    @staticmethod
    def _row(entity_type, entity_name, tags):
        return {"entity_type": entity_type, "entity_name": entity_name, "tags": tags}

    def test_value_change_emits_single_patch(self, tmp_path):
        from urllib.parse import quote

        fqn = "prod.sales.orders"
        # Desired value differs from the live value → PATCH, not POST/DELETE.
        action = _write_action(write_target=_st_target(tags={"team": "new"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        snapshot = [self._row("table", fqn, {"team": "old"})]
        with _fake_pyspark_and_sdk(collect_result=snapshot) as st:
            ns = _exec_hook(content)
            assert ns["_snapshot_error"] is None
            st["hooks"][0](_evt("RUNNING"))

            patches = [c for c in st["calls"] if c["method"] == "PATCH"]
            assert len(patches) == 1
            assert sum(c["method"] == "POST" for c in st["calls"]) == 0
            assert sum(c["method"] == "DELETE" for c in st["calls"]) == 0
            expected = (
                f"{ns['_TAG_BASE']}/tables/{quote(fqn, safe='')}"
                f"/tags/{quote('team', safe='')}"
            )
            assert patches[0]["path"] == expected
            # query is now recorded by _Api.do (T7 harness extension).
            assert patches[0]["query"] == {"update_mask": "tag_value"}
            assert "prod.sales.orders" in ns["_processed"]

    def test_remove_undeclared_emits_single_delete(self, tmp_path):
        from urllib.parse import quote

        fqn = "prod.sales.orders"
        # Desired declares only k1 (already live, same value → no PATCH). The live
        # k_foreign is undeclared, so with remove_undeclared_tags it is DELETEd.
        action = _write_action(write_target=_st_target(tags={"k1": "v1"}))
        content = _build(
            [action],
            uc_tagging=UCTaggingConfig(remove_undeclared_tags=True),
            root=tmp_path,
        )[HOOK_FILENAME]

        snapshot = [self._row("table", fqn, {"k1": "v1", "k_foreign": "vf"})]
        with _fake_pyspark_and_sdk(collect_result=snapshot) as st:
            ns = _exec_hook(content)
            assert ns["_snapshot_error"] is None
            st["hooks"][0](_evt("RUNNING"))

            deletes = [c for c in st["calls"] if c["method"] == "DELETE"]
            assert len(deletes) == 1
            expected = (
                f"{ns['_TAG_BASE']}/tables/{quote(fqn, safe='')}"
                f"/tags/{quote('k_foreign', safe='')}"
            )
            assert deletes[0]["path"] == expected
            # k1 (declared, unchanged) is neither PATCHed nor DELETEd.
            assert sum(c["method"] == "POST" for c in st["calls"]) == 0
            assert sum(c["method"] == "PATCH" for c in st["calls"]) == 0

    def test_default_keeps_undeclared_tags_no_delete(self, tmp_path):
        # Safety invariant: remove_undeclared_tags defaults to False, so a foreign live
        # tag is NEVER removed. Same snapshot as the DELETE test, default config.
        fqn = "prod.sales.orders"
        action = _write_action(write_target=_st_target(tags={"k1": "v1"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]
        assert "_REMOVE_UNDECLARED_TAGS = False" in content

        snapshot = [self._row("table", fqn, {"k1": "v1", "k_foreign": "vf"})]
        with _fake_pyspark_and_sdk(collect_result=snapshot) as st:
            ns = _exec_hook(content)
            st["hooks"][0](_evt("RUNNING"))
            assert sum(c["method"] == "DELETE" for c in st["calls"]) == 0

    def test_awkward_key_is_percent_encoded_on_patch(self, tmp_path):
        # S-1 regression lock. Key "a b" is E-2-legal (only an internal space) yet
        # REQUIRES encoding. Value change → PATCH; the path segment must be 'a%20b',
        # never a raw space. Without T6a's quote() this test is RED.
        from urllib.parse import quote

        fqn = "prod.sales.orders"
        action = _write_action(write_target=_st_target(tags={"a b": "new"}))
        content = _build([action], root=tmp_path)[HOOK_FILENAME]

        snapshot = [self._row("table", fqn, {"a b": "old"})]
        with _fake_pyspark_and_sdk(collect_result=snapshot) as st:
            ns = _exec_hook(content)
            st["hooks"][0](_evt("RUNNING"))

            patches = [c for c in st["calls"] if c["method"] == "PATCH"]
            assert len(patches) == 1
            expected = (
                f"{ns['_TAG_BASE']}/tables/{quote(fqn, safe='')}"
                f"/tags/{quote('a b', safe='')}"
            )
            assert patches[0]["path"] == expected
            assert "a%20b" in patches[0]["path"]
            assert "a b" not in patches[0]["path"]  # no raw space leaked through
