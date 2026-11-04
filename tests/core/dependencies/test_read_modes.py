"""``producer_kind``: the kind of view each action produces.

A reader must read a streaming view with ``spark.readStream`` and a batch view
with ``spark.read``. The kind of an action's view follows the read its
generator declares, except that a SQL view is streaming exactly when its SQL
reads a ``STREAM``, and a temp table, a write or a test produce no view.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from lhp.core.dependencies import producer_kind, sql_reads_stream
from lhp.core.dependencies._parse_cache import ParseCache
from lhp.core.loaders import load_sql_file
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


def _load(source: Any, read_mode: str | None = None) -> Action:
    return Action(
        name="load", type=ActionType.LOAD, source=source, target="v", readMode=read_mode
    )


def _transform(
    transform_type: TransformType, read_mode: str | None = None, **fields: Any
) -> Action:
    return Action(
        name="transform",
        type=ActionType.TRANSFORM,
        transform_type=transform_type,
        source="v_in",
        target="v_out",
        readMode=read_mode,
        **fields,
    )


def _kind(action: Action, **kwargs: Any) -> str:
    kwargs.setdefault("registry", ActionRegistry())
    return producer_kind(action, [action], **kwargs)


class TestProducerTable:
    @pytest.mark.parametrize(
        ("action", "expected"),
        [
            (_load({"type": "cloudfiles", "path": "/p", "format": "json"}), "stream"),
            (_load({"type": "kafka", "bootstrap_servers": "b"}), "stream"),
            (_load({"type": "delta", "table": "t"}), "batch"),
            (_load({"type": "delta", "table": "t"}, "stream"), "stream"),
            (_load({"type": "delta", "table": "t", "readMode": "stream"}), "stream"),
            (
                _load({"type": "delta", "table": "t", "readMode": "stream"}, "batch"),
                "batch",
            ),
            (_load({"type": "custom_datasource"}), "stream"),
            (_load({"type": "custom_datasource"}, "batch"), "batch"),
            (_load({"type": "jdbc", "url": "u"}), "batch"),
            (_load({"type": "jdbc", "url": "u"}, "stream"), "batch"),
            (_load({"type": "python", "module_path": "m.py"}), "unknown"),
            (_load({"type": "python", "module_path": "m.py"}, "batch"), "unknown"),
            (_transform(TransformType.PYTHON), "batch"),
            (_transform(TransformType.PYTHON, "stream"), "stream"),
            (_transform(TransformType.SCHEMA), "stream"),
            (_transform(TransformType.SCHEMA, "batch"), "batch"),
            (_transform(TransformType.DATA_QUALITY), "stream"),
            (_transform(TransformType.DATA_QUALITY, "batch"), "batch"),
            (_transform(TransformType.TEMP_TABLE), "unknown"),
            (_transform(TransformType.TEMP_TABLE, "stream"), "unknown"),
        ],
        ids=[
            "cloudfiles",
            "kafka",
            "delta-default-batch",
            "delta-readMode",
            "delta-source-readMode",
            "delta-readMode-wins-over-source",
            "custom_datasource-default-stream",
            "custom_datasource-readMode",
            "jdbc",
            "jdbc-ignores-readMode",
            "python-load",
            "python-load-ignores-readMode",
            "python-transform-default-batch",
            "python-transform-readMode",
            "schema-default-stream",
            "schema-readMode",
            "data_quality-default-stream",
            "data_quality-readMode",
            "temp_table-not-a-view",
            "temp_table-not-a-view-any-readMode",
        ],
    )
    def test_kind(self, action: Action, expected: str) -> None:
        assert _kind(action) == expected

    def test_cloudfiles_view_is_stream_whatever_readMode_asks(self) -> None:
        # The generated code always reads with spark.readStream; the
        # validator rejects the batch request itself (LHP-VAL-068).
        action = _load({"type": "cloudfiles", "path": "/p", "format": "json"}, "batch")
        assert _kind(action) == "stream"

    @pytest.mark.parametrize(
        "write_target",
        [
            {"type": "streaming_table", "table": "t"},
            {"type": "materialized_view", "table": "t"},
            {"type": "sink", "sink_type": "delta"},
        ],
    )
    def test_writes_produce_no_view(self, write_target: dict) -> None:
        action = Action(
            name="w", type=ActionType.WRITE, source="v", write_target=write_target
        )
        assert _kind(action) == "unknown"

    def test_tests_produce_no_view(self) -> None:
        action = Action(
            name="t", type=ActionType.TEST, test_type="row_count", source=["a", "b"]
        )
        assert _kind(action) == "unknown"

    def test_unregistered_load_type_is_unknown(self) -> None:
        assert _kind(_load({"type": "not_a_source"})) == "unknown"

    def test_uses_the_given_registry(self) -> None:
        action = _load({"type": "delta", "table": "t"}, "stream")
        assert _kind(action, registry=ActionRegistry()) == "stream"


class TestSqlKind:
    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            ("SELECT * FROM STREAM(bronze.orders)", "stream"),
            ("SELECT * FROM stream(v_orders)", "stream"),
            ("SELECT * FROM STREAM v_orders", "stream"),
            ("SELECT * FROM STREAM read_files('/data', format => 'json')", "stream"),
            ("SELECT * FROM bronze.orders", "batch"),
            ("SELECT stream_id FROM bronze.orders", "batch"),
            ("SELECT * FROM bronze.orders -- not a stream", "batch"),
            ("SELECT * FROM ${catalog}.bronze.orders", "batch"),
            ("SELECT * FROM STREAM(${catalog}.bronze.orders)", "stream"),
        ],
    )
    def test_inline_load_sql(self, sql: str, expected: str) -> None:
        assert _kind(_load(sql)) == expected
        assert _kind(_load({"type": "sql", "sql": sql})) == expected

    def test_transform_sql(self) -> None:
        streaming = _transform(TransformType.SQL, sql="SELECT * FROM STREAM(v_in)")
        batch = _transform(TransformType.SQL, sql="SELECT * FROM v_in")
        assert _kind(streaming) == "stream"
        assert _kind(batch) == "batch"

    def test_sql_kind_ignores_readMode(self) -> None:
        action = _transform(
            TransformType.SQL, "stream", sql="SELECT * FROM v_in WHERE id > 0"
        )
        assert _kind(action) == "batch"

    @pytest.mark.parametrize(
        "action",
        [
            _transform(TransformType.SQL, sql="SELECT * FROM STREAM($source)"),
            _transform(TransformType.SQL, sql="SELECT * FROM $source"),
            _load({"type": "sql", "sql": "SELECT * FROM $source"}),
        ],
    )
    def test_source_placeholder_is_unknown(self, action: Action) -> None:
        assert _kind(action) == "unknown"

    def test_unparseable_stream_sql_is_unknown(self) -> None:
        assert _kind(_load("SELECT * FROM STREAM(")) == "unknown"

    def test_no_stream_word_is_batch_without_parsing(self) -> None:
        with patch("lhp.core.dependencies.sql_extraction.sqlglot.parse") as parse:
            assert _kind(_load("SELECT FROM WHERE ((")) == "batch"
        parse.assert_not_called()

    def test_sql_path_file(self, tmp_path: Path) -> None:
        (tmp_path / "sql").mkdir()
        (tmp_path / "sql" / "s.sql").write_text("SELECT * FROM STREAM(v_in)\n")
        (tmp_path / "sql" / "b.sql").write_text("SELECT * FROM v_in\n")
        load = _load({"type": "sql", "sql_path": "sql/s.sql"})
        transform = _transform(TransformType.SQL, sql_path="sql/b.sql")

        assert _kind(load, project_root=tmp_path) == "stream"
        assert _kind(transform, project_root=tmp_path) == "batch"

    def test_inline_sql_wins_over_sql_path(self, tmp_path: Path) -> None:
        (tmp_path / "b.sql").write_text("SELECT * FROM v_in\n")
        load = _load(
            {"type": "sql", "sql": "SELECT * FROM STREAM v_in", "sql_path": "b.sql"}
        )
        transform = _transform(
            TransformType.SQL, sql="SELECT * FROM STREAM v_in", sql_path="b.sql"
        )
        assert _kind(load, project_root=tmp_path) == "stream"
        assert _kind(transform, project_root=tmp_path) == "stream"

    @pytest.mark.parametrize(
        ("action", "project_root"),
        [
            (_load({"type": "sql", "sql_path": "missing.sql"}), "tmp"),
            (_transform(TransformType.SQL, sql_path="missing.sql"), "tmp"),
            (_load({"type": "sql", "sql_path": "present.sql"}), None),
            (_load({"type": "sql", "sql_path": "a_directory"}), "tmp"),
            (_load({"type": "sql"}), "tmp"),
            (_transform(TransformType.SQL), "tmp"),
        ],
        ids=[
            "missing-file",
            "missing-file-transform",
            "no-project-root",
            "path-is-directory",
            "no-sql",
            "no-sql-transform",
        ],
    )
    def test_unreadable_sql_is_unknown(
        self, tmp_path: Path, action: Action, project_root: str | None
    ) -> None:
        (tmp_path / "present.sql").write_text("SELECT * FROM v_in\n")
        (tmp_path / "a_directory").mkdir()
        root = tmp_path if project_root == "tmp" else None
        assert _kind(action, project_root=root) == "unknown"


def _substitutions(tmp_path: Path, body: str) -> EnhancedSubstitutionManager:
    path = tmp_path / "dev.yaml"
    path.write_text(body)
    return EnhancedSubstitutionManager(path, env="dev")


class TestSqlSubstitution:
    def test_inline_sql_is_substituted(self, tmp_path: Path) -> None:
        manager = _substitutions(tmp_path, "dev:\n  orders_read: STREAM(v_orders)\n")
        action = _load({"type": "sql", "sql": "SELECT * FROM ${orders_read}"})

        assert _kind(action) == "batch"
        assert _kind(action, substitution_mgr=manager) == "stream"

    def test_sql_path_is_substituted(self, tmp_path: Path) -> None:
        manager = _substitutions(tmp_path, "dev:\n  orders_read: STREAM(v_orders)\n")
        (tmp_path / "q.sql").write_text("SELECT * FROM ${orders_read}\n")
        action = _transform(TransformType.SQL, sql_path="q.sql")

        assert _kind(action, project_root=tmp_path) == "batch"
        assert (
            _kind(action, project_root=tmp_path, substitution_mgr=manager) == "stream"
        )

    def test_unresolved_token_in_file_is_unknown(self, tmp_path: Path) -> None:
        manager = _substitutions(tmp_path, "dev:\n  catalog: main\n")
        (tmp_path / "q.sql").write_text("SELECT * FROM STREAM(${not_defined}.t)\n")
        action = _load({"type": "sql", "sql_path": "q.sql"})

        kind = _kind(action, project_root=tmp_path, substitution_mgr=manager)
        assert kind == "unknown"

    def test_secret_references_are_not_recorded(self, tmp_path: Path) -> None:
        manager = _substitutions(
            tmp_path, "dev:\n  catalog: main\nsecrets:\n  default_scope: s\n"
        )
        sql = "SELECT '${secret:s/k}' AS k FROM STREAM(v_in)"
        (tmp_path / "q.sql").write_text(sql)
        inline = _load({"type": "sql", "sql": sql})
        from_file = _transform(TransformType.SQL, sql_path="q.sql")

        assert _kind(inline, substitution_mgr=manager) == "stream"
        assert (
            _kind(from_file, project_root=tmp_path, substitution_mgr=manager)
            == "stream"
        )
        assert manager.secret_references == set()
        # Rendering the same file records them, as code generation does.
        load_sql_file("q.sql", tmp_path, manager)
        assert {(ref.scope, ref.key) for ref in manager.secret_references} == {
            ("s", "k")
        }


class TestParseCacheReuse:
    def test_probe_runs_once_per_sql_body(self) -> None:
        cache = ParseCache()
        first = _load("SELECT * FROM STREAM(v_a)")
        second = _transform(TransformType.SQL, sql="SELECT * FROM STREAM(v_a)")

        with patch(
            "lhp.core.dependencies._parse_cache.sql_reads_stream",
            wraps=sql_reads_stream,
        ) as probe:
            assert _kind(first, parse_cache=cache) == "stream"
            assert _kind(second, parse_cache=cache) == "stream"

        assert probe.call_count == 1


class TestRegistryIsRequired:
    def test_a_read_needs_the_registry_it_is_dispatched_by(self) -> None:
        # A spawned worker's default registry holds no generators, so a
        # fallback would silently report every read as unknown.
        action = _load({"type": "delta", "table": "t"})

        with pytest.raises(TypeError, match="registry"):
            producer_kind(action, [action])  # type: ignore[call-arg]  # intentional: registry is required
