"""Unit tests for the assistant session/config CRUD layer and its v2 migration."""

from __future__ import annotations

import json
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lhp.webapp.services import assistant_store, run_history, sqlite_store
from lhp.webapp.services.sqlite_migrations import MIGRATIONS

pytestmark = pytest.mark.webapp


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A migrated throwaway project root."""
    sqlite_store.run_migrations(tmp_path)
    return tmp_path


def _user_version(project_root: Path) -> int:
    with closing(sqlite_store.connect(project_root)) as conn:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _schema_names(project_root: Path, kind: str) -> set[str]:
    with closing(sqlite_store.connect(project_root)) as conn:
        return {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = ?", (kind,)
            )
        }


def _insert(project_root: Path, session_id: str, title: str = "chat") -> None:
    """Insert a session with deterministic non-id fields."""
    assistant_store.insert_session(
        project_root,
        session_id,
        agent_id=f"ag_{session_id}",
        host_id="host-1",
        agent_bundle_hash="hash-1",
        title=title,
    )


def _set_last_used_at(project_root: Path, session_id: str, last_used_at: str) -> None:
    """Backdate a session for deterministic ordering / touch assertions."""
    with closing(sqlite_store.connect(project_root)) as conn, conn:
        conn.execute(
            "UPDATE assistant_sessions SET last_used_at = ? WHERE session_id = ?",
            (last_used_at, session_id),
        )


def test_fresh_db_migrates_to_latest_with_assistant_schema(tmp_path: Path) -> None:
    sqlite_store.run_migrations(tmp_path)

    assert _user_version(tmp_path) == len(MIGRATIONS)
    assert {"assistant_sessions", "assistant_config", "assistant_items"} <= (
        _schema_names(tmp_path, "table")
    )
    assert "idx_assistant_sessions_status" in _schema_names(tmp_path, "index")


def test_v1_db_upgrades_in_place_and_preserves_runs(tmp_path: Path) -> None:
    # Build a database exactly as v1 shipped it: batch 1 only, user_version 1.
    with closing(sqlite_store.connect(tmp_path)) as conn, conn:
        for statement in MIGRATIONS[0]:
            conn.execute(statement)
        conn.execute("PRAGMA user_version = 1")
    run_history.create_run(tmp_path, "r1", "validate", "dev", pipeline="bronze")

    sqlite_store.run_migrations(tmp_path)

    assert _user_version(tmp_path) == len(MIGRATIONS)
    assert {"assistant_sessions", "assistant_config"} <= _schema_names(
        tmp_path, "table"
    )
    run = run_history.get_run(tmp_path, "r1")
    assert run is not None
    assert run["pipeline"] == "bronze"
    assert run["status"] == "running"


def test_get_config_unset_key_returns_none(project: Path) -> None:
    assert assistant_store.get_config(project, "executor") is None


def test_config_put_get_roundtrip(project: Path) -> None:
    value = {"mode": "api_key", "api_key_env": "LHP_ASSISTANT_API_KEY"}
    assistant_store.put_config(project, "executor", value)

    assert assistant_store.get_config(project, "executor") == value
    assert assistant_store.get_config(project, "agent") is None


def test_config_overwrite_updates_value_and_updated_at(project: Path) -> None:
    assistant_store.put_config(project, "agent", {"model": "a"})
    backdated = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with closing(sqlite_store.connect(project)) as conn, conn:
        conn.execute(
            "UPDATE assistant_config SET updated_at = ? WHERE key = 'agent'",
            (backdated,),
        )

    assistant_store.put_config(project, "agent", {"model": "b"})

    assert assistant_store.get_config(project, "agent") == {"model": "b"}
    with closing(sqlite_store.connect(project)) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n, MAX(updated_at) AS updated_at "
            "FROM assistant_config WHERE key = 'agent'"
        ).fetchone()
    assert row["n"] == 1
    # ISO-8601 UTC strings compare chronologically as strings.
    assert row["updated_at"] > backdated


def test_get_active_session_none_on_fresh_db(project: Path) -> None:
    assert assistant_store.get_active_session(project) is None


def test_insert_session_stores_all_fields(project: Path) -> None:
    _insert(project, "conv_1", title="first chat")

    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["session_id"] == "conv_1"
    assert active["agent_id"] == "ag_conv_1"
    assert active["host_id"] == "host-1"
    assert active["agent_bundle_hash"] == "hash-1"
    assert active["title"] == "first chat"
    assert active["status"] == "active"
    assert active["created_at"] is not None
    assert active["last_used_at"] is not None


def test_insert_session_archives_previous_active(project: Path) -> None:
    _insert(project, "conv_1")
    _insert(project, "conv_2")

    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["session_id"] == "conv_2"

    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {"conv_1": "archived", "conv_2": "active"}
    assert list(by_id.values()).count("active") == 1


def test_archive_active_demotes_and_reports_count(project: Path) -> None:
    _insert(project, "conv_1")

    assert assistant_store.archive_active(project) == 1
    assert assistant_store.get_active_session(project) is None
    assert assistant_store.archive_active(project) == 0

    sessions = assistant_store.list_sessions(project)
    assert [s["status"] for s in sessions] == ["archived"]


def test_mark_stale(project: Path) -> None:
    _insert(project, "conv_1")
    assistant_store.mark_stale(project, "conv_1")

    assert assistant_store.get_active_session(project) is None
    sessions = assistant_store.list_sessions(project)
    assert [(s["session_id"], s["status"]) for s in sessions] == [("conv_1", "stale")]


def test_touch_session_bumps_last_used_at(project: Path) -> None:
    _insert(project, "conv_1")
    backdated = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    _set_last_used_at(project, "conv_1", backdated)

    assistant_store.touch_session(project, "conv_1")

    (session,) = assistant_store.list_sessions(project)
    assert session["last_used_at"] > backdated


def test_list_sessions_most_recent_first_and_limit(project: Path) -> None:
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    for i in range(3):
        session_id = f"conv_{i}"
        _insert(project, session_id)
        _set_last_used_at(
            project, session_id, (base + timedelta(minutes=i)).isoformat()
        )

    sessions = assistant_store.list_sessions(project)
    assert [s["session_id"] for s in sessions] == ["conv_2", "conv_1", "conv_0"]

    limited = assistant_store.list_sessions(project, limit=2)
    assert [s["session_id"] for s in limited] == ["conv_2", "conv_1"]


# ---------------------------------------------------------------------------
# v3 migration (providers + transcript items)
# ---------------------------------------------------------------------------


def test_v2_db_upgrades_to_v3_and_backfills_provider(tmp_path: Path) -> None:
    # Build a database exactly as v2 shipped it: batches 1+2, user_version 2,
    # with one pre-v3 omnigent session row already present.
    with closing(sqlite_store.connect(tmp_path)) as conn, conn:
        for batch in MIGRATIONS[:2]:
            for statement in batch:
                conn.execute(statement)
        conn.execute("PRAGMA user_version = 2")
        conn.execute(
            "INSERT INTO assistant_sessions "
            "(session_id, agent_id, host_id, agent_bundle_hash, status, "
            "created_at, last_used_at) "
            "VALUES ('conv_old', 'ag_old', 'host-1', 'hash-1', 'active', "
            "'2026-07-01T00:00:00+00:00', '2026-07-01T00:00:00+00:00')"
        )

    sqlite_store.run_migrations(tmp_path)

    assert _user_version(tmp_path) == len(MIGRATIONS)
    assert "assistant_items" in _schema_names(tmp_path, "table")
    active = assistant_store.get_active_session(tmp_path)
    assert active is not None
    assert active["session_id"] == "conv_old"
    assert active["provider"] == "omnigent"
    assert active["runtime_session_id"] is None


def test_omnigent_insert_session_defaults_provider(project: Path) -> None:
    _insert(project, "conv_1")

    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["provider"] == "omnigent"


def test_insert_claude_session_writes_sentinels(project: Path) -> None:
    assistant_store.insert_claude_session(
        project, "claude_abc", agent_bundle_hash="hash-c", title="claude chat"
    )

    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["session_id"] == "claude_abc"
    assert active["provider"] == "claude_sdk"
    assert active["agent_id"] == ""
    assert active["host_id"] == "local"
    assert active["agent_bundle_hash"] == "hash-c"
    assert active["title"] == "claude chat"
    assert active["runtime_session_id"] is None


def test_claude_sessions_coexist_active_multi_tab(project: Path) -> None:
    # Multi-tab: claude inserts demote NOTHING — every open tab stays active.
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")
    assistant_store.insert_claude_session(project, "claude_2", "hash-c")
    assistant_store.insert_claude_session(project, "claude_3", "hash-c")

    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {
        "claude_1": "active",
        "claude_2": "active",
        "claude_3": "active",
    }


def test_omnigent_insert_session_still_demotes_all_actives(project: Path) -> None:
    # The omnigent provider keeps its single-active invariant: inserting an
    # omnigent session archives EVERY active row, claude tabs included.
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")
    assistant_store.insert_claude_session(project, "claude_2", "hash-c")

    _insert(project, "conv_1")

    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {
        "claude_1": "archived",
        "claude_2": "archived",
        "conv_1": "active",
    }


def test_get_active_session_returns_mru_of_multiple_actives(project: Path) -> None:
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    for i in range(3):
        assistant_store.insert_claude_session(project, f"claude_{i}", "hash-c")
        _set_last_used_at(
            project, f"claude_{i}", (base + timedelta(minutes=i)).isoformat()
        )
    _set_last_used_at(project, "claude_1", (base + timedelta(hours=1)).isoformat())

    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["session_id"] == "claude_1"

    # Archived/stale rows never win, however recently used.
    assistant_store.archive_session(project, "claude_1")
    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["session_id"] == "claude_2"


def test_get_session_any_status_and_unknown_none(project: Path) -> None:
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")
    assistant_store.archive_session(project, "claude_1")

    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["status"] == "archived"
    assert assistant_store.get_session(project, "claude_ghost") is None


def test_archive_reopen_roundtrip(project: Path) -> None:
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")
    backdated = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    _set_last_used_at(project, "claude_1", backdated)

    assert assistant_store.archive_session(project, "claude_1") == 1
    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["status"] == "archived"
    # Archiving a non-active session is a no-op.
    assert assistant_store.archive_session(project, "claude_1") == 0
    assert assistant_store.archive_session(project, "claude_ghost") == 0

    assert assistant_store.reopen_session(project, "claude_1") == 1
    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["status"] == "active"
    # Reopening bumps last_used_at so the tab becomes the MRU active row.
    assert row["last_used_at"] > backdated
    assert assistant_store.reopen_session(project, "claude_1") == 0


def test_reopen_never_resurrects_a_stale_session(project: Path) -> None:
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")
    assistant_store.mark_stale(project, "claude_1")

    assert assistant_store.reopen_session(project, "claude_1") == 0
    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["status"] == "stale"


def test_set_title_if_default_sets_exactly_once(project: Path) -> None:
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")

    assert assistant_store.set_title_if_default(project, "claude_1", "first msg") == 1
    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["title"] == "first msg"

    # Second call is a no-op: the placeholder was already claimed.
    assert assistant_store.set_title_if_default(project, "claude_1", "second") == 0
    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["title"] == "first msg"


def test_set_title_if_default_respects_explicit_titles(project: Path) -> None:
    assistant_store.insert_claude_session(
        project, "claude_1", "hash-c", title="chosen name"
    )

    assert assistant_store.set_title_if_default(project, "claude_1", "usurper") == 0
    row = assistant_store.get_session(project, "claude_1")
    assert row is not None
    assert row["title"] == "chosen name"


def test_set_runtime_session_id_roundtrip_and_clear(project: Path) -> None:
    assistant_store.insert_claude_session(project, "claude_1", "hash-c")

    assistant_store.set_runtime_session_id(project, "claude_1", "sdk-uuid-1")
    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["runtime_session_id"] == "sdk-uuid-1"

    # Cleared (e.g. after a failed resume) so the next turn starts fresh.
    assistant_store.set_runtime_session_id(project, "claude_1", None)
    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["runtime_session_id"] is None


def test_insert_item_assigns_monotonic_seq_per_session(project: Path) -> None:
    assert assistant_store.insert_item(project, "claude_1", {"id": "a"}) == 1
    assert assistant_store.insert_item(project, "claude_1", {"id": "b"}) == 2
    # Sequences are per-session, not global.
    assert assistant_store.insert_item(project, "claude_2", {"id": "x"}) == 1
    assert assistant_store.insert_item(project, "claude_1", {"id": "c"}) == 3


def test_list_items_returns_envelopes_in_insertion_order(project: Path) -> None:
    envelopes = [
        {"id": "m1", "type": "message", "status": "completed", "data": {"n": i}}
        for i in range(3)
    ]
    for env in envelopes:
        assistant_store.insert_item(project, "claude_1", env)

    assert assistant_store.list_items(project, "claude_1") == envelopes
    assert assistant_store.list_items(project, "claude_other") == []


# ---------------------------------------------------------------------------
# v4 migration (per-turn usage rows)
# ---------------------------------------------------------------------------


def _usage(**overrides: int) -> dict[str, int]:
    base = {
        "input_tokens": 100,
        "output_tokens": 50,
        "cache_read_input_tokens": 1000,
        "cache_creation_input_tokens": 200,
    }
    base.update(overrides)
    return base


def test_fresh_db_reaches_user_version_4_with_usage_table(tmp_path: Path) -> None:
    sqlite_store.run_migrations(tmp_path)

    assert _user_version(tmp_path) == 4
    assert "assistant_turn_usage" in _schema_names(tmp_path, "table")


def test_rerunning_migrations_is_idempotent(project: Path) -> None:
    before = _user_version(project)
    sqlite_store.run_migrations(project)
    sqlite_store.run_migrations(project)

    assert _user_version(project) == before == len(MIGRATIONS)
    assert "assistant_turn_usage" in _schema_names(project, "table")


def test_v3_db_upgrades_to_v4(tmp_path: Path) -> None:
    # Build a database exactly as v3 shipped it: batches 1-3, user_version 3.
    with closing(sqlite_store.connect(tmp_path)) as conn, conn:
        for batch in MIGRATIONS[:3]:
            for statement in batch:
                conn.execute(statement)
        conn.execute("PRAGMA user_version = 3")

    sqlite_store.run_migrations(tmp_path)

    assert _user_version(tmp_path) == len(MIGRATIONS)
    assert "assistant_turn_usage" in _schema_names(tmp_path, "table")


def test_insert_turn_usage_allocates_sequential_seq_per_session(
    project: Path,
) -> None:
    assert (
        assistant_store.insert_turn_usage(project, "s1", _usage(), 0.1, None, None) == 1
    )
    assert (
        assistant_store.insert_turn_usage(project, "s1", _usage(), 0.2, None, None) == 2
    )
    # Sequences are per-session, not global.
    assert (
        assistant_store.insert_turn_usage(project, "s2", _usage(), 0.3, None, None) == 1
    )
    assert (
        assistant_store.insert_turn_usage(project, "s1", _usage(), 0.4, None, None) == 3
    )


def test_insert_turn_usage_defaults_missing_counters_and_stores_model_json(
    project: Path,
) -> None:
    model_usage = {"claude-sonnet-5": _usage()}
    assistant_store.insert_turn_usage(
        project, "s1", {"input_tokens": 5}, None, 1.25, model_usage
    )

    with closing(sqlite_store.connect(project)) as conn:
        row = conn.execute(
            "SELECT * FROM assistant_turn_usage WHERE session_id = 's1'"
        ).fetchone()
    assert row["input_tokens"] == 5
    assert row["output_tokens"] == 0
    assert row["cache_read_input_tokens"] == 0
    assert row["cache_creation_input_tokens"] == 0
    assert row["sdk_cost_usd"] is None
    assert row["configured_cost_usd"] == 1.25
    assert json.loads(row["model_usage_json"]) == model_usage
    assert row["created_at"] is not None


def test_usage_totals_sums_rows_and_none_when_empty(project: Path) -> None:
    assert assistant_store.usage_totals(project, "s1") is None

    assistant_store.insert_turn_usage(project, "s1", _usage(), 0.10, 0.20, None)
    assistant_store.insert_turn_usage(
        project, "s1", _usage(input_tokens=900), 0.05, None, None
    )
    assistant_store.insert_turn_usage(project, "other", _usage(), 9.99, 9.99, None)

    totals = assistant_store.usage_totals(project, "s1")
    assert totals == {
        "input_tokens": 1000,
        "output_tokens": 100,
        "cache_read_input_tokens": 2000,
        "cache_creation_input_tokens": 400,
        "sdk_cost_usd": pytest.approx(0.15),
        # SUM skips the NULL of the unpriced second turn.
        "configured_cost_usd": pytest.approx(0.20),
    }


def test_usage_totals_all_null_costs_stay_none(project: Path) -> None:
    assistant_store.insert_turn_usage(project, "s1", _usage(), None, None, None)

    totals = assistant_store.usage_totals(project, "s1")
    assert totals is not None
    assert totals["sdk_cost_usd"] is None
    assert totals["configured_cost_usd"] is None


def test_usage_totals_by_session_groups_in_one_query(project: Path) -> None:
    assistant_store.insert_turn_usage(project, "s1", _usage(), 0.1, None, None)
    assistant_store.insert_turn_usage(project, "s1", _usage(), 0.1, None, None)
    assistant_store.insert_turn_usage(
        project, "s2", _usage(input_tokens=1), 0.2, 0.3, None
    )

    by_id = assistant_store.usage_totals_by_session(project, ["s1", "s2", "s3"])

    assert set(by_id) == {"s1", "s2"}  # s3 has no rows -> absent
    assert by_id["s1"]["input_tokens"] == 200
    assert by_id["s1"]["sdk_cost_usd"] == pytest.approx(0.2)
    assert by_id["s2"]["input_tokens"] == 1
    assert by_id["s2"]["configured_cost_usd"] == pytest.approx(0.3)
    assert assistant_store.usage_totals_by_session(project, []) == {}
