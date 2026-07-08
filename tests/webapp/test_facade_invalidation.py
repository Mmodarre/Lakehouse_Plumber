"""Facade-invalidation tests: file mutations are visible without a restart.

The application facade is built once and cached on ``app.state.facade``; it
memoizes flowgroup discovery, so without invalidation an edit made through the
file-write API would stay invisible to browse / validate / generate until the
process restarts. The files router now calls
:func:`lhp.webapp.dependencies.invalidate_facade` after any successful mutation
outside the generated/internal trees, dropping the cache so the next request
rebuilds a fresh facade that re-discovers from disk.

These tests pin the two staleness blockers end-to-end:

* BLOCKER-1 — an edit to an existing flowgroup (and a deletion) is reflected in
  ``/api/flowgroups`` without a restart.
* BLOCKER-2 — a brand-new flowgroup file dropped into an existing pipeline dir
  becomes discoverable and is covered by a subsequent validate run.

All mutations use ``mutable_client`` (per-test deep copy). Subjects are real
flowgroups in ``tests/e2e/fixtures/testing_project``: ``region_bronze`` (a
self-contained flowgroup in pipeline ``acmi_edw_bronze``) is the edit/delete
subject; a new sibling is authored from the same template.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.dependencies import invalidate_facade

pytestmark = pytest.mark.webapp

# Existing self-contained flowgroup in pipeline acmi_edw_bronze.
_REGION_PATH = "pipelines/02_bronze/statics/region_bronze.yaml"
# A new sibling in the same pipeline directory / pipeline.
_NEW_PATH = "pipelines/02_bronze/statics/region_bronze_new_test.yaml"
_NEW_FLOWGROUP_NAME = "region_bronze_new_test"

# region_bronze re-authored with a renamed flowgroup — a valid in-place edit
# (writes the same ``region`` table it already owned, so no duplicate-target).
_RENAMED_REGION_YAML = """\
pipeline: acmi_edw_bronze
flowgroup: region_bronze_renamed
presets:
  - default_delta_properties
actions:
  - name: region_raw_incremental_load
    type: load
    readMode: stream
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: region_raw
    target: v_region_raw
  - name: region_bronze_incremental_cleanse
    type: transform
    transform_type: sql
    source: v_region_raw
    target: v_region_bronze_cleaned
    sql: |
      SELECT * FROM stream(v_region_raw)
  - name: write_region_bronze_incremental
    type: write
    source: v_region_bronze_cleaned
    write_target:
      create_table: true
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: "region"
"""

# A brand-new flowgroup: unique flowgroup / action / view / table names so it
# discovers and validates cleanly alongside the untouched project.
_NEW_FLOWGROUP_YAML = """\
pipeline: acmi_edw_bronze
flowgroup: region_bronze_new_test
presets:
  - default_delta_properties
actions:
  - name: region_new_test_raw_load
    type: load
    readMode: stream
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: region_raw
    target: v_region_new_test_raw
  - name: region_new_test_cleanse
    type: transform
    transform_type: sql
    source: v_region_new_test_raw
    target: v_region_new_test_cleaned
    sql: |
      SELECT * FROM stream(v_region_new_test_raw)
  - name: write_region_new_test
    type: write
    source: v_region_new_test_cleaned
    write_target:
      create_table: true
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: "region_new_test"
"""

# The same new flowgroup with a DELIBERATE structural error: the load action's
# ``type`` is not a registered action type. It parses as valid YAML (the PUT
# persists it with ``yaml_error: null``) but discovery rejects it with
# LHP-ACT-001 — an error only a facade rebuilt AFTER the PUT can report, which
# makes the staleness assertion in
# ``test_validate_stream_covers_new_flowgroup_after_put`` non-vacuous.
_BROKEN_NEW_FLOWGROUP_YAML = _NEW_FLOWGROUP_YAML.replace(
    "    type: load\n", "    type: not_a_real_action_type\n", 1
)

# Syntactically broken YAML (unterminated flow sequence) for the pinned
# "200 + yaml_error still invalidates" contract.
_BROKEN_SYNTAX_YAML = "pipeline: [unclosed\n"
_BROKEN_SYNTAX_PATH = "pipelines/02_bronze/statics/broken_syntax_test.yaml"


def _flowgroup_names(client: TestClient) -> set[str]:
    """Return the set of flowgroup names currently reported by the API."""
    resp = client.get("/api/flowgroups")
    assert resp.status_code == 200
    return {fg["name"] for fg in resp.json()["flowgroups"]}


def _run_validate_stream(client: TestClient, env: str) -> list[dict]:
    """Run the NDJSON validate stream and return the decoded frame list."""
    resp = client.post("/api/validate/stream", json={"env": env})
    assert resp.status_code == 200
    return [
        json.loads(line)
        for line in resp.content.decode("utf-8").splitlines()
        if line.strip()
    ]


class TestBrowseReflectsMutations:
    """/api/flowgroups reflects file edits/creates/deletes without a restart."""

    def test_edit_existing_flowgroup_is_reflected(
        self, mutable_client: TestClient
    ) -> None:
        before = _flowgroup_names(mutable_client)
        assert "region_bronze" in before

        resp = mutable_client.put(
            f"/api/files/{_REGION_PATH}", json={"content": _RENAMED_REGION_YAML}
        )
        assert resp.status_code == 200

        after = _flowgroup_names(mutable_client)
        assert "region_bronze_renamed" in after
        assert "region_bronze" not in after

    def test_new_flowgroup_file_becomes_visible(
        self, mutable_client: TestClient
    ) -> None:
        assert _NEW_FLOWGROUP_NAME not in _flowgroup_names(mutable_client)

        resp = mutable_client.put(
            f"/api/files/{_NEW_PATH}", json={"content": _NEW_FLOWGROUP_YAML}
        )
        assert resp.status_code == 200
        assert resp.json()["yaml_error"] is None

        assert _NEW_FLOWGROUP_NAME in _flowgroup_names(mutable_client)

    def test_deleting_flowgroup_removes_it(self, mutable_client: TestClient) -> None:
        assert "region_bronze" in _flowgroup_names(mutable_client)

        resp = mutable_client.delete(f"/api/files/{_REGION_PATH}")
        assert resp.status_code == 200

        assert "region_bronze" not in _flowgroup_names(mutable_client)


class TestValidateSeesMutations:
    """A validate run after a mutation operates on the fresh disk state."""

    def test_validate_stream_covers_new_flowgroup_after_put(
        self, mutable_client: TestClient
    ) -> None:
        """A new flowgroup written AFTER the facade was built is covered.

        Staleness-sensitive by construction: the facade cache is warmed FIRST
        (both the browse surface and a full validate run over the untouched
        project), then a new flowgroup carrying a deliberate LHP-ACT-001
        structural error is PUT. Only a facade rebuilt after the PUT can
        discover the new file and report its error; with invalidation no-op'd
        the second validate would reuse the stale discovery and terminate
        cleanly, failing this test.
        """
        # Warm the facade cache: browse + a clean validate over the untouched
        # project build and memoize discovery on app.state.facade.
        assert _NEW_FLOWGROUP_NAME not in _flowgroup_names(mutable_client)
        warm_frames = _run_validate_stream(mutable_client, env="dev")
        assert warm_frames[-1].get("type") == "ValidationCompleted"
        assert warm_frames[-1]["response"]["success"] is True

        put = mutable_client.put(
            f"/api/files/{_NEW_PATH}", json={"content": _BROKEN_NEW_FLOWGROUP_YAML}
        )
        assert put.status_code == 200
        # Parseable YAML — the deliberate error is semantic, not syntactic.
        assert put.json()["yaml_error"] is None

        frames = _run_validate_stream(mutable_client, env="dev")
        assert frames, "validate stream produced no frames"

        types = [frame.get("type") for frame in frames]
        # The deliberate error in the NEW file aborts discovery with the
        # pinned terminal error frame — proof the run saw the fresh disk
        # state. A stale facade would have terminated with a clean
        # ValidationCompleted instead.
        assert types[-1] == "error", types
        assert frames[-1]["code"] == "LHP-ACT-001"
        assert "ValidationCompleted" not in types


class TestYamlErrorStillInvalidates:
    """PINNED: invalidation fires even when the PUT response carries yaml_error.

    The bytes persisted (the user saved broken YAML mid-edit), so the cached
    facade is stale regardless of the syntax diagnostic and must be dropped.
    """

    def test_put_with_yaml_error_still_invalidates_facade(
        self, mutable_client: TestClient
    ) -> None:
        app = mutable_client.app
        # Warm the cache: a facade-building request caches an instance.
        assert mutable_client.get("/api/flowgroups").status_code == 200
        assert app.state.facade is not None

        resp = mutable_client.put(
            f"/api/files/{_BROKEN_SYNTAX_PATH}",
            json={"content": _BROKEN_SYNTAX_YAML},
        )
        assert resp.status_code == 200
        assert resp.json()["yaml_error"] is not None

        # The write persisted, so the cached facade was reset.
        assert app.state.facade is None


class TestInvalidateFacadeUnit:
    """Unit-level: invalidate drops the cache; get_facade rebuilds a new one."""

    def test_invalidate_forces_rebuild_with_new_identity(
        self, mutable_client: TestClient
    ) -> None:
        app = mutable_client.app
        # First request builds and caches the facade.
        assert mutable_client.get("/api/project").status_code == 200
        first = app.state.facade
        assert first is not None

        invalidate_facade(app)
        assert app.state.facade is None

        # The next request rebuilds a fresh, distinct instance.
        assert mutable_client.get("/api/project").status_code == 200
        second = app.state.facade
        assert second is not None
        assert second is not first
