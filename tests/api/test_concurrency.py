"""ETag-based optimistic concurrency control tests.

TDD tests -- these validate that ETag concurrency semantics work correctly
across Phase 2 CRUD endpoints (flowgroups, environments, presets, templates).

ETag flow:
  1. Client GETs a resource -> response includes ETag header
  2. Client PUTs with If-Match: {etag} -> succeeds if file hasn't changed
  3. Second client PUTs with same stale If-Match -> gets 412 Precondition Failed
  4. Client PUTs WITHOUT If-Match -> last-write-wins (always succeeds)

All tests use ``mutable_client`` (session-scoped) because they write to disk.
Each test uses unique resource names (containing "concurrency") to avoid
collisions with other test modules sharing the session-scoped project copy.
"""

import uuid

import pytest

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid(prefix: str = "concurrency") -> str:
    """Return a unique name for test isolation within the session-scoped project."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_flowgroup(client, *, pipeline: str, flowgroup: str):
    """POST a new flowgroup and return the response."""
    return client.post(
        "/api/flowgroups",
        json={
            "pipeline": pipeline,
            "flowgroup": flowgroup,
            "config": {"catalog": "main", "target": "schema.concurrency_test"},
        },
    )


def _make_preset(name: str) -> dict:
    """Return a minimal preset payload with the given name."""
    return {
        "name": name,
        "version": "1.0",
        "defaults": {
            "load_actions": {},
            "write_actions": {
                "streaming_table": {
                    "table_properties": {
                        "delta.enableRowTracking": "true",
                    }
                }
            },
        },
    }


# ---------------------------------------------------------------------------
# TestConcurrentFlowgroupWrites
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestConcurrentFlowgroupWrites:
    """Verify ETag-based optimistic concurrency for flowgroup updates.

    The flowgroup ETag is obtained via GET /api/flowgroups/{name}/source
    which returns the raw YAML content and an ETag header computed from
    the file contents on disk.
    """

    def test_two_updates_same_etag_second_gets_412(self, mutable_client):
        """Simulate two clients that both read the same ETag, then race to
        update.  The first PUT succeeds; the second PUT with the now-stale
        ETag must receive 412 Precondition Failed."""
        name = _uid("fg_etag_race")
        create_resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        assert create_resp.status_code == 201

        # Both clients read the same ETag (simulated by a single GET)
        source_resp = mutable_client.get(f"/api/flowgroups/{name}/source")
        assert source_resp.status_code == 200
        shared_etag = source_resp.headers["ETag"]

        # First client updates with If-Match — should succeed
        resp_first = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "client_a_wins"}},
            headers={"If-Match": shared_etag},
        )
        assert resp_first.status_code == 200

        # Second client attempts update with the same (now stale) ETag — 412
        resp_second = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "client_b_loses"}},
            headers={"If-Match": shared_etag},
        )
        assert resp_second.status_code == 412

    def test_update_without_etag_succeeds_always(self, mutable_client):
        """When no If-Match header is sent, the server uses last-write-wins
        semantics — both updates must succeed regardless of intervening changes."""
        name = _uid("fg_no_etag")
        create_resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        assert create_resp.status_code == 201

        # First update without If-Match
        resp1 = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "update_one"}},
        )
        assert resp1.status_code == 200

        # Second update without If-Match (last write wins)
        resp2 = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "update_two"}},
        )
        assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# TestConcurrentEnvironmentWrites
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestConcurrentEnvironmentWrites:
    """Verify ETag-based optimistic concurrency for environment updates.

    The environment ETag is obtained via GET /api/environments/{name}
    which returns the tokens dict and an ETag header.
    """

    def test_environment_etag_conflict(self, mutable_client):
        """Create -> GET etag -> first PUT succeeds -> second PUT with
        same stale etag returns 412."""
        env_name = _uid("env_etag_conflict")

        # Create environment
        create_resp = mutable_client.post(
            "/api/environments",
            json={
                "name": env_name,
                "tokens": {"catalog": f"{env_name}_catalog", "env": env_name},
            },
        )
        assert create_resp.status_code == 201

        # GET the current ETag
        get_resp = mutable_client.get(f"/api/environments/{env_name}")
        assert get_resp.status_code == 200
        shared_etag = get_resp.headers["etag"]

        # First update with matching ETag — succeeds
        resp_first = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "client_a_catalog", "env": env_name}},
            headers={"If-Match": shared_etag},
        )
        assert resp_first.status_code == 200

        # Second update with the same (now stale) ETag — 412
        resp_second = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "client_b_catalog", "env": env_name}},
            headers={"If-Match": shared_etag},
        )
        assert resp_second.status_code == 412


# ---------------------------------------------------------------------------
# TestConcurrentPresetWrites
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestConcurrentPresetWrites:
    """Verify ETag-based optimistic concurrency for preset updates.

    The preset ETag is obtained via GET /api/presets/{name} which
    returns the preset detail and an ETag header.
    """

    def test_preset_etag_conflict(self, mutable_client):
        """Create -> GET etag -> first PUT succeeds -> second PUT with
        same stale etag returns 412."""
        preset_name = _uid("preset_etag_conflict")
        payload = _make_preset(preset_name)

        # Create preset
        create_resp = mutable_client.post("/api/presets", json=payload)
        assert create_resp.status_code == 201

        # GET to obtain the current ETag
        get_resp = mutable_client.get(f"/api/presets/{preset_name}")
        assert get_resp.status_code == 200
        shared_etag = get_resp.headers["ETag"]

        # First update with matching ETag — succeeds
        updated_payload = _make_preset(preset_name)
        updated_payload["version"] = "2.0"
        resp_first = mutable_client.put(
            f"/api/presets/{preset_name}",
            json=updated_payload,
            headers={"If-Match": shared_etag},
        )
        assert resp_first.status_code == 200

        # Second update with the same (now stale) ETag — 412
        updated_payload["version"] = "3.0"
        resp_second = mutable_client.put(
            f"/api/presets/{preset_name}",
            json=updated_payload,
            headers={"If-Match": shared_etag},
        )
        assert resp_second.status_code == 412
