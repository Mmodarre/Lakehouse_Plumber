"""Tests for ``GET /api/lineage`` (table→table dataset lineage).

Runs over the read-only E2E fixture project (``client`` fixture, ``env=dev``)
and the public :meth:`lhp.api.DependencyFacade.build_dataset_index` surface, plus
a handful of monkeypatched-index tests that pin the collision policy, warnings
passthrough, per-env caching, and serve-stale invalidation deterministically.

Concrete fixture anchors (verified against the shared testing_project, env=dev):

- ``acme_edw_dev.edw_gold.customer_lifetime_value_mv`` — a UNIQUE produced table
  (pipeline ``gold_load`` / flowgroup ``customer_lifetime_value`` / action
  ``write_customer_lifetime_value``, standard write) with a multi-hop upstream
  chain and downstream consumers. Used for the happy path (no collision).
- ``acme_edw_dev.edw_bronze.customer`` — produced by TWO write actions in
  ``customer_bronze`` (a natural collision), exercised via a crafted index below
  rather than the fixture so the assertion is drift-proof.
- ``sink:custom/custom_wildcard_sink`` — a sink FQN whose ``/`` proves the
  query-param design (a path param would break routing).
"""

from __future__ import annotations

import asyncio

import pytest

from lhp.api import DatasetIndexResult, DatasetView, DependencyFacade
from lhp.webapp.services import file_watcher

pytestmark = pytest.mark.webapp

ENV = "dev"
TABLE_FQN = "acme_edw_dev.edw_gold.customer_lifetime_value_mv"
SINK_FQN = "sink:custom/custom_wildcard_sink"


def _get(client, fqn, env=ENV):
    return client.get("/api/lineage", params={"env": env, "fqn": fqn})


@pytest.fixture
def build_spy(monkeypatch):
    """Count real ``build_dataset_index`` calls (delegates to the real build)."""
    calls: list[str] = []
    real = DependencyFacade.build_dataset_index

    def spy(self, *, env, force_rebuild=False):
        calls.append(env)
        return real(self, env=env, force_rebuild=force_rebuild)

    monkeypatch.setattr(DependencyFacade, "build_dataset_index", spy)
    return calls


def _stub_index(monkeypatch, result: DatasetIndexResult) -> None:
    """Force ``build_dataset_index`` to return a crafted index for any env."""
    monkeypatch.setattr(
        DependencyFacade,
        "build_dataset_index",
        lambda self, *, env, force_rebuild=False: result,
    )


class TestLineageHappyPath:
    def test_returns_200_for_table(self, client):
        assert _get(client, TABLE_FQN).status_code == 200

    def test_dataset_metadata_fields(self, client):
        data = _get(client, TABLE_FQN).json()
        assert data["fqn"] == TABLE_FQN
        assert data["kind"] == "table"
        assert data["pipeline"] == "gold_load"
        assert data["flowgroup"] == "customer_lifetime_value"
        assert data["action_name"] == "write_customer_lifetime_value"
        assert data["write_mode"] == "standard"
        assert data["scd_type"] is None
        assert data["source_file"].endswith(".yaml")

    def test_chain_nodes_and_edges_shape(self, client):
        data = _get(client, TABLE_FQN).json()
        node_ids = {n["id"] for n in data["nodes"]}
        kinds = {n["kind"] for n in data["nodes"]}
        # The terminal write hop is present and carries the produced FQN.
        write_nodes = [n for n in data["nodes"] if n["kind"] == "write"]
        assert len(write_nodes) == 1
        assert write_nodes[0]["dataset_fqn"] == TABLE_FQN
        # A real upstream chain (at least a load feeding the write).
        assert {"load", "write"} <= kinds
        assert len(data["edges"]) >= 1
        # Every edge endpoint resolves to a node in the chain.
        for edge in data["edges"]:
            assert edge["source"] in node_ids
            assert edge["target"] in node_ids

    def test_consumers_shape(self, client):
        data = _get(client, TABLE_FQN).json()
        assert len(data["consumers"]) >= 1
        for consumer in data["consumers"]:
            assert {"dataset_fqn", "pipeline", "flowgroup", "action_name"} <= set(
                consumer.keys()
            )
            assert consumer["flowgroup"]

    def test_stale_and_warnings_present_and_clean(self, client):
        data = _get(client, TABLE_FQN).json()
        # Fixture (env=dev) resolves cleanly: no project-wide warnings, no
        # collision on this unique FQN, and the graph starts fresh.
        assert data["warnings"] == []
        assert data["stale"] is False


class TestLineageSink:
    def test_sink_lookup_via_query_param_slash(self, client):
        resp = _get(client, SINK_FQN)
        assert resp.status_code == 200
        data = resp.json()
        assert data["fqn"] == SINK_FQN
        assert data["kind"] == "sink"
        assert len(data["nodes"]) >= 1


class TestLineageErrors:
    def test_missing_env_returns_422(self, client):
        # ``env`` is a required query param (mirrors /api/tables).
        assert client.get("/api/lineage", params={"fqn": TABLE_FQN}).status_code == 422

    def test_missing_fqn_returns_422(self, client):
        assert client.get("/api/lineage", params={"env": ENV}).status_code == 422

    def test_unknown_env_returns_400(self, client):
        resp = _get(client, TABLE_FQN, env="nonexistent_env_xyz")
        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"]

    def test_unknown_fqn_returns_404(self, client):
        resp = _get(client, "no.such.table_xyz")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]


class TestLineageWarningsAndCollision:
    def test_project_wide_warnings_passthrough(self, client, monkeypatch):
        dataset = DatasetView(
            fqn="cat.sch.t",
            kind="table",
            pipeline="p",
            flowgroup="f",
            action_name="w",
            write_mode="standard",
            scd_type=None,
            source_file="pipelines/f.yaml",
        )
        result = DatasetIndexResult(
            env=ENV,
            datasets=(dataset,),
            warnings=("Failed to resolve flowgroup 'broken'",),
            fingerprint="deadbeef",
        )
        _stub_index(monkeypatch, result)

        data = _get(client, "cat.sch.t").json()
        assert "Failed to resolve flowgroup 'broken'" in data["warnings"]

    def test_duplicate_fqn_collision_warning(self, client, monkeypatch):
        first = DatasetView(
            fqn="cat.sch.dup",
            kind="table",
            pipeline="p1",
            flowgroup="f1",
            action_name="w1",
            write_mode="standard",
            scd_type=None,
            source_file="a.yaml",
        )
        second = DatasetView(
            fqn="cat.sch.dup",
            kind="table",
            pipeline="p2",
            flowgroup="f2",
            action_name="w2",
            write_mode="standard",
            scd_type=None,
            source_file="b.yaml",
        )
        _stub_index(
            monkeypatch,
            DatasetIndexResult(env=ENV, datasets=(first, second), fingerprint="x"),
        )

        data = _get(client, "cat.sch.dup").json()
        # The FIRST writer in deterministic order is served.
        assert data["pipeline"] == "p1"
        assert data["flowgroup"] == "f1"
        assert data["action_name"] == "w1"
        # The other writer is named, never silently dropped.
        assert (
            "multiple writers for cat.sch.dup: also written by p2/f2/w2"
            in data["warnings"]
        )


class TestLineageStale:
    def test_stale_false_by_default(self, client):
        assert _get(client, TABLE_FQN).json()["stale"] is False

    def test_stale_reflects_graph_stale_flag(self, client):
        client.app.state.graph_stale = True
        assert _get(client, TABLE_FQN).json()["stale"] is True


class TestLineageCacheAndInvalidation:
    def test_second_call_same_env_uses_cache(self, client, build_spy):
        assert _get(client, TABLE_FQN).status_code == 200
        assert _get(client, SINK_FQN).status_code == 200
        # One build backs both lookups: the per-env index is cached.
        assert build_spy == [ENV]

    def test_watcher_graph_relevant_edit_keeps_index_consistent(
        self, client, build_spy, monkeypatch
    ):
        # Regression (#7): a graph-relevant edit flips graph_stale and serves the
        # dependency graph stale, but must NOT drop the lineage index. Dropping
        # it while the graph memo survives would rebuild the index against that
        # stale graph and false-404 a produced dataset whose write node is not in
        # the memo. The last-good index is kept (no rebuild) so it stays
        # consistent with the served-stale graph; stale=True is the honest skew
        # signal until an explicit refresh.
        app = client.app
        assert _get(client, TABLE_FQN).status_code == 200
        assert build_spy == [ENV]

        # One watcher tick sees a graph-relevant edit (no real file I/O).
        monkeypatch.setattr(
            file_watcher, "scan_tree", lambda root: {"pipelines/x.yaml": (2.0, 10)}
        )
        asyncio.run(file_watcher._tick(app, {"pipelines/x.yaml": (1.0, 10)}))
        assert app.state.graph_stale is True

        # The produced dataset still resolves — NOT a false 404 — and no rebuild
        # happened: the last-good index is served, now flagged stale.
        resp = _get(client, TABLE_FQN)
        assert resp.status_code == 200
        assert resp.json()["stale"] is True
        assert build_spy == [ENV]  # index kept: never rebuilt against a stale graph

    def test_refresh_drops_cache(self, mutable_client, build_spy):
        assert _get(mutable_client, TABLE_FQN).status_code == 200
        assert build_spy == [ENV]

        assert mutable_client.post("/api/dependencies/refresh").status_code == 200

        assert _get(mutable_client, TABLE_FQN).status_code == 200
        assert build_spy == [ENV, ENV]  # refresh dropped the cache → rebuilt
