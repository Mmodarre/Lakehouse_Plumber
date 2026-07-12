"""Serve-stale integration: an out-of-API edit refreshes inspection reads
while the dependency graph keeps serving the last-good (stale) result.

Pins the FIX-1 contract for the ``lhp web`` serve-stale model. When a
graph-relevant edit is made OUTSIDE the write API (an external editor, or the
live designer writing a YAML), one file-watcher tick must, against the SAME
cached facade (never dropped):

* clear the flowgroup-discovery memo so the inspection endpoints
  (``/api/flowgroups``) re-read the edited project — freshness restored; AND
* leave the in-process dependency-graph memo untouched so the graph endpoints
  keep serving the last-good result with ``graph_stale=True`` until an explicit
  ``POST /api/dependencies/refresh`` rebuilds it — serve-stale preserved.

Dropping the whole facade (the naive ``invalidate_facade``) would satisfy the
first half but destroy the second; this test proves both hold simultaneously.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.services import file_watcher

pytestmark = pytest.mark.webapp

# A self-contained flowgroup in a BRAND-NEW pipeline (modelled on the proven
# region_bronze template). New-pipeline so a REBUILT dependency graph gains a
# new ``pipeline_dependencies`` key — the discriminator between a stale
# (memoized) graph and a freshly-rebuilt one.
_PROBE_PIPELINE = "staleness_probe_pipeline"
_PROBE_PATH = "pipelines/02_bronze/statics/staleness_probe.yaml"
_PROBE_FLOWGROUP = "staleness_probe_fg"
_PROBE_YAML = """\
pipeline: staleness_probe_pipeline
flowgroup: staleness_probe_fg
presets:
  - default_delta_properties
actions:
  - name: probe_raw_load
    type: load
    readMode: stream
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: region_raw
    target: v_probe_raw
  - name: probe_cleanse
    type: transform
    transform_type: sql
    source: v_probe_raw
    target: v_probe_cleaned
    sql: |
      SELECT * FROM stream(v_probe_raw)
  - name: write_probe
    type: write
    source: v_probe_cleaned
    write_target:
      create_table: true
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: "staleness_probe"
"""


def _flowgroup_names(client: TestClient) -> set[str]:
    resp = client.get("/api/flowgroups")
    assert resp.status_code == 200
    return {fg["name"] for fg in resp.json()["flowgroups"]}


def _dependencies(client: TestClient) -> dict:
    resp = client.get("/api/dependencies")
    assert resp.status_code == 200
    return resp.json()


def _source_file_for(client: TestClient, name: str) -> str:
    resp = client.get("/api/flowgroups")
    assert resp.status_code == 200
    for fg in resp.json()["flowgroups"]:
        if fg["name"] == name:
            return fg["source_file"]
    raise AssertionError(f"flowgroup {name!r} not present in /api/flowgroups")


def test_out_of_api_edit_refreshes_inspection_but_serves_stale_graph(
    mutable_client: TestClient,
) -> None:
    app = mutable_client.app
    project_root: Path = app.state.settings.project_root

    # Warm BOTH caches on the same cached facade: the discovery memo (browse)
    # and the in-process dependency-graph memo (analysis).
    assert _PROBE_FLOWGROUP not in _flowgroup_names(mutable_client)
    dep_before = _dependencies(mutable_client)
    assert _PROBE_PIPELINE not in dep_before["pipeline_dependencies"]

    # Establish the watcher baseline BEFORE the edit (previous=None publishes
    # nothing and records the pre-edit tree).
    baseline = asyncio.run(file_watcher._tick(app, None))
    assert app.state.graph_stale is False

    # Out-of-API edit: write a new flowgroup DIRECTLY to disk, bypassing the
    # write API and its own facade invalidation.
    (project_root / _PROBE_PATH).write_text(_PROBE_YAML, encoding="utf-8")

    # One watcher tick sees the graph-relevant add.
    asyncio.run(file_watcher._tick(app, baseline))
    assert app.state.graph_stale is True

    # The facade was NOT dropped — serve-stale depends on it (and its graph
    # memo) surviving.
    assert app.state.facades.get(None) is not None

    # HALF 1 — inspection freshness restored: the new flowgroup is now visible.
    assert _PROBE_FLOWGROUP in _flowgroup_names(mutable_client)

    # HALF 2 — graph serve-stale preserved: the analysis memo still returns the
    # byte-identical pre-edit result (the new pipeline is absent).
    dep_stale = _dependencies(mutable_client)
    assert dep_stale == dep_before
    assert _PROBE_PIPELINE not in dep_stale["pipeline_dependencies"]

    # The staleness endpoint reports stale from the watcher-set flag.
    staleness = mutable_client.get("/api/dependencies/staleness")
    assert staleness.status_code == 200
    assert staleness.json()["stale"] is True

    # Explicit refresh rebuilds the graph and clears the flag: the new pipeline
    # now appears and the stale flag is cleared.
    refresh = mutable_client.post("/api/dependencies/refresh")
    assert refresh.status_code == 200
    assert refresh.json()["stale"] is False
    assert app.state.graph_stale is False

    dep_fresh = _dependencies(mutable_client)
    assert _PROBE_PIPELINE in dep_fresh["pipeline_dependencies"]


def test_out_of_api_add_refreshes_source_path_index_while_graph_stays_stale(
    mutable_client: TestClient,
) -> None:
    """A newly ADDED flowgroup must resolve its source YAML after one tick.

    Freshness of the name list (proved by the sibling test) is not enough: the
    discovery service memoizes a lazily-built ``(pipeline, flowgroup) -> path``
    index that returns ``None`` on a miss and never re-scans on its own. Before
    the fix, ``reset_discovery_cache`` cleared the bootstrap memo but NOT that
    downstream index, so a graph-relevant out-of-API ADD left the new flowgroup
    with an empty ``source_file`` in ``/api/flowgroups`` and made
    ``find_source_yaml_for_flowgroup`` return ``None`` until a full facade drop.
    This must now hold WHILE the dependency graph is still served stale.
    """
    app = mutable_client.app
    project_root: Path = app.state.settings.project_root

    # Warm the discovery memo AND the source-path index: listing builds a source
    # path for every existing flowgroup, so the index is populated (and the
    # probe is a genuine miss) BEFORE the edit — the precondition that makes a
    # non-re-scanning index observable.
    assert _PROBE_FLOWGROUP not in _flowgroup_names(mutable_client)
    dep_before = _dependencies(mutable_client)

    facade = app.state.facades.get(None)
    assert facade is not None
    assert facade.inspection.find_source_yaml_for_flowgroup(_PROBE_FLOWGROUP) is None

    baseline = asyncio.run(file_watcher._tick(app, None))
    assert app.state.graph_stale is False

    # Out-of-API ADD: a brand-new flowgroup file at a path never seen before.
    (project_root / _PROBE_PATH).write_text(_PROBE_YAML, encoding="utf-8")

    asyncio.run(file_watcher._tick(app, baseline))
    assert app.state.graph_stale is True
    assert app.state.facades.get(None) is facade  # facade NOT dropped

    # HALF 1 — source-path index refreshed: the add now resolves to its YAML,
    # both via the direct lookup and via the browse endpoint's source_file.
    resolved = facade.inspection.find_source_yaml_for_flowgroup(_PROBE_FLOWGROUP)
    assert resolved == project_root / _PROBE_PATH
    assert _source_file_for(mutable_client, _PROBE_FLOWGROUP) == _PROBE_PATH

    # HALF 2 — graph serve-stale preserved: the analysis memo is byte-identical
    # to the pre-edit result and the staleness flag reports stale.
    assert _dependencies(mutable_client) == dep_before
    staleness = mutable_client.get("/api/dependencies/staleness")
    assert staleness.status_code == 200
    assert staleness.json()["stale"] is True
