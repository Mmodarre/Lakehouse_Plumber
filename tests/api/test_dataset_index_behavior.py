"""Behavioural tests for ``DependencyFacade.build_dataset_index(env=...)``.

Drives the table→table lineage capability end-to-end through the public
entrypoint ``LakehousePlumberApplicationFacade.for_project(...).dependency``
against a small real on-disk project (the ``_write_project`` /
``for_project`` pattern shared with ``test_analyze_dependencies_graphs.py``
and ``test_action_view_write_metadata.py``).

The fixture exercises every §6.7 join path:

- a single-flowgroup chain (external source → load → transform → write → table),
  with the write target resolved through ``${token}`` substitution;
- a cross-flowgroup upstream ``dataset`` node (silver reads bronze's table);
- downstream consumers (bronze's table read by a silver flowgroup);
- a sink entry (``kind == "sink"``, ``sink:<type>/<id>`` fqn);
- an action name embedding ``${token}`` → positional fallback + warning;
- a flowgroup that fails env resolution → warning + skip.

``enforce_version=False`` relaxes the ``required_lhp_version`` gate;
``no_cache=True`` keeps the on-disk graph cache out of the tmp project.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from lhp.api import DatasetIndexResult, LakehousePlumberApplicationFacade

pytestmark = pytest.mark.unit

ENV = "dev"


def _write_project(root: Path) -> None:
    (root / "lhp.yaml").write_text(
        textwrap.dedent("""\
            name: dataset_index_project
            version: "1.0"
            """)
    )
    for sub in ("presets", "templates"):
        (root / sub).mkdir(parents=True, exist_ok=True)

    subs = root / "substitutions"
    subs.mkdir(parents=True, exist_ok=True)
    (subs / f"{ENV}.yaml").write_text(
        textwrap.dedent(f"""\
            {ENV}:
              catalog: dev_catalog
              schema_bronze: bronze
              schema_silver: silver
              table_suffix: data
            """)
    )

    # bronze: external -> load -> transform -> write (token-resolved FQN).
    bronze = root / "pipelines" / "bronze"
    bronze.mkdir(parents=True, exist_ok=True)
    (bronze / "customers.yaml").write_text(
        textwrap.dedent("""\
            pipeline: bronze
            flowgroup: fg_customers
            actions:
              - name: load_customers
                type: load
                source:
                  type: sql
                  sql: "SELECT * FROM raw_db.landing_customers"
                target: v_customers_raw
              - name: clean_customers
                type: transform
                transform_type: sql
                source: v_customers_raw
                sql: "SELECT * FROM v_customers_raw"
                target: v_customers_clean
              - name: write_customers
                type: write
                source: v_customers_clean
                write_target:
                  type: streaming_table
                  database: "${catalog}.${schema_bronze}"
                  table: customers
            """)
    )

    # silver: reads bronze's table (cross-flowgroup dataset upstream) + writes.
    silver = root / "pipelines" / "silver"
    silver.mkdir(parents=True, exist_ok=True)
    (silver / "orders.yaml").write_text(
        textwrap.dedent("""\
            pipeline: silver
            flowgroup: fg_orders
            actions:
              - name: load_orders
                type: load
                source:
                  type: delta
                  database: "${catalog}.${schema_bronze}"
                  table: customers
                target: v_orders_raw
              - name: write_orders
                type: write
                source: v_orders_raw
                write_target:
                  type: streaming_table
                  database: "${catalog}.${schema_silver}"
                  table: orders
            """)
    )

    # silver: a sink write target.
    (silver / "events.yaml").write_text(
        textwrap.dedent("""\
            pipeline: silver
            flowgroup: fg_events
            actions:
              - name: load_events
                type: load
                source:
                  type: sql
                  sql: "SELECT * FROM raw_db.landing_events"
                target: v_events
              - name: write_events_sink
                type: write
                source: v_events
                write_target:
                  type: sink
                  sink_type: delta
                  sink_name: events_sink
                  options:
                    tableName: dev_catalog.silver.events_archive
            """)
    )

    # gold: a write action whose NAME embeds a token -> positional fallback.
    gold = root / "pipelines" / "gold"
    gold.mkdir(parents=True, exist_ok=True)
    (gold / "metrics.yaml").write_text(
        textwrap.dedent("""\
            pipeline: gold
            flowgroup: fg_tokenized
            actions:
              - name: load_metrics
                type: load
                source:
                  type: sql
                  sql: "SELECT * FROM raw_db.landing_metrics"
                target: v_metrics
              - name: "write_${table_suffix}"
                type: write
                source: v_metrics
                write_target:
                  type: streaming_table
                  database: "${catalog}.${schema_silver}"
                  table: metrics
            """)
    )

    # gold: a flowgroup that references a missing template -> resolution fails.
    (gold / "broken.yaml").write_text(
        textwrap.dedent("""\
            pipeline: gold
            flowgroup: fg_broken
            use_template: nonexistent_template
            """)
    )


@pytest.fixture
def facade(tmp_path: Path) -> LakehousePlumberApplicationFacade:
    _write_project(tmp_path)
    return LakehousePlumberApplicationFacade.for_project(
        tmp_path, enforce_version=False, no_cache=True
    )


@pytest.fixture
def index(facade: LakehousePlumberApplicationFacade) -> DatasetIndexResult:
    return facade.dependency.build_dataset_index(env=ENV)


def _by_fqn(index: DatasetIndexResult, fqn: str):  # type: ignore[no-untyped-def]
    return next(d for d in index.datasets if d.fqn == fqn)


class TestTopLevelDatasets:
    def test_env_recorded(self, index: DatasetIndexResult) -> None:
        assert index.env == ENV

    def test_writes_become_datasets_with_resolved_fqns(
        self, index: DatasetIndexResult
    ) -> None:
        fqns = {d.fqn for d in index.datasets}
        assert fqns == {
            "dev_catalog.bronze.customers",
            "dev_catalog.silver.orders",
            "dev_catalog.silver.metrics",
            "sink:delta/events_sink",
        }

    def test_session_views_are_never_top_level(self, index: DatasetIndexResult) -> None:
        assert not any(d.fqn.startswith("v_") for d in index.datasets)

    def test_fingerprint_is_stable_and_nonempty(
        self, facade: LakehousePlumberApplicationFacade, index: DatasetIndexResult
    ) -> None:
        assert index.fingerprint
        again = facade.dependency.build_dataset_index(env=ENV)
        assert again.fingerprint == index.fingerprint


class TestChainShape:
    def test_source_load_transform_write_chain(self, index: DatasetIndexResult) -> None:
        customers = _by_fqn(index, "dev_catalog.bronze.customers")
        assert customers.kind == "table"
        assert customers.pipeline == "bronze"
        assert customers.flowgroup == "fg_customers"
        assert customers.action_name == "write_customers"
        assert customers.source_file == "pipelines/bronze/customers.yaml"

        by_kind = {n.kind for n in customers.nodes}
        assert {"external", "load", "transform", "write"} <= by_kind

        # An external source node feeds the load.
        external = [n for n in customers.nodes if n.kind == "external"]
        assert external and "landing_customers" in external[0].label

        # Write node carries the resolved FQN.
        write_node = next(n for n in customers.nodes if n.kind == "write")
        assert write_node.dataset_fqn == "dev_catalog.bronze.customers"

        # Edges form a connected producer->consumer chain into the write.
        targets = {e.target for e in customers.edges}
        assert write_node.id in targets


class TestCrossFlowgroupUpstream:
    def test_upstream_dataset_node_has_resolved_fqn(
        self, index: DatasetIndexResult
    ) -> None:
        orders = _by_fqn(index, "dev_catalog.silver.orders")
        dataset_nodes = [n for n in orders.nodes if n.kind == "dataset"]
        assert len(dataset_nodes) == 1
        upstream = dataset_nodes[0]
        assert upstream.dataset_fqn == "dev_catalog.bronze.customers"
        assert upstream.id == "dataset:dev_catalog.bronze.customers"
        # It points at a same-flowgroup node.
        assert any(e.source == upstream.id for e in orders.edges)


class TestDownstreamConsumers:
    def test_producer_lists_foreign_consumer(self, index: DatasetIndexResult) -> None:
        customers = _by_fqn(index, "dev_catalog.bronze.customers")
        consumers = customers.consumers
        assert len(consumers) == 1
        consumer = consumers[0]
        assert consumer.pipeline == "silver"
        assert consumer.flowgroup == "fg_orders"
        assert consumer.action_name == "load_orders"
        # Consumer FQN is the consuming flowgroup's own produced dataset.
        assert consumer.dataset_fqn == "dev_catalog.silver.orders"

    def test_terminal_dataset_has_no_consumers(self, index: DatasetIndexResult) -> None:
        orders = _by_fqn(index, "dev_catalog.silver.orders")
        assert orders.consumers == ()


class TestSinkEntry:
    def test_sink_dataset(self, index: DatasetIndexResult) -> None:
        sink = _by_fqn(index, "sink:delta/events_sink")
        assert sink.kind == "sink"
        assert sink.pipeline == "silver"
        assert sink.flowgroup == "fg_events"
        assert sink.action_name == "write_events_sink"


class TestPositionalFallback:
    def test_token_in_action_name_falls_back_with_warning(
        self, index: DatasetIndexResult
    ) -> None:
        # The write action name resolves to write_data (from ${table_suffix}),
        # which does not match the substitution-agnostic graph node id.
        metrics = _by_fqn(index, "dev_catalog.silver.metrics")
        assert metrics.action_name == "write_data"
        assert any(
            "positional matching" in w and "fg_tokenized" in w for w in index.warnings
        )


class TestUnresolvableFlowgroup:
    def test_failed_flowgroup_warns_and_is_skipped(
        self, index: DatasetIndexResult
    ) -> None:
        assert any(
            "fg_broken" in w and "Failed to resolve" in w for w in index.warnings
        )
        assert not any(d.flowgroup == "fg_broken" for d in index.datasets)
