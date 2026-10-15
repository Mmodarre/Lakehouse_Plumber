"""Partitioning vs liquid clustering in ``DltTableOptionsValidator`` (#297).

Databricks rejects ``PARTITIONED BY`` together with ``CLUSTER BY`` (and
``CLUSTER BY AUTO`` is part of that clause), so a write target setting
``partition_columns`` alongside ``cluster_columns`` or ``cluster_by_auto: true``
fails validation with one error. ``cluster_columns`` + ``cluster_by_auto`` stays
legal (#282). Exercised through ``ConfigValidator.validate_action`` for every
write path that carries the fields — real validators, no mocks.
"""

import pytest

from lhp.core.validators import ConfigValidator
from lhp.models import Action, ActionType

CONFLICT_ERROR = (
    "Action[0] 'write_orders': cannot specify both 'partition_columns' and "
    "liquid clustering ('cluster_columns' / 'cluster_by_auto') — Databricks "
    "does not allow partitioning together with liquid clustering; remove one"
)

WRITE_PATHS = [
    pytest.param(
        "v_orders",
        {"type": "streaming_table", "create_table": True},
        id="streaming_table_standard",
    ),
    pytest.param(
        "v_orders",
        {
            "type": "streaming_table",
            "mode": "cdc",
            "create_table": True,
            "cdc_config": {"keys": ["order_id"], "sequence_by": "updated_at"},
        },
        id="streaming_table_cdc",
    ),
    pytest.param(
        None,
        {
            "type": "streaming_table",
            "mode": "snapshot_cdc",
            "snapshot_cdc_config": {
                "source": "raw.order_snapshots",
                "keys": ["order_id"],
            },
        },
        id="streaming_table_snapshot_cdc",
    ),
    pytest.param(
        "v_orders",
        {
            "type": "streaming_table",
            "mode": "replace",
            "create_table": True,
            "replace_config": {
                "replace_using": ["order_id"],
                "sequence_by": "updated_at",
            },
        },
        id="streaming_table_replace",
    ),
    pytest.param(
        None,
        {"type": "materialized_view", "sql": "SELECT * FROM silver.orders"},
        id="materialized_view",
    ),
]

CLUSTERING = [
    pytest.param({"cluster_columns": ["order_id"]}, id="cluster_columns"),
    pytest.param({"cluster_by_auto": True}, id="cluster_by_auto"),
    pytest.param(
        {"cluster_columns": ["order_id"], "cluster_by_auto": True},
        id="cluster_columns_and_cluster_by_auto",
    ),
]

UNSET_COMBINATIONS = [
    pytest.param(
        {"partition_columns": [], "cluster_columns": ["order_id"]},
        id="empty_partition_columns",
    ),
    pytest.param(
        {"partition_columns": ["order_date"], "cluster_columns": []},
        id="empty_cluster_columns",
    ),
    pytest.param(
        {"partition_columns": ["order_date"], "cluster_by_auto": False},
        id="cluster_by_auto_false",
    ),
    pytest.param(
        {
            "partition_columns": ["order_date"],
            "cluster_columns": [],
            "cluster_by_auto": False,
        },
        id="both_clustering_fields_unset",
    ),
]


def _errors(source, mode_config, **table_options) -> list[str]:
    action = Action(
        name="write_orders",
        type=ActionType.WRITE,
        source=source,
        write_target={
            "catalog": "cat",
            "schema": "sch",
            "table": "orders",
            **mode_config,
            **table_options,
        },
    )
    return [str(e) for e in ConfigValidator().validate_action(action, 0)]


@pytest.mark.unit
@pytest.mark.parametrize(("source", "mode_config"), WRITE_PATHS)
class TestPartitionWithLiquidClustering:
    def test_partition_columns_alone_is_valid(self, source, mode_config):
        assert _errors(source, mode_config, partition_columns=["order_date"]) == []

    def test_cluster_columns_with_cluster_by_auto_is_valid(self, source, mode_config):
        # #282 deliberately allows the two clustering fields together.
        errors = _errors(
            source, mode_config, cluster_columns=["order_id"], cluster_by_auto=True
        )
        assert errors == []

    @pytest.mark.parametrize("clustering", CLUSTERING)
    def test_partition_with_clustering_is_exactly_one_error(
        self, source, mode_config, clustering
    ):
        errors = _errors(
            source, mode_config, partition_columns=["order_date"], **clustering
        )
        assert errors == [CONFLICT_ERROR]

    @pytest.mark.parametrize("table_options", UNSET_COMBINATIONS)
    def test_empty_list_and_false_count_as_unset(
        self, source, mode_config, table_options
    ):
        assert _errors(source, mode_config, **table_options) == []


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["standard", "cdc"])
def test_rule_applies_when_create_table_is_false(mode):
    mode_config = {"type": "streaming_table", "mode": mode, "create_table": False}
    if mode == "cdc":
        mode_config["cdc_config"] = {"keys": ["order_id"], "sequence_by": "updated_at"}

    errors = _errors(
        "v_orders",
        mode_config,
        partition_columns=["order_date"],
        cluster_columns=["order_id"],
    )

    assert errors == [CONFLICT_ERROR]
