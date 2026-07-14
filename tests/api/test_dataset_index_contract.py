"""DTO contract tests for the dataset-index lineage surface.

Covers the public, frozen return shape
:class:`lhp.api.responses.DatasetIndexResult` and its nested view DTOs
(:class:`~lhp.api.views.DatasetView`, :class:`~lhp.api.views.LineageNodeView`,
:class:`~lhp.api.views.LineageEdgeView`,
:class:`~lhp.api.views.DatasetConsumerView`) returned by
:meth:`lhp.api.DependencyFacade.build_dataset_index` — the ``lhp web``
table→table lineage capability (§8.3 + §9.15).

Every field must be JSON-serialisable, each type is frozen, and the nested
projection must survive both pickle and JSON round-trips so external consumers
(the web IDE, future clients) can capture the lineage index across process and
wire boundaries.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from typing import Mapping

import pytest

from lhp.api import to_dict
from lhp.api.responses import DatasetIndexResult
from lhp.api.views import (
    DatasetConsumerView,
    DatasetView,
    LineageEdgeView,
    LineageNodeView,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def sample_result() -> DatasetIndexResult:
    """A two-dataset index: a table with full lineage plus a sink."""
    customers = DatasetView(
        fqn="dev_catalog.bronze.customers",
        kind="table",
        pipeline="bronze",
        flowgroup="fg_customers",
        action_name="write_customers",
        write_mode="standard",
        scd_type=None,
        source_file="pipelines/bronze/customers.yaml",
        nodes=(
            LineageNodeView(
                id="external:raw_db.landing_customers",
                kind="external",
                label="raw_db.landing_customers",
            ),
            LineageNodeView(
                id="bronze.fg_customers.load_customers",
                kind="load",
                label="v_customers_raw",
                pipeline="bronze",
                flowgroup="fg_customers",
            ),
            LineageNodeView(
                id="bronze.fg_customers.write_customers",
                kind="write",
                label="dev_catalog.bronze.customers",
                pipeline="bronze",
                flowgroup="fg_customers",
                dataset_fqn="dev_catalog.bronze.customers",
            ),
        ),
        edges=(
            LineageEdgeView(
                source="external:raw_db.landing_customers",
                target="bronze.fg_customers.load_customers",
            ),
            LineageEdgeView(
                source="bronze.fg_customers.load_customers",
                target="bronze.fg_customers.write_customers",
            ),
        ),
        consumers=(
            DatasetConsumerView(
                dataset_fqn="dev_catalog.silver.orders",
                pipeline="silver",
                flowgroup="fg_orders",
                action_name="load_orders",
            ),
        ),
    )
    events_sink = DatasetView(
        fqn="sink:delta/events_sink",
        kind="sink",
        pipeline="silver",
        flowgroup="fg_events",
        action_name="write_events_sink",
        write_mode="",
        scd_type=None,
        source_file="pipelines/silver/events.yaml",
    )
    return DatasetIndexResult(
        env="dev",
        datasets=(customers, events_sink),
        warnings=("Failed to resolve flowgroup 'fg_broken'",),
        fingerprint="a1b2c3d4e5f60718",
    )


def _node_from(payload: Mapping[str, object]) -> LineageNodeView:
    return LineageNodeView(**payload)  # type: ignore[arg-type]


def _edge_from(payload: Mapping[str, object]) -> LineageEdgeView:
    return LineageEdgeView(**payload)  # type: ignore[arg-type]


def _consumer_from(payload: Mapping[str, object]) -> DatasetConsumerView:
    return DatasetConsumerView(**payload)  # type: ignore[arg-type]


def _dataset_from(payload: Mapping[str, object]) -> DatasetView:
    return DatasetView(
        fqn=payload["fqn"],  # type: ignore[arg-type]
        kind=payload["kind"],  # type: ignore[arg-type]
        pipeline=payload["pipeline"],  # type: ignore[arg-type]
        flowgroup=payload["flowgroup"],  # type: ignore[arg-type]
        action_name=payload["action_name"],  # type: ignore[arg-type]
        write_mode=payload["write_mode"],  # type: ignore[arg-type]
        scd_type=payload["scd_type"],  # type: ignore[arg-type]
        source_file=payload["source_file"],  # type: ignore[arg-type]
        nodes=tuple(_node_from(n) for n in payload["nodes"]),  # type: ignore[union-attr]
        edges=tuple(_edge_from(e) for e in payload["edges"]),  # type: ignore[union-attr]
        consumers=tuple(
            _consumer_from(c)
            for c in payload["consumers"]  # type: ignore[union-attr]
        ),
    )


def _result_from(payload: Mapping[str, object]) -> DatasetIndexResult:
    return DatasetIndexResult(
        env=payload["env"],  # type: ignore[arg-type]
        datasets=tuple(
            _dataset_from(d)
            for d in payload["datasets"]  # type: ignore[union-attr]
        ),
        warnings=tuple(payload["warnings"]),  # type: ignore[arg-type]
        fingerprint=payload["fingerprint"],  # type: ignore[arg-type]
    )


_FROZEN_TYPES = (
    DatasetIndexResult,
    DatasetView,
    LineageNodeView,
    LineageEdgeView,
    DatasetConsumerView,
)


class TestFrozenContract:
    @pytest.mark.parametrize("cls", _FROZEN_TYPES)
    def test_is_frozen(self, cls: type) -> None:
        params = getattr(cls, "__dataclass_params__", None)
        assert params is not None
        assert params.frozen is True

    def test_mutation_raises(self, sample_result: DatasetIndexResult) -> None:
        with pytest.raises(FrozenInstanceError):
            sample_result.env = "prod"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            sample_result.datasets[0].fqn = "x"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            sample_result.datasets[0].nodes[0].label = "x"  # type: ignore[misc]


class TestPickleRoundTrip:
    def test_pickle(self, sample_result: DatasetIndexResult) -> None:
        assert pickle.loads(pickle.dumps(sample_result)) == sample_result


class TestJSONRoundTrip:
    def test_json_round_trip(self, sample_result: DatasetIndexResult) -> None:
        wire = json.loads(json.dumps(to_dict(sample_result)))
        restored = _result_from(wire)
        assert restored == sample_result

    def test_nested_lineage_survives(self, sample_result: DatasetIndexResult) -> None:
        wire = json.loads(json.dumps(to_dict(sample_result)))
        restored = _result_from(wire)
        customers = restored.datasets[0]
        assert customers.kind == "table"
        assert {n.kind for n in customers.nodes} == {"external", "load", "write"}
        assert (
            LineageEdgeView(
                source="bronze.fg_customers.load_customers",
                target="bronze.fg_customers.write_customers",
            )
            in customers.edges
        )
        assert customers.consumers[0].dataset_fqn == "dev_catalog.silver.orders"
        assert restored.datasets[1].kind == "sink"


_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "DTOs must not carry live Exception instances (§4.8).",
    "LHPError": "DTOs must not carry LHPError instances (§4.8).",
    "BaseModel": "Public DTOs must not embed Pydantic models (§9.12).",
}


def _annotation_strings(cls: type) -> dict[str, str]:
    return {
        f.name: f.type if isinstance(f.type, str) else str(f.type)
        for f in dataclasses.fields(cls)
    }


class TestFieldTypeContract:
    @pytest.mark.parametrize("cls", _FROZEN_TYPES)
    def test_no_banned_annotations(self, cls: type) -> None:
        for name, annotation in _annotation_strings(cls).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"{cls.__name__}.{name}: annotation {annotation!r} contains "
                    f"banned token {needle!r}. {reason}"
                )

    @pytest.mark.parametrize("cls", _FROZEN_TYPES)
    def test_no_any(self, cls: type) -> None:
        for name, annotation in _annotation_strings(cls).items():
            assert "Any" not in annotation, (
                f"{cls.__name__}.{name}: annotation {annotation!r} contains 'Any'."
            )


class TestPublicExport:
    @pytest.mark.parametrize(
        "name",
        [
            "DatasetIndexResult",
            "DatasetView",
            "LineageNodeView",
            "LineageEdgeView",
            "DatasetConsumerView",
        ],
    )
    def test_in_lhp_api(self, name: str) -> None:
        import lhp.api as api

        assert name in api.__all__
        assert getattr(api, name) is not None
