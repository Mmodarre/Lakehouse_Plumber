"""Pickle round-trip invariants for :class:`FlowGroupContext`.

Workers receive :class:`FlowGroupContext` envelopes across the ``spawn``
process boundary; :mod:`pickle` is the serialisation contract. The
envelope (a frozen, slotted dataclass) and the FlowGroup it wraps both
participate in the same wire format, so a round-trip MUST survive
without loss for any combination of optional fields.

These tests mirror the structure of
``tests/test_pipeline_delta_serialization.py`` (the canonical wire-format
test for ``PipelineDelta``).
"""

from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from lhp.models.config import (
    Action,
    ActionType,
    FlowGroup,
    FlowGroupContext,
    LoadSourceType,
)


def _make_flowgroup(
    pipeline: str = "bronze_ingest",
    flowgroup: str = "customers",
) -> FlowGroup:
    """Build a small but non-trivial FlowGroup with one realistic action."""
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        actions=[
            Action(
                name="load_customers",
                type=ActionType.LOAD,
                source={
                    "type": LoadSourceType.SQL.value,
                    "sql": "SELECT * FROM raw.customers",
                },
                target="v_customers",
            )
        ],
    )


class TestFlowGroupContextSerialization:
    def test_default_context_round_trips(self):
        original = FlowGroupContext(
            flowgroup=_make_flowgroup(),
            source_yaml=Path("pipelines/bronze_ingest/customers.yaml"),
        )
        restored = pickle.loads(pickle.dumps(original))
        assert restored.flowgroup.pipeline == "bronze_ingest"
        assert restored.flowgroup.flowgroup == "customers"
        assert restored.source_yaml == Path(
            "pipelines/bronze_ingest/customers.yaml"
        )
        assert restored.synthetic is False
        assert dict(restored.auxiliary_files) == {}
        assert restored.had_test_actions is False

    def test_synthetic_context_round_trips(self):
        original = FlowGroupContext(
            flowgroup=_make_flowgroup("monitoring", "monitoring"),
            source_yaml=None,
            synthetic=True,
            auxiliary_files={"jobs_stats_loader.py": "def get_jobs_stats(): ..."},
            had_test_actions=True,
        )
        restored = pickle.loads(pickle.dumps(original))
        assert restored.flowgroup.pipeline == "monitoring"
        assert restored.source_yaml is None
        assert restored.synthetic is True
        assert dict(restored.auxiliary_files) == {
            "jobs_stats_loader.py": "def get_jobs_stats(): ..."
        }
        assert restored.had_test_actions is True

    @pytest.mark.parametrize(
        "source_yaml",
        [
            None,
            Path("blueprints/erp.yaml"),
            Path("pipelines/silver/joins.yaml"),
        ],
    )
    def test_source_yaml_variants_round_trip(self, source_yaml: Path | None):
        """``source_yaml`` accepts ``Path | None`` — both must survive pickle."""
        original = FlowGroupContext(
            flowgroup=_make_flowgroup(),
            source_yaml=source_yaml,
        )
        restored = pickle.loads(pickle.dumps(original))
        assert restored.source_yaml == source_yaml

    def test_repeated_round_trips_are_stable(self):
        """Double round-trip keeps all fields equal — no aliasing on frozen slots."""
        original = FlowGroupContext(
            flowgroup=_make_flowgroup(),
            source_yaml=Path("pipelines/x.yaml"),
            synthetic=True,
            auxiliary_files={"a.py": "x = 1"},
            had_test_actions=True,
        )
        once = pickle.loads(pickle.dumps(original))
        twice = pickle.loads(pickle.dumps(once))
        assert original.flowgroup.pipeline == twice.flowgroup.pipeline
        assert original.flowgroup.flowgroup == twice.flowgroup.flowgroup
        assert original.source_yaml == twice.source_yaml
        assert original.synthetic == twice.synthetic
        assert dict(original.auxiliary_files) == dict(twice.auxiliary_files)
        assert original.had_test_actions == twice.had_test_actions

    def test_frozen_dataclass_rejects_mutation(self):
        """``@dataclass(frozen=True, slots=True)`` is the contract; verify."""
        ctx = FlowGroupContext(
            flowgroup=_make_flowgroup(),
            source_yaml=None,
        )
        with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
            ctx.synthetic = True  # type: ignore[misc]

    def test_dataclasses_replace_creates_new_context(self):
        """``dataclasses.replace`` is how the processor sets ``had_test_actions``."""
        import dataclasses

        ctx_in = FlowGroupContext(
            flowgroup=_make_flowgroup(),
            source_yaml=None,
        )
        ctx_out = dataclasses.replace(ctx_in, had_test_actions=True)
        assert ctx_in.had_test_actions is False
        assert ctx_out.had_test_actions is True
        # Same FlowGroup identity carried through.
        assert ctx_out.flowgroup is ctx_in.flowgroup
