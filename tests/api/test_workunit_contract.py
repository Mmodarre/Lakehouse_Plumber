"""DTO contract tests for PipelineWorkUnit (§8.3 contract test set).

Verifies pickle round-trip (spawn-boundary requirement) and JSON
round-trip via to_dict / from_dict (telemetry / event-bus requirement).
The frozen contract is verified by attempting to mutate a field and
expecting FrozenInstanceError.
"""

import json
import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from lhp.models.config import FlowGroupContext
from lhp.models.processing import PipelineWorkUnit


@pytest.fixture
def minimal_unit():
    """A unit with only the two required fields populated."""
    return PipelineWorkUnit(
        pipeline_name="bronze",
        flowgroups=(),
    )


@pytest.fixture
def full_unit():
    """A unit with every Optional populated (sub-mgr stays None — see notes)."""
    return PipelineWorkUnit(
        pipeline_name="silver",
        flowgroups=(),
        substitution_manager=None,  # Optional; None is valid
        output_dir=Path("/tmp/lhp_test"),
        discovery_error=None,
    )


@pytest.fixture
def discovery_error_unit():
    """A unit representing a discovery failure (D6 short-circuit path)."""
    return PipelineWorkUnit(
        pipeline_name="bronze",
        flowgroups=(),
        discovery_error="Pipeline validation failed: missing required field",
    )


@pytest.mark.unit
class TestPipelineWorkUnitContract:
    """Frozen / slots / pickle / JSON-projection contract for PipelineWorkUnit."""

    def test_pickle_round_trip_minimal(self, minimal_unit):
        """A minimal unit survives pickle / unpickle unchanged."""
        restored = pickle.loads(pickle.dumps(minimal_unit))
        assert restored == minimal_unit

    def test_pickle_round_trip_with_path(self, full_unit):
        """``output_dir: Path`` survives pickle as a Path (not stringified)."""
        restored = pickle.loads(pickle.dumps(full_unit))
        assert restored.pipeline_name == full_unit.pipeline_name
        assert restored.output_dir == full_unit.output_dir
        assert isinstance(restored.output_dir, Path)

    def test_pickle_round_trip_discovery_error(self, discovery_error_unit):
        """``discovery_error: str`` survives pickle unchanged."""
        restored = pickle.loads(pickle.dumps(discovery_error_unit))
        assert restored.discovery_error == (
            "Pipeline validation failed: missing required field"
        )

    def test_frozen_contract(self, minimal_unit):
        """Frozen dataclass: attribute assignment raises FrozenInstanceError."""
        with pytest.raises(FrozenInstanceError):
            minimal_unit.pipeline_name = "tampered"

    def test_slots_contract(self, minimal_unit):
        """slots=True means no per-instance __dict__."""
        assert not hasattr(minimal_unit, "__dict__")

    def test_to_dict_returns_json_safe(self, full_unit):
        """to_dict() output round-trips through json.dumps without error."""
        d = full_unit.to_dict()
        s = json.dumps(d)
        assert isinstance(s, str)
        loaded = json.loads(s)
        assert loaded["pipeline_name"] == "silver"

    def test_to_dict_path_serialization(self, full_unit):
        """output_dir is stringified by to_dict()."""
        d = full_unit.to_dict()
        assert isinstance(d.get("output_dir"), str)
        assert d["output_dir"] == str(full_unit.output_dir)

    def test_to_dict_none_passthrough(self, minimal_unit):
        """Optional fields with None values map to None / null in the dict form."""
        d = minimal_unit.to_dict()
        assert d.get("substitution_manager") is None
        assert d.get("output_dir") is None
        assert d.get("discovery_error") is None

    def test_to_dict_flowgroup_names_projection(self):
        """flowgroups: Tuple[FlowGroupContext, ...] projects to a name tuple."""
        from lhp.models.config import FlowGroup

        fg = FlowGroup(
            pipeline="bronze",
            flowgroup="customer_ingest",
            actions=[],
        )
        ctx = FlowGroupContext(flowgroup=fg, source_yaml=None)
        unit = PipelineWorkUnit(pipeline_name="bronze", flowgroups=(ctx,))
        d = unit.to_dict()
        assert d["flowgroup_names"] == ("customer_ingest",)
        # The projection is JSON-safe (tuple → list when round-tripped through json).
        loaded = json.loads(json.dumps(d))
        assert loaded["flowgroup_names"] == ["customer_ingest"]

    def test_from_dict_round_trip(self, full_unit):
        """to_dict → from_dict yields a unit with the JSON-safe subset preserved.

        ``substitution_manager`` cannot round-trip through JSON (it is a live
        collaborator with non-JSON state); ``flowgroups`` round-trips only the
        name projection. Production code does not rely on full from_dict
        replay — to_dict is for telemetry, not replay.
        """
        d = full_unit.to_dict()
        restored = PipelineWorkUnit.from_dict(d)
        assert restored.pipeline_name == full_unit.pipeline_name
        assert restored.output_dir == full_unit.output_dir
        assert restored.discovery_error == full_unit.discovery_error
        # substitution_manager and flowgroups default to None / () on rehydrate.
        assert restored.substitution_manager is None
        assert restored.flowgroups == ()

    def test_from_dict_discovery_error_round_trip(self, discovery_error_unit):
        """discovery_error survives the JSON round-trip."""
        d = discovery_error_unit.to_dict()
        restored = PipelineWorkUnit.from_dict(d)
        assert restored.discovery_error == discovery_error_unit.discovery_error

    def test_defaults_preserve_d1_caller_compatibility(self):
        """A caller that supplies only (pipeline_name, flowgroups) must still work.

        D1 callers constructed PipelineWorkUnit with the two original fields
        only. The three new D2 fields default to None so those call sites
        keep working unchanged.
        """
        u = PipelineWorkUnit(pipeline_name="x", flowgroups=())
        assert u.substitution_manager is None
        assert u.output_dir is None
        assert u.discovery_error is None
