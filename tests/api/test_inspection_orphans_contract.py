"""DTO contract tests for the inspection-facade *orphan* result types.

Per constitution §8.3 + §9.15. Dropping the ``show`` / ``info`` / ``stats`` CLI
commands in the rebuild left three :class:`InspectionFacade` methods —
``compute_stats``, ``get_project_config``, ``process_flowgroup`` — with no
in-tree CLI caller. They remain PUBLIC API (re-exported from :mod:`lhp.api`) and
so must stay under contract test: external consumers (a future WebUI / VSCode
extension) capture these results across process and wire boundaries.

The §8.3 quartet is asserted for each return DTO —
:class:`~lhp.api.StatsResult` (``compute_stats``),
:class:`~lhp.api.ProjectConfigView` (``get_project_config``), and
:class:`~lhp.api.ProcessedFlowgroupView` (``process_flowgroup``):

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises :class:`dataclasses.FrozenInstanceError`.
3. Pickle round-trip with ``==`` equality.
4. JSON-style round-trip via flat field values, then ``==`` after rebuild.
5. Field-type contract — no bare ``Any``, no ``Dict`` / ``List``, no
   ``Exception`` / ``LHPError``, no Pydantic ``BaseModel``.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from typing import get_type_hints

import pytest

from lhp.api import (
    ActionView,
    FlowgroupView,
    PipelineStats,
    ProcessedFlowgroupView,
    ProjectConfigView,
    StatsResult,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------- #
# Fixtures — populated instances of each orphan DTO.
# --------------------------------------------------------------------------- #
@pytest.fixture
def populated_stats_result() -> StatsResult:
    return StatsResult(
        pipeline_count=2,
        flowgroup_count=3,
        total_actions=7,
        action_counts_by_type={"load": 3, "transform": 2, "write": 2},
        pipeline_breakdown=(
            PipelineStats(pipeline_name="bronze", flowgroup_count=2, total_actions=5),
            PipelineStats(pipeline_name="silver", flowgroup_count=1, total_actions=2),
        ),
        templates_used=("standard_ingest",),
        presets_used=("bronze_layer", "default"),
    )


@pytest.fixture
def populated_project_config_view() -> ProjectConfigView:
    return ProjectConfigView(
        name="acme_edw",
        version="1.0",
        description="A test project",
        author="Test Author",
        created_date="2025-01-01",
        required_lhp_version=">=0.5.0",
        include=("01_raw/**", "02_silver/**"),
        blueprint_include=("blueprints/**",),
        instance_include=("instances/**",),
        has_operational_metadata=True,
        has_event_log=False,
        has_monitoring=True,
        has_test_reporting=False,
    )


@pytest.fixture
def populated_processed_flowgroup_view() -> ProcessedFlowgroupView:
    return ProcessedFlowgroupView(
        flowgroup=FlowgroupView(
            name="fg_customers",
            pipeline="bronze",
            file_path=None,
            presets=("bronze_layer",),
            template="standard_ingest",
            load_action_count=1,
            transform_action_count=1,
            write_action_count=1,
            test_action_count=0,
            job_name="ingest_job",
        ),
        actions=(
            ActionView(
                name="load_customers",
                action_type="load",
                target="v_customers_raw",
                description="Load raw customers",
            ),
            ActionView(
                name="clean_customers",
                action_type="transform",
                target="v_customers_clean",
                transform_type="sql",
            ),
            ActionView(
                name="write_customers",
                action_type="write",
                target="bronze.customers",
            ),
        ),
        job_name="ingest_job",
        variables={"region": "us", "retries": 3},
    )


# --------------------------------------------------------------------------- #
# JSON-shape projection helpers (mirror lhp.api serialization conventions:
# Mapping -> dict, Tuple[DTO, ...] -> list of dicts).
# --------------------------------------------------------------------------- #
def _stats_to_json_safe_dict(view: StatsResult) -> dict[str, object]:
    return {
        "pipeline_count": view.pipeline_count,
        "flowgroup_count": view.flowgroup_count,
        "total_actions": view.total_actions,
        "action_counts_by_type": dict(view.action_counts_by_type),
        "pipeline_breakdown": [dataclasses.asdict(p) for p in view.pipeline_breakdown],
        "templates_used": list(view.templates_used),
        "presets_used": list(view.presets_used),
    }


def _stats_from_json_safe_dict(payload: dict[str, object]) -> StatsResult:
    breakdown = payload["pipeline_breakdown"]
    assert isinstance(breakdown, list)
    return StatsResult(
        pipeline_count=payload["pipeline_count"],  # type: ignore[arg-type]
        flowgroup_count=payload["flowgroup_count"],  # type: ignore[arg-type]
        total_actions=payload["total_actions"],  # type: ignore[arg-type]
        action_counts_by_type=dict(payload["action_counts_by_type"]),  # type: ignore[arg-type]
        pipeline_breakdown=tuple(PipelineStats(**row) for row in breakdown),
        templates_used=tuple(payload["templates_used"]),  # type: ignore[arg-type]
        presets_used=tuple(payload["presets_used"]),  # type: ignore[arg-type]
    )


def _project_config_to_json_safe_dict(view: ProjectConfigView) -> dict[str, object]:
    return {
        "name": view.name,
        "version": view.version,
        "description": view.description,
        "author": view.author,
        "created_date": view.created_date,
        "required_lhp_version": view.required_lhp_version,
        "include": list(view.include),
        "blueprint_include": list(view.blueprint_include),
        "instance_include": list(view.instance_include),
        "has_operational_metadata": view.has_operational_metadata,
        "has_event_log": view.has_event_log,
        "has_monitoring": view.has_monitoring,
        "has_test_reporting": view.has_test_reporting,
    }


def _project_config_from_json_safe_dict(
    payload: dict[str, object],
) -> ProjectConfigView:
    return ProjectConfigView(
        name=payload["name"],  # type: ignore[arg-type]
        version=payload["version"],  # type: ignore[arg-type]
        description=payload["description"],  # type: ignore[arg-type]
        author=payload["author"],  # type: ignore[arg-type]
        created_date=payload["created_date"],  # type: ignore[arg-type]
        required_lhp_version=payload["required_lhp_version"],  # type: ignore[arg-type]
        include=tuple(payload["include"]),  # type: ignore[arg-type]
        blueprint_include=tuple(payload["blueprint_include"]),  # type: ignore[arg-type]
        instance_include=tuple(payload["instance_include"]),  # type: ignore[arg-type]
        has_operational_metadata=payload["has_operational_metadata"],  # type: ignore[arg-type]
        has_event_log=payload["has_event_log"],  # type: ignore[arg-type]
        has_monitoring=payload["has_monitoring"],  # type: ignore[arg-type]
        has_test_reporting=payload["has_test_reporting"],  # type: ignore[arg-type]
    )


def _action_to_json_safe_dict(view: ActionView) -> dict[str, object]:
    return {
        "name": view.name,
        "action_type": view.action_type,
        "target": view.target,
        "description": view.description,
        "transform_type": view.transform_type,
        "test_type": view.test_type,
    }


def _flowgroup_to_json_safe_dict(view: FlowgroupView) -> dict[str, object]:
    return {
        "name": view.name,
        "pipeline": view.pipeline,
        # file_path is Optional[Path]; None here, str(path) when present.
        "file_path": None if view.file_path is None else str(view.file_path),
        "presets": list(view.presets),
        "template": view.template,
        "load_action_count": view.load_action_count,
        "transform_action_count": view.transform_action_count,
        "write_action_count": view.write_action_count,
        "test_action_count": view.test_action_count,
        "job_name": view.job_name,
    }


def _processed_to_json_safe_dict(view: ProcessedFlowgroupView) -> dict[str, object]:
    return {
        "flowgroup": _flowgroup_to_json_safe_dict(view.flowgroup),
        "actions": [_action_to_json_safe_dict(a) for a in view.actions],
        "job_name": view.job_name,
        "variables": dict(view.variables),
    }


def _processed_from_json_safe_dict(
    payload: dict[str, object],
) -> ProcessedFlowgroupView:
    fg = payload["flowgroup"]
    assert isinstance(fg, dict)
    actions = payload["actions"]
    assert isinstance(actions, list)
    return ProcessedFlowgroupView(
        flowgroup=FlowgroupView(
            name=fg["name"],
            pipeline=fg["pipeline"],
            file_path=fg["file_path"],
            presets=tuple(fg["presets"]),
            template=fg["template"],
            load_action_count=fg["load_action_count"],
            transform_action_count=fg["transform_action_count"],
            write_action_count=fg["write_action_count"],
            test_action_count=fg["test_action_count"],
            job_name=fg["job_name"],
        ),
        actions=tuple(ActionView(**a) for a in actions),
        job_name=payload["job_name"],  # type: ignore[arg-type]
        variables=dict(payload["variables"]),  # type: ignore[arg-type]
    )


# --------------------------------------------------------------------------- #
# 1. Frozen dataclass.
# --------------------------------------------------------------------------- #
class TestFrozenContract:
    @pytest.mark.parametrize(
        "cls", [StatsResult, ProjectConfigView, ProcessedFlowgroupView, PipelineStats]
    )
    def test_is_frozen_dataclass(self, cls: type) -> None:
        params = getattr(cls, "__dataclass_params__", None)
        assert params is not None, f"{cls.__name__} is not a dataclass"
        assert params.frozen is True, f"{cls.__name__} must be frozen"


# --------------------------------------------------------------------------- #
# 2. Mutation raises FrozenInstanceError.
# --------------------------------------------------------------------------- #
class TestFrozenMutationRaises:
    def test_stats_result_mutation_raises(
        self, populated_stats_result: StatsResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_stats_result.pipeline_count = 99  # type: ignore[misc]

    def test_project_config_view_mutation_raises(
        self, populated_project_config_view: ProjectConfigView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_project_config_view.name = "tampered"  # type: ignore[misc]

    def test_processed_flowgroup_view_mutation_raises(
        self, populated_processed_flowgroup_view: ProcessedFlowgroupView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_processed_flowgroup_view.job_name = "tampered"  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# 3. Pickle round-trip.
# --------------------------------------------------------------------------- #
class TestPickleRoundTrip:
    def test_stats_result_pickles(self, populated_stats_result: StatsResult) -> None:
        restored = pickle.loads(pickle.dumps(populated_stats_result))
        assert restored == populated_stats_result

    def test_project_config_view_pickles(
        self, populated_project_config_view: ProjectConfigView
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_project_config_view))
        assert restored == populated_project_config_view

    def test_processed_flowgroup_view_pickles(
        self, populated_processed_flowgroup_view: ProcessedFlowgroupView
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_processed_flowgroup_view))
        assert restored == populated_processed_flowgroup_view


# --------------------------------------------------------------------------- #
# 4. JSON-style round-trip.
# --------------------------------------------------------------------------- #
class TestJSONRoundTrip:
    def test_stats_result_json_round_trip(
        self, populated_stats_result: StatsResult
    ) -> None:
        wire = json.loads(json.dumps(_stats_to_json_safe_dict(populated_stats_result)))
        restored = _stats_from_json_safe_dict(wire)
        assert restored == populated_stats_result
        assert restored.pipeline_breakdown == populated_stats_result.pipeline_breakdown
        assert dict(restored.action_counts_by_type) == dict(
            populated_stats_result.action_counts_by_type
        )

    def test_project_config_view_json_round_trip(
        self, populated_project_config_view: ProjectConfigView
    ) -> None:
        wire = json.loads(
            json.dumps(_project_config_to_json_safe_dict(populated_project_config_view))
        )
        restored = _project_config_from_json_safe_dict(wire)
        assert restored == populated_project_config_view

    def test_processed_flowgroup_view_json_round_trip(
        self, populated_processed_flowgroup_view: ProcessedFlowgroupView
    ) -> None:
        wire = json.loads(
            json.dumps(_processed_to_json_safe_dict(populated_processed_flowgroup_view))
        )
        restored = _processed_from_json_safe_dict(wire)
        assert restored == populated_processed_flowgroup_view
        assert restored.actions == populated_processed_flowgroup_view.actions
        assert restored.flowgroup == populated_processed_flowgroup_view.flowgroup


# --------------------------------------------------------------------------- #
# 5. Field-type contract.
# --------------------------------------------------------------------------- #
_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "DTOs must not carry live Exception instances (§4.8).",
    "LHPError": "DTOs must not carry LHPError instances (§4.8).",
    "BaseModel": "Public DTOs must not embed Pydantic models (§9.12).",
}

_ORPHAN_DTOS = (StatsResult, ProjectConfigView, ProcessedFlowgroupView, PipelineStats)


def _annotation_strings(cls: type) -> dict[str, str]:
    return {
        f.name: f.type if isinstance(f.type, str) else str(f.type)
        for f in dataclasses.fields(cls)
    }


# ``StatsResult.pipeline_breakdown`` is ``Tuple["PipelineStats", ...]`` — a
# string forward ref to a class defined in ``lhp.api.views`` (not the
# ``responses`` module where ``StatsResult`` lives), so a bare
# ``get_type_hints`` cannot resolve it. Mirror the established pattern in
# ``test_responses_contract.py``: pass the cross-module names via ``localns``.
_TYPE_HINT_NAMESPACE = {
    "PipelineStats": PipelineStats,
    "FlowgroupView": FlowgroupView,
    "ActionView": ActionView,
}


def _resolved_hints(cls: type) -> dict[str, object]:
    return get_type_hints(cls, localns=_TYPE_HINT_NAMESPACE)


class TestFieldTypeContract:
    @pytest.mark.parametrize("cls", _ORPHAN_DTOS)
    def test_no_banned_annotations(self, cls: type) -> None:
        for name, annotation in _annotation_strings(cls).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"{cls.__name__}.{name}: annotation {annotation!r} contains "
                    f"banned token {needle!r}. {reason}"
                )

    @pytest.mark.parametrize("cls", _ORPHAN_DTOS)
    def test_no_bare_any(self, cls: type) -> None:
        for name, annotation in _annotation_strings(cls).items():
            assert "Any" not in annotation, (
                f"{cls.__name__}.{name}: annotation {annotation!r} contains 'Any'. "
                f"Use precise types per §4.8."
            )

    @pytest.mark.parametrize("cls", _ORPHAN_DTOS)
    def test_resolved_hints_carry_no_exception_or_lhperror(self, cls: type) -> None:
        for name, hint in _resolved_hints(cls).items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"{cls.__name__}.{name}: resolved type {text!r} carries an Exception."
            )
            assert "LHPError" not in text, (
                f"{cls.__name__}.{name}: resolved type {text!r} carries an LHPError."
            )


# --------------------------------------------------------------------------- #
# Public-export guard — these orphans must stay on the lhp.api surface.
# --------------------------------------------------------------------------- #
class TestPublicExport:
    @pytest.mark.parametrize(
        "name", ["StatsResult", "ProjectConfigView", "ProcessedFlowgroupView"]
    )
    def test_in_lhp_api_all(self, name: str) -> None:
        import lhp.api as api

        assert name in api.__all__, f"{name} dropped from lhp.api.__all__ (§9.15)"
