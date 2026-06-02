"""Round-trip contract tests for :func:`lhp.api.to_dict`.

Every public DTO and concrete event in ``lhp.api`` must survive::

    instance -> to_dict -> json.dumps -> json.loads -> reconstruct -> ==

Constitution §1.8 mandates the contract; §1.9 requires a contract test
on each public DTO; §9.22 forbids per-type dispatch — so reconstruction
walks ``dataclasses.fields`` + resolved type hints, never per-type.

``ErrorEmitted`` is excluded: its ``lhp_error`` field is a live
:class:`Exception` (the §9.21 exception to the "no exceptions in DTO
fields" rule); ``to_dict`` correctly raises :class:`TypeError` on it.
"""

from __future__ import annotations

import datetime
import json
import typing
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

import pytest

from lhp.api import (
    ActionView,
    BatchGenerationResponse,
    BatchValidationResponse,
    BlueprintInstanceView,
    BlueprintView,
    BundleEnableResult,
    BundleSyncCompleted,
    BundleSyncResult,
    BundleValidationResult,
    DependencyAnalysisResult,
    DependencyOutputEntry,
    DependencyOutputsResult,
    FinalizeMonitoringResult,
    FlowgroupView,
    GeneratedCodeView,
    GenerationCompleted,
    GenerationResponse,
    InitProjectResult,
    OperationCompleted,
    OperationStarted,
    PipelineStats,
    PresetView,
    ProcessedFlowgroupView,
    ProjectConfigView,
    SecretReferenceView,
    StatsResult,
    SubstitutionView,
    TemplateParameterView,
    TemplateView,
    ValidationCompleted,
    ValidationIssueView,
    ValidationResponse,
    to_dict,
)

# Localns for ``get_type_hints`` — ``responses.py`` uses string forward
# refs to ``views`` types to avoid a circular import.
_TYPE_NS: dict[str, Any] = {
    "ValidationIssueView": ValidationIssueView,
    "PipelineStats": PipelineStats,
    "GenerationResponse": GenerationResponse,
    "ValidationResponse": ValidationResponse,
    "DependencyOutputEntry": DependencyOutputEntry,
    "BatchGenerationResponse": BatchGenerationResponse,
    "BatchValidationResponse": BatchValidationResponse,
    "BundleSyncResult": BundleSyncResult,
}


def _coerce(value: Any, hint: Any) -> Any:
    """Coerce a JSON-loaded value back to the type the field declared.

    Walks the type hint structurally (no class-name dispatch — §9.22).
    Unknown shapes pass through unchanged.
    """
    if value is None:
        return None

    origin = get_origin(hint)
    args = get_args(hint)

    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _coerce(value, non_none[0])
        return value

    if hint is Path:
        return Path(value)

    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_coerce(v, args[0]) for v in value)
        return tuple(_coerce(v, a) for v, a in zip(value, args, strict=True))

    # ``isinstance(origin, type)`` guards against parameterised typing
    # constructs (Literal, Union, …) whose origin is not a real class —
    # ``issubclass`` would raise on them.
    if isinstance(origin, type) and issubclass(origin, typing.Mapping):
        val_hint = args[1] if len(args) >= 2 else Any
        return {k: _coerce(v, val_hint) for k, v in value.items()}

    if isinstance(hint, type) and is_dataclass(hint):
        return _reconstruct(hint, value)

    return value


def _reconstruct(cls: type, payload: dict[str, Any]) -> Any:
    """Rebuild a dataclass instance from its ``to_dict`` payload.

    Type-generic — no class names appear (constitution §9.22).
    """
    hints = get_type_hints(cls, localns=_TYPE_NS)
    kwargs = {f.name: _coerce(payload[f.name], hints[f.name]) for f in fields(cls)}
    return cls(**kwargs)


# Minimal-but-legal field sets so the test isolates the serializer,
# not the constructors.
_issue = ValidationIssueView(
    code="LHP-VAL-021",
    category="VAL",
    severity="error",
    title="Bad field",
    details="Missing required key",
    suggestions=("Add the key",),
    context={"path": "x.yaml", "line": 3},
    doc_link="https://example/lhp-val-021",
)
_flowgroup = FlowgroupView(
    name="fg1", pipeline="pipe1", file_path=Path("flowgroups/fg1.yaml"), presets=("p1",)
)
_action = ActionView(name="load1", action_type="load", target="t1")
_gen_resp = GenerationResponse(
    success=True,
    generated_filenames=("a.py", "b.py"),
    files_written=2,
    total_flowgroups=2,
    output_location=Path("generated/dev"),
    performance_info={"elapsed_ms": 12},
)
_val_resp = ValidationResponse(
    success=False, issues=(_issue,), validated_pipelines=("pipe1",)
)
_dep_entry = DependencyOutputEntry(format_name="dot", label="", path=Path("out.dot"))

_INSTANCES = [
    pytest.param(
        OperationStarted(operation_name="generate", env="dev"), id="OperationStarted"
    ),
    # ``response: object`` accepts any JSON shape; a primitive keeps the
    # round-trip generic (no per-type fixup).
    pytest.param(OperationCompleted(response="ok"), id="OperationCompleted"),
    pytest.param(
        GenerationCompleted(
            response=BatchGenerationResponse(
                success=True,
                pipeline_responses={"pipe1": _gen_resp},
                total_files_written=2,
                aggregate_generated_filenames=("a.py", "b.py"),
                output_location=Path("generated/dev"),
            )
        ),
        id="GenerationCompleted",
    ),
    pytest.param(
        ValidationCompleted(
            response=BatchValidationResponse(
                success=False,
                pipeline_responses={"pipe1": _val_resp},
                total_errors=1,
                total_warnings=0,
                validated_pipelines=("pipe1",),
            )
        ),
        id="ValidationCompleted",
    ),
    pytest.param(
        BundleSyncCompleted(
            response=BundleSyncResult(
                success=True,
                synced_file_count=3,
                deleted_file_count=1,
                bundle_path=Path("bundle.yml"),
            )
        ),
        id="BundleSyncCompleted",
    ),
    pytest.param(_gen_resp, id="GenerationResponse"),
    pytest.param(
        BatchGenerationResponse(
            success=True,
            pipeline_responses={"pipe1": _gen_resp},
            total_files_written=2,
            aggregate_generated_filenames=("a.py",),
            output_location=Path("generated/dev"),
        ),
        id="BatchGenerationResponse",
    ),
    pytest.param(_val_resp, id="ValidationResponse"),
    pytest.param(
        BatchValidationResponse(
            success=False,
            pipeline_responses={"pipe1": _val_resp},
            total_errors=1,
            total_warnings=0,
            validated_pipelines=("pipe1",),
        ),
        id="BatchValidationResponse",
    ),
    pytest.param(
        InitProjectResult(
            success=True,
            target_dir=Path("/tmp/proj"),
            created_files=(Path("/tmp/proj/lhp.yaml"),),
            created_dirs=(Path("/tmp/proj"),),
            bundle_enabled=False,
        ),
        id="InitProjectResult",
    ),
    pytest.param(
        StatsResult(
            pipeline_count=1,
            flowgroup_count=2,
            total_actions=4,
            action_counts_by_type={"load": 2, "write": 2},
            pipeline_breakdown=(
                PipelineStats(pipeline_name="p1", flowgroup_count=2, total_actions=4),
            ),
            templates_used=("t",),
            presets_used=("p",),
        ),
        id="StatsResult",
    ),
    pytest.param(
        DependencyAnalysisResult(
            pipeline_dependencies={"p1": ("p2",)},
            execution_stages=(("p2",), ("p1",)),
            circular_dependencies=(),
            external_sources=("src.tbl",),
            total_pipelines=2,
            total_external_sources=1,
        ),
        id="DependencyAnalysisResult",
    ),
    pytest.param(_dep_entry, id="DependencyOutputEntry"),
    pytest.param(
        DependencyOutputsResult(
            success=True, entries=(_dep_entry,), output_dir=Path("out")
        ),
        id="DependencyOutputsResult",
    ),
    pytest.param(
        FinalizeMonitoringResult(
            success=True,
            monitoring_pipeline_path=Path("mon.py"),
            event_log_table_created=True,
        ),
        id="FinalizeMonitoringResult",
    ),
    pytest.param(
        BundleSyncResult(
            success=True,
            synced_file_count=3,
            deleted_file_count=1,
            bundle_path=Path("bundle.yml"),
        ),
        id="BundleSyncResult",
    ),
    pytest.param(
        BundleValidationResult(
            success=False, issues=("missing root_path",), error_code="LHP-CFG-001"
        ),
        id="BundleValidationResult",
    ),
    pytest.param(
        BundleEnableResult(
            success=True,
            target_dir=Path("/tmp/proj"),
            created_files=(Path("/tmp/proj/databricks.yml"),),
            created_dirs=(),
        ),
        id="BundleEnableResult",
    ),
    pytest.param(_issue, id="ValidationIssueView"),
    pytest.param(_flowgroup, id="FlowgroupView"),
    pytest.param(_action, id="ActionView"),
    pytest.param(
        ProcessedFlowgroupView(
            flowgroup=_flowgroup,
            actions=(_action,),
            job_name="job1",
            variables={"k": "v"},
        ),
        id="ProcessedFlowgroupView",
    ),
    pytest.param(
        GeneratedCodeView(
            flowgroup_name="fg1",
            pipeline="pipe1",
            generated_code="x = 1",
            target_filename="fg1.py",
        ),
        id="GeneratedCodeView",
    ),
    pytest.param(
        ProjectConfigView(name="proj", version="1.0.0", include=("pipelines/**",)),
        id="ProjectConfigView",
    ),
    pytest.param(
        BlueprintInstanceView(
            instance_file_path=Path("i.yaml"), flowgroup_count=2, pipelines=("p1",)
        ),
        id="BlueprintInstanceView",
    ),
    pytest.param(
        BlueprintView(
            name="bp1",
            file_path=Path("bp.yaml"),
            version="1.0",
            instances=(
                BlueprintInstanceView(
                    instance_file_path=Path("i.yaml"),
                    flowgroup_count=1,
                    pipelines=("p1",),
                ),
            ),
        ),
        id="BlueprintView",
    ),
    pytest.param(
        PresetView(
            name="pre1", file_path=Path("pre.yaml"), version="1.0", extends="base"
        ),
        id="PresetView",
    ),
    pytest.param(
        TemplateParameterView(name="env", type_="string", required=True, default="dev"),
        id="TemplateParameterView",
    ),
    pytest.param(
        TemplateView(
            name="tpl1",
            file_path=Path("t.yaml"),
            version="1.0",
            parameter_count=1,
            parameters=(TemplateParameterView(name="env"),),
        ),
        id="TemplateView",
    ),
    pytest.param(
        PipelineStats(pipeline_name="p1", flowgroup_count=2, total_actions=4),
        id="PipelineStats",
    ),
    pytest.param(
        SecretReferenceView(scope="kv", key="db_url"), id="SecretReferenceView"
    ),
    pytest.param(
        SubstitutionView(
            env="dev",
            tokens={"catalog": "dev_catalog", "schema": "main"},
            raw_mappings={"catalog_map": {"prod": "main"}},
            secret_references=(
                SecretReferenceView(scope="kv", key="db_url"),
                SecretReferenceView(scope="kv", key="api_token"),
            ),
            default_secret_scope="kv",
        ),
        id="SubstitutionView",
    ),
]


@pytest.mark.parametrize("instance", _INSTANCES)
def test_dto_round_trip_via_to_dict(instance: Any) -> None:
    payload = to_dict(instance)
    # Dict form must be JSON-clean — no tuples, no Paths, no enums.
    rehydrated = json.loads(json.dumps(payload))
    assert rehydrated == payload
    reconstructed = _reconstruct(type(instance), rehydrated)
    assert reconstructed == instance


def test_to_dict_raises_typeerror_on_datetime() -> None:
    """Non-serialisable types fail loudly per §9.22 (no silent coercion)."""
    with pytest.raises(TypeError) as exc:
        to_dict(datetime.datetime.now())
    assert "datetime" in str(exc.value).lower()


def test_to_dict_raises_typeerror_on_error_emitted_live_exception() -> None:
    """``ErrorEmitted`` carries a live :class:`LHPError` (§9.21 exception
    to §4.8); it cannot JSON-round-trip. Producers must decompose into
    :class:`ValidationIssueView` before serialising.
    """
    from lhp.api import ErrorEmitted
    from lhp.errors.types import ErrorCategory, LHPError

    err = LHPError(
        category=ErrorCategory.VALIDATION,
        code_number="999",
        title="boom",
        details="d",
    )
    event = ErrorEmitted(lhp_error=err)
    with pytest.raises(TypeError) as exc:
        to_dict(event)
    assert "LHPError" in str(exc.value)
