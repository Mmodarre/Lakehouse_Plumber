"""Frozen view DTOs — projections of internal state for CLI/external rendering.

Distinct from response DTOs: views describe inspection results
(``ValidationIssueView``, ``FlowgroupView``, ...) rather than the
outcome of a runtime operation.

:stability: provisional
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping, Optional, Tuple

from lhp.api.responses import JSONValue


@dataclass(frozen=True)
class ValidationIssueView:
    """A single validation diagnostic — error or warning.

    Frozen, immutable projection of an internal validation outcome.
    Replaces the legacy ``ValidationIssue`` dataclass which carried a
    live :class:`LHPError` instance. The exception is decomposed here
    into flat, JSON-serialisable fields (``code``, ``category``,
    ``suggestions``, ``context``, ``doc_link``) per constitution §4.8.

    Sourced from one of two paths:

    1. **Structured** — built from an :class:`LHPError` raised by a
       validation worker. ``code`` carries the LHP error code (e.g.
       ``"LHP-VAL-021"``), ``category`` mirrors :class:`ErrorCategory`,
       and ``suggestions`` / ``context`` / ``doc_link`` preserve the
       rich payload for CLI rendering.
    2. **Unstructured** — built from a raw error or warning string
       (legacy CDC fan-in errors, discovery errors, deprecation
       warnings). ``code`` is the empty string, ``category`` is
       ``"VAL"``, and the rich fields are empty / ``None``.

    :stability: provisional
    """

    code: str
    category: str
    severity: Literal["error", "warning"]
    title: str
    details: Optional[str] = None
    pipeline_name: Optional[str] = None
    flowgroup_name: Optional[str] = None
    suggestions: Tuple[str, ...] = ()
    context: Mapping[str, JSONValue] = field(default_factory=dict)
    doc_link: Optional[str] = None


@dataclass(frozen=True)
class FlowgroupView:
    """A single flowgroup discovered from a project.

    Projection of the internal :class:`FlowGroup` Pydantic model onto
    a public, frozen view. Action lists are summarised by type rather
    than enumerated — callers needing full action detail must use
    :meth:`InspectionFacade.process_flowgroup` to obtain a
    :class:`ProcessedFlowgroupView`.

    :stability: provisional
    """

    name: str
    pipeline: str
    file_path: Optional[Path]
    presets: Tuple[str, ...] = ()
    template: Optional[str] = None
    load_action_count: int = 0
    transform_action_count: int = 0
    write_action_count: int = 0
    test_action_count: int = 0
    job_name: Optional[str] = None


@dataclass(frozen=True)
class ActionView:
    """A single action inside a processed flowgroup.

    Frozen surface projection of the internal :class:`Action` Pydantic
    model. Only fields useful for inspection-style rendering are kept;
    rich per-type subfields (CDC details, expectations payload, etc.)
    are intentionally omitted.

    :stability: provisional
    """

    name: str
    action_type: str
    target: Optional[str] = None
    description: Optional[str] = None
    transform_type: Optional[str] = None
    test_type: Optional[str] = None


@dataclass(frozen=True)
class ProcessedFlowgroupView:
    """A flowgroup after template expansion, preset merging, and substitutions.

    Captures the post-processing shape — the same information an
    internal :class:`FlowGroup` instance carries after the resolution
    service has run, but exposed as a frozen view (no Pydantic models
    reach the public API, §9.12).

    :stability: provisional
    """

    flowgroup: FlowgroupView
    actions: Tuple[ActionView, ...]
    job_name: Optional[str] = None
    variables: Mapping[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True)
class GeneratedCodeView:
    """Python source generated for a single flowgroup.

    Carries the in-memory generated source plus the target filename
    the writer would use. Filesystem writes are not performed by the
    inspection facade — callers do that themselves.

    :stability: provisional
    """

    flowgroup_name: str
    pipeline: str
    generated_code: str
    target_filename: str


@dataclass(frozen=True)
class ProjectConfigView:
    """Project-level configuration projection.

    Translation of the internal :class:`ProjectConfig` Pydantic model
    into a frozen view. Nested config sub-models (operational metadata,
    event log, monitoring, test reporting) collapse to a boolean
    ``has_*`` flag rather than re-export the Pydantic nested shape;
    callers needing the underlying detail must reach into internal
    modules (forbidden for public consumers).

    :stability: provisional
    """

    name: str
    version: str
    description: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    required_lhp_version: Optional[str] = None
    include: Tuple[str, ...] = ()
    blueprint_include: Tuple[str, ...] = ()
    instance_include: Tuple[str, ...] = ()
    has_operational_metadata: bool = False
    has_event_log: bool = False
    has_monitoring: bool = False
    has_test_reporting: bool = False


@dataclass(frozen=True)
class BlueprintInstanceView:
    """A single blueprint instance and the flowgroups it would produce.

    Frozen projection used by the verbose-mode listing in
    :meth:`InspectionFacade.list_blueprints`. ``flowgroup_count`` is
    the number of flowgroup contexts the expander materialises from the
    instance; ``pipelines`` is the sorted tuple of unique
    ``pipeline`` fields across those contexts.

    :stability: provisional
    """

    instance_file_path: Path
    flowgroup_count: int = 0
    pipelines: Tuple[str, ...] = ()


@dataclass(frozen=True)
class BlueprintView:
    """A single blueprint discovered in the project.

    Frozen projection of an internal :class:`Blueprint` Pydantic model
    plus its source filesystem path. Parameter and flowgroup-spec
    counts are summarised; the public API does not expose the nested
    blueprint shape.

    ``instances`` lists each instance file that targets this blueprint
    together with its resolved flowgroup count and pipeline names —
    populated when callers request the verbose listing (see
    :meth:`InspectionFacade.list_blueprints` and its ``include_instances``
    parameter).

    :stability: provisional
    """

    name: str
    file_path: Path
    version: str
    description: Optional[str] = None
    parameter_count: int = 0
    flowgroup_count: int = 0
    instance_count: int = 0
    instances: Tuple[BlueprintInstanceView, ...] = ()


@dataclass(frozen=True)
class PresetView:
    """A single preset discovered in the project.

    Frozen projection of the internal :class:`Preset` Pydantic model
    plus its source filesystem path.

    :stability: provisional
    """

    name: str
    file_path: Path
    version: str
    extends: Optional[str] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class TemplateParameterView:
    """A single declared parameter on a template.

    Frozen projection of a template parameter mapping. Mirrors the
    keys the legacy CLI presenter rendered: ``name``, ``type_``
    (defaulting to ``"string"``), ``required`` (defaulting to
    ``False``), ``description``, and an optional ``default``.

    :stability: provisional
    """

    name: str
    type_: str = "string"
    required: bool = False
    description: Optional[str] = None
    default: Optional[JSONValue] = None


@dataclass(frozen=True)
class TemplateView:
    """A single template discovered in the project.

    Frozen projection of the internal template model plus its source
    filesystem path. Parameter counts are summarised as
    ``required_parameter_count`` / ``parameter_count``; the action body
    count is exposed as a single integer.

    ``parameters`` carries the per-parameter view for callers that
    need to render parameter detail (name / type / required /
    description / default). Empty by default — populated when
    callers request it.

    :stability: provisional
    """

    name: str
    file_path: Path
    version: str
    description: Optional[str] = None
    parameter_count: int = 0
    required_parameter_count: int = 0
    action_count: int = 0
    parameters: Tuple[TemplateParameterView, ...] = ()


@dataclass(frozen=True)
class PipelineStats:
    """Per-pipeline statistics row used inside :class:`StatsResult`.

    :stability: provisional
    """

    pipeline_name: str
    flowgroup_count: int
    total_actions: int


@dataclass(frozen=True)
class SecretReferenceView:
    """A single ``${secret:scope/key}`` reference resolved by substitution.

    Frozen projection of the internal :class:`SecretReference` value
    object produced by :class:`EnhancedSubstitutionManager`. Carries
    only the resolved ``scope`` (after any ``secret_scopes`` alias
    lookup) and the secret ``key`` — both are JSON-safe strings.

    :stability: provisional
    """

    scope: str
    key: str


@dataclass(frozen=True)
class SubstitutionView:
    """Read-only view of the resolved substitution context for an environment.

    Frozen projection of the internal
    :class:`EnhancedSubstitutionManager` state after token expansion
    and YAML loading. Captures the four pieces of information CLI and
    external consumers need to inspect substitutions for a given
    environment:

    - ``env`` — the environment name the manager was constructed for.
    - ``tokens`` — fully-expanded ``${token}`` mappings as JSON-safe
      key/value pairs. Internal manager values that are nested
      ``dict`` / ``list`` objects (used by prefix/suffix rules) are
      flattened to strings via ``str(...)`` before exposure, since
      DTO fields must be flat per §4.8.
    - ``raw_mappings`` — the same expanded mappings as ``tokens`` but
      with their nested structure preserved: ``dict`` / ``list``
      values (of scalars) are kept as-is rather than flattened to a
      ``str`` repr. Consumers that need the structured mapping (e.g.
      bucketing values by ``isinstance(value, dict)``) read this
      field; ``tokens`` remains the flat string projection.
    - ``secret_references`` — every ``${secret:scope/key}`` reference
      the manager has observed, as a sorted, deduplicated tuple.
    - ``default_secret_scope`` — the configured fallback scope used
      when a ``${secret:key}`` reference omits the scope segment;
      ``None`` if no default is configured.

    The view contains no ``mappings`` for nested rule objects and no
    ``prefix_suffix_rules`` — those are internal implementation
    details and not part of the public substitution surface.

    :stability: provisional
    """

    env: str
    tokens: Mapping[str, str]
    raw_mappings: Mapping[str, JSONValue] = field(default_factory=dict)
    secret_references: Tuple[SecretReferenceView, ...] = ()
    default_secret_scope: Optional[str] = None
