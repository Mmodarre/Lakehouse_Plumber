"""Frozen view DTOs — projections of internal state for CLI/external rendering.

Distinct from response DTOs: views describe inspection results
(``ValidationIssueView``, ``FlowgroupView``, ...) rather than the
outcome of a runtime operation.

:stability: provisional
"""

# JUSTIFIED: this file is the constitution-mandated single registry of
# public view DTOs (TARGET §11): every inspection-result projection lives
# here by design so the public contract stays auditable in one place, and
# TARGET §8 grants DTO files <=600 lines. Each dataclass is a flat frozen
# value object; splitting the registry would scatter the versioned surface
# without removing any complexity.

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
    file_path: Optional[Path] = None
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

    For *write* actions, three optional write-metadata fields are
    populated from the resolved ``write_target`` (all ``None`` for
    non-write actions and when not derivable):

    - ``write_mode`` — the streaming-table write mode
      (``"standard"`` / ``"cdc"`` / ``"snapshot_cdc"``); defaults to
      ``"standard"`` when a streaming-table target omits ``mode``.
      :stability: provisional
    - ``scd_type`` — the SCD type (``1`` / ``2``) read from the CDC or
      snapshot-CDC config; ``None`` outside CDC modes.
      :stability: provisional
    - ``target_full_name`` — the fully-qualified target name. For
      table targets this is ``catalog.schema.table`` (falling back to
      ``database.table`` or the bare ``table``); for sink targets it is
      ``sink:<sink_type>/<id>``. Any unresolved substitution tokens are
      passed through verbatim — the converter never resolves them.
      :stability: provisional

    :stability: provisional
    """

    name: str
    action_type: str
    target: Optional[str] = None
    description: Optional[str] = None
    transform_type: Optional[str] = None
    test_type: Optional[str] = None
    write_mode: Optional[str] = None
    scd_type: Optional[int] = None
    target_full_name: Optional[str] = None


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
class OperationalMetadataColumnView:
    """A single operational-metadata column available to the project.

    Frozen projection of one ``MetadataColumnConfig`` entry resolved with
    the generator's ``_get_available_columns`` REPLACE semantics: when the
    project declares ``operational_metadata.columns`` those columns replace
    the five built-ins wholesale (``source == "project"``); otherwise the
    built-ins are exposed (``source == "builtin"``). ``expression`` is the
    raw PySpark expression as declared — substitution tokens (e.g.
    ``${pipeline_name}``) are passed through verbatim, never resolved.

    :stability: provisional
    """

    name: str
    expression: str
    description: Optional[str]
    applies_to: Tuple[str, ...]
    source: Literal["builtin", "project"]


@dataclass(frozen=True)
class OperationalMetadataPresetView:
    """A named operational-metadata column preset declared in ``lhp.yaml``.

    Frozen projection of one ``MetadataPresetConfig`` entry under
    ``operational_metadata.presets``. ``columns`` lists the column names the
    preset selects, in declaration order.

    :stability: provisional
    """

    name: str
    columns: Tuple[str, ...]
    description: Optional[str]


@dataclass(frozen=True)
class OperationalMetadataView:
    """The operational-metadata columns and presets available to a project.

    Frozen projection returned by
    :meth:`InspectionFacade.get_operational_metadata`. ``columns`` are
    resolved with the generator's REPLACE semantics (see
    :class:`OperationalMetadataColumnView`), so the web IDE offers exactly
    the columns generation would apply. ``presets`` is empty unless the
    project declares ``operational_metadata.presets``.

    :stability: provisional
    """

    columns: Tuple[OperationalMetadataColumnView, ...]
    presets: Tuple[OperationalMetadataPresetView, ...]


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
class PresetResolutionResult:
    """Resolved inheritance chain and merged configuration for one preset.

    Returned by :meth:`InspectionFacade.resolve_preset`. ``chain`` holds
    preset *names* ordered base→leaf (the requested preset is last).
    Plain names are exposed rather than nested :class:`PresetView`
    entries: consumers need the chain labels only, and per-preset
    metadata (file path, version, description) is already available
    through :meth:`InspectionFacade.list_presets`.

    ``merged_config`` is the deep merge of every ``defaults`` payload
    along the chain, applied base→leaf:

    - per key, the more-derived (later) preset's value wins;
    - nested mappings merge recursively rather than replace;
    - ``operational_metadata`` *lists* are concatenated with
      order-preserving dedup instead of replaced.

    Values are parsed-YAML shapes (scalars / lists / mappings), exposed
    un-coerced as ``Mapping[str, JSONValue]`` — the same convention as
    :attr:`SubstitutionView.raw_mappings`.

    :stability: provisional
    """

    name: str
    chain: Tuple[str, ...]
    merged_config: Mapping[str, JSONValue] = field(default_factory=dict)


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


@dataclass(frozen=True)
class WheelModuleView:
    """A single module entry inside a built wheel.

    Frozen projection of one archive member observed while inspecting a
    Python wheel (``.whl``). ``arcname`` is the member's path inside the
    archive; ``size_bytes`` is its uncompressed size. Both fields are
    flat and JSON-safe (§4.8).

    :stability: provisional
    """

    arcname: str
    size_bytes: int


@dataclass(frozen=True)
class WheelContentsView:
    """Read-only view of the modules packaged inside a built wheel.

    Frozen projection of the result of inspecting a Python wheel
    (``.whl``) for a generated pipeline. Captures the source wheel path,
    the optional ``pipeline`` / ``env`` context the wheel was built for,
    and the enumerated module entries. ``modules`` is a frozen tuple of
    :class:`WheelModuleView`; ``module_count`` mirrors its length for
    callers that only need the count. No live archive handle reaches the
    public API — only flat, JSON-shape-compatible fields (§4.8).

    :stability: provisional
    """

    wheel_path: Path
    pipeline: Optional[str]
    env: Optional[str]
    modules: Tuple[WheelModuleView, ...]
    module_count: int


@dataclass(frozen=True)
class LineageNodeView:
    """A single node in a dataset's lineage graph.

    A flat projection of one hop in the upstream chain of a produced
    dataset. ``kind`` is one of ``load`` / ``transform`` / ``write`` /
    ``test`` (a resolved action node), ``dataset`` (an upstream table
    produced by a *different* flowgroup, reached over a cross-flowgroup
    write edge), or ``external`` (an unmatched source read — an external
    table / path the project does not itself produce).

    ``id`` is the action id ``{pipeline}.{flowgroup}.{action}`` for action
    nodes, ``dataset:<fqn>`` for upstream dataset nodes, and
    ``external:<source>`` for external nodes. ``label`` is a human-facing
    name (the view ``target`` for load/transform hops, the resolved FQN for
    the write hop and dataset nodes, the raw source string for external
    nodes). ``pipeline`` / ``flowgroup`` are populated for action and
    dataset nodes and empty for external nodes; ``dataset_fqn`` carries the
    resolved fully-qualified name on write and dataset nodes and is empty
    elsewhere.

    :stability: provisional
    """

    id: str
    kind: Literal["load", "transform", "write", "test", "dataset", "external"]
    label: str
    pipeline: str = ""
    flowgroup: str = ""
    dataset_fqn: str = ""


@dataclass(frozen=True)
class LineageEdgeView:
    """A directed producer→consumer edge between two lineage nodes.

    ``source`` and ``target`` are :class:`LineageNodeView` ``id`` values;
    the edge points from the upstream node to the node that reads it.

    :stability: provisional
    """

    source: str
    target: str


@dataclass(frozen=True)
class DatasetConsumerView:
    """A downstream action that consumes a produced dataset.

    One entry per foreign-flowgroup action that reads the dataset over a
    cross-flowgroup edge. ``pipeline`` / ``flowgroup`` / ``action_name``
    identify the consuming action; ``dataset_fqn`` is the resolved
    fully-qualified name of what the *consuming* flowgroup itself produces
    (its first resolved write target, or ``""`` when it produces none),
    letting callers pivot from a table to the tables built from it.

    :stability: provisional
    """

    dataset_fqn: str
    pipeline: str
    flowgroup: str
    action_name: str


@dataclass(frozen=True)
class DatasetView:
    """A produced dataset (table or sink) with its lineage and consumers.

    One :class:`DatasetView` per *write* action across the project,
    env-resolved. ``fqn`` is the resolved ``catalog.schema.table`` for a
    table (``kind == "table"``) or ``sink:<sink_type>/<id>`` for a sink
    (``kind == "sink"``); any unresolved substitution tokens pass through
    verbatim. ``pipeline`` / ``flowgroup`` / ``action_name`` / ``write_mode``
    / ``scd_type`` / ``source_file`` describe the producing write action.

    v1 limitation: a delta sink that writes a real table via
    ``options.tableName`` is still indexed under its ``sink:<sink_type>/<id>``
    id, never the underlying table FQN — a lineage lookup by that table's FQN
    misses it.

    ``nodes`` / ``edges`` embed the same-flowgroup upstream lineage chain
    (external source → load → view → transform → view → write) plus any
    cross-flowgroup upstream ``dataset`` nodes; session views (``v_*``)
    appear only as intermediate chain nodes, never as their own top-level
    :class:`DatasetView`. ``consumers`` lists the downstream actions that
    read this dataset from another flowgroup.

    :stability: provisional
    """

    fqn: str
    kind: Literal["table", "sink"]
    pipeline: str
    flowgroup: str
    action_name: str
    write_mode: str
    scd_type: Optional[int]
    source_file: str
    nodes: Tuple[LineageNodeView, ...] = ()
    edges: Tuple[LineageEdgeView, ...] = ()
    consumers: Tuple[DatasetConsumerView, ...] = ()
