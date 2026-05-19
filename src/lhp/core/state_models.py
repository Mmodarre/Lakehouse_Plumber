"""State management data models for LakehousePlumber."""

import traceback
from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional


@dataclass
class DependencyInfo:
    """Information about a dependency file."""

    path: str  # Relative path to dependency file
    checksum: str  # SHA256 checksum of dependency
    type: str  # 'preset', 'template', 'substitution', 'project_config'
    last_modified: str  # ISO timestamp of last modification
    mtime: Optional[float] = None  # Unix timestamp for fast comparison


@dataclass
class GlobalDependencies:
    """Dependencies that affect all files in scope."""

    substitution_file: Optional[DependencyInfo] = None  # Per environment
    project_config: Optional[DependencyInfo] = None  # Global across environments


@dataclass
class FileState:
    """Represents the state of a generated file."""

    source_yaml: str  # Path to the YAML file that generated this
    generated_path: str  # Path to the generated file
    checksum: str  # SHA256 checksum of the generated file
    source_yaml_checksum: str  # SHA256 checksum of the source YAML file
    timestamp: str  # When it was generated
    environment: str  # Environment name
    pipeline: str  # Pipeline name
    flowgroup: str  # FlowGroup name

    # File-specific dependencies (presets, templates, substitution references, etc.)
    file_dependencies: Optional[Dict[str, DependencyInfo]] = None
    artifact_type: Optional[str] = None
    # True when produced by blueprint expansion (source_yaml is a blueprint
    # path). Default False preserves pre-existing state entries on load and
    # tells the cleanup slow path to skip orphan-checks it can't run without
    # re-running expansion.
    synthetic: bool = False


@dataclass
class ProjectState:
    """Represents the complete state of a project."""

    version: str = "1.0"
    last_updated: str = ""
    environments: Dict[str, Dict[str, FileState]] = (
        None  # env -> file_path -> FileState
    )

    # Global dependencies per environment
    global_dependencies: Optional[Dict[str, GlobalDependencies]] = (
        None  # env -> GlobalDependencies
    )

    # Per-env record of the generation flags in effect at the last successful
    # save. Used to detect env-wide context changes (e.g. ``include_tests``
    # flip) without per-file composite checksums.
    # Shape: {env_name: {"include_tests": "True" | "False"}}
    last_generation_context: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self):
        if self.environments is None:
            self.environments = {}
        if self.global_dependencies is None:
            self.global_dependencies = {}


@dataclass
class PipelineStatePayload:
    """Wire-format payload for ``.lhp_state/<pipeline>.json``.

    Mirrors the on-disk shape for one pipeline shard. Multi-env entries are
    co-located within ``environments`` so a worker mutating env A's section can
    round-trip env B's entries unchanged (load-modify-write multi-env
    preservation).

    Non-frozen by design: workers mutate the loaded payload before saving.
    """

    pipeline: str = ""
    schema_version: str = "2"
    # env -> {rel_generated_path: FileState}
    environments: Dict[str, Dict[str, FileState]] = field(default_factory=dict)


@dataclass
class GlobalStatePayload:
    """Wire-format payload for ``.lhp_state/_global.json``.

    Holds project-wide fields not tied to a single pipeline shard:
    LHP version, last_updated timestamp, per-env global dependencies, and the
    per-env generation context (e.g. ``include_tests`` flag in effect at the
    last successful save).

    Non-frozen by design: ``save_global`` mutates ``last_updated`` before
    writing.
    """

    schema_version: str = "2"
    version: str = "1.0"
    last_updated: str = ""
    # env -> GlobalDependencies
    global_dependencies: Dict[str, GlobalDependencies] = field(default_factory=dict)
    # env -> {flag_name: stringified_value}
    last_generation_context: Dict[str, Dict[str, str]] = field(default_factory=dict)



@dataclass(frozen=True, slots=True)
class PipelineDelta:
    """Worker → main-thread report for ONE pipeline's generate run.

    Carries the small bit of state the main thread needs after a worker
    finishes a whole pipeline: a success flag, file-count rollups for the
    summary line, the ``{relative_path: formatted_code}`` map the facade
    surfaces as ``aggregate_generated_files``, and — on failure — the
    exception serialized as plain strings. No live ``BaseException`` is
    shipped across the process boundary; tracebacks come through
    pre-formatted via :func:`traceback.format_exception` so the main thread
    can re-raise a fresh :class:`LHPError` with full chained context.

    The shard itself does NOT travel through this delta — workers write
    their per-pipeline shard via :meth:`PipelineStateManager.save` before
    returning, so the state lives on disk and only the in-memory
    presentation-layer payload (generated_files) travels back.

    Why ``frozen=True, slots=True``:
      - Immutability across the spawn boundary: workers can't accidentally
        mutate a delta after it's been queued for the main thread.
      - Small per-instance overhead: ``slots=True`` drops the per-instance
        ``__dict__`` so the dataclass machinery itself stays tight even
        though ``generated_files`` (which is the bulk) is a plain dict.
    """

    pipeline_name: str
    success: bool
    files_written: int = 0
    files_skipped: int = 0
    artifacts_count: int = 0
    generated_files: Mapping[str, str] = field(default_factory=dict)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    @classmethod
    def success_(
        cls,
        pipeline_name: str,
        *,
        files_written: int = 0,
        files_skipped: int = 0,
        artifacts_count: int = 0,
        generated_files: Optional[Mapping[str, str]] = None,
    ) -> "PipelineDelta":
        """Build a success delta. The trailing underscore on the name avoids
        shadowing the ``success`` field while still reading naturally at
        call sites (``PipelineDelta.success_(...)``).
        """
        return cls(
            pipeline_name=pipeline_name,
            success=True,
            files_written=files_written,
            files_skipped=files_skipped,
            artifacts_count=artifacts_count,
            generated_files=dict(generated_files) if generated_files else {},
        )

    @classmethod
    def failure(cls, pipeline_name: str, exc: BaseException) -> "PipelineDelta":
        """Build a failure delta from a live exception.

        Captures ``type(exc).__name__``, ``str(exc)``, and a fully-formatted
        traceback (chained ``__cause__``/``__context__`` walked by
        :func:`traceback.format_exception`). The exception object itself
        is never stored — only its string projection — so the delta
        survives pickling across the spawn boundary intact.
        """
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return cls(
            pipeline_name=pipeline_name,
            success=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
            error_traceback=tb,
        )
