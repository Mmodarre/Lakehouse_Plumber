"""Worker-side state manager for one pipeline + one environment.

:class:`PipelineStateManager` is the worker-process entry point into the state
subsystem. Each worker constructs one instance, mutates it via
``track_generated_file`` / ``track_pipeline_artifact``, and calls ``save()``
exactly once at end of pipeline. The shard is written via ``os.replace`` so
the on-disk state is either the prior generation's or this generation's,
never partial.

Workers MUST NOT receive a :class:`ProjectStateManager` — that invariant is
enforced statically by the worker's signature in :mod:`pipeline_executor`.
Workers see only this class, scoped to one pipeline at a time, so
cross-pipeline mutations are impossible by construction.

Multi-env invariant
-------------------
A pipeline's shard may already contain entries for other environments
(e.g. ``dev`` populated yesterday, ``prod`` mutated today). The load-modify-write
cycle preserves the unmodified environments untouched: workers mutate only
``state.environments[self._environment]``, then ``_state_to_payload`` rounds
all environments back to disk. :func:`_state_from_payload` /
:func:`_state_to_payload` are deliberately written as identity maps over the
``environments`` field for that reason.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from ..state_models import FileState, PipelineStatePayload
from .dependency_tracker import DependencyTracker, state_key
from .state_persistence import StatePersistence

if TYPE_CHECKING:
    from ..services.blueprint_expander import BlueprintProvenance

logger = logging.getLogger(__name__)


@dataclass
class PipelineState:
    """In-memory state for ONE pipeline across its (possibly multiple) envs.

    Conforms to the structural ``_StateLike`` protocol expected by
    :class:`DependencyTracker` — exposes the same ``environments`` mapping
    shape that :class:`~lhp.core.state_models.ProjectState` does, so the same
    tracker code paths work for both.

    Args:
        pipeline: Name of the pipeline this state belongs to. Becomes the
            shard's filename stem on disk.
        environments: ``env -> {rel_generated_path: FileState}``. Populated
            from the on-disk shard at construction; mutated in-place by the
            tracker; serialized back to disk on :meth:`PipelineStateManager.save`.
    """

    pipeline: str = ""
    environments: Dict[str, Dict[str, FileState]] = field(default_factory=dict)


def _state_from_payload(
    payload: Optional[PipelineStatePayload],
    pipeline_name: str,
) -> PipelineState:
    """Hydrate a :class:`PipelineState` from a loaded shard payload.

    Returns a fresh empty :class:`PipelineState` when ``payload is None``
    (first-time generation; the shard doesn't exist yet). Otherwise copies
    fields from the payload — environments are preserved across ALL envs
    present in the payload, not just the worker's current env.

    Args:
        payload: Loaded :class:`PipelineStatePayload` or ``None``.
        pipeline_name: Falls back to this when the payload is absent.

    Returns:
        Hydrated :class:`PipelineState`.
    """
    if payload is None:
        return PipelineState(pipeline=pipeline_name, environments={})
    return PipelineState(
        pipeline=payload.pipeline or pipeline_name,
        environments=payload.environments,
    )


def _state_to_payload(state: PipelineState) -> PipelineStatePayload:
    """Inverse of :func:`_state_from_payload`. Lossless round-trip.

    Copies the ``environments`` mapping into a :class:`PipelineStatePayload`
    suitable for atomic write by :meth:`StatePersistence.save_pipeline_shard`.

    Args:
        state: In-memory :class:`PipelineState`.

    Returns:
        Serialization-ready :class:`PipelineStatePayload`.
    """
    return PipelineStatePayload(
        pipeline=state.pipeline,
        environments=state.environments,
    )


class PipelineStateManager:
    """Worker-side state manager. One pipeline, one current environment.

    Mutations are kept local to ``self._state`` (a :class:`PipelineState`).
    Calling :meth:`save` atomically writes the shard via ``os.replace`` and
    is a no-op when nothing changed (the dirty flag is set by the track*
    methods).

    Invariant: workers mutate only ``self._state.environments[self._environment]``.
    Other environments present in the shard at load time are round-tripped
    back to disk unchanged by the load-modify-write cycle.
    """

    def __init__(
        self,
        state_dir: Path,
        pipeline_name: str,
        environment: str,
        project_root: Path,
    ):
        """Construct a state manager scoped to one pipeline + one env.

        Loads the existing shard if present; otherwise initializes an empty
        :class:`PipelineState`. Constructs an internal
        :class:`DependencyTracker` via :meth:`DependencyTracker.for_pipeline`,
        which gives the worker its own per-process :class:`ChecksumCache`
        so multi-doc source YAMLs are hashed once per pipeline.

        Args:
            state_dir: ``<project_root>/.lhp_state`` directory. Created on
                first write.
            pipeline_name: Identifier used as the shard's filename stem
                (also stored on the in-memory state). Must not collide with
                the reserved ``_global`` stem; :meth:`save` will raise on
                conflict via :meth:`StatePersistence.save_pipeline_shard`.
            environment: Environment this worker is generating for. Mutations
                are restricted to this env's slice of the shard.
            project_root: Project root directory (passed through to the
                tracker for relative-path resolution).
        """
        self._state_dir = state_dir
        self._pipeline_name = pipeline_name
        self._environment = environment
        self._project_root = project_root

        existing = StatePersistence.load_pipeline_shard(state_dir, pipeline_name)
        self._state: PipelineState = _state_from_payload(existing, pipeline_name)
        self._tracker = DependencyTracker.for_pipeline(self._state, project_root)
        self._dirty = False

    @property
    def state_dir(self) -> Path:
        """``<project_root>/.lhp_state`` directory."""
        return self._state_dir

    @property
    def pipeline_name(self) -> str:
        """Pipeline this manager is scoped to."""
        return self._pipeline_name

    @property
    def environment(self) -> str:
        """Environment this manager is scoped to."""
        return self._environment

    @property
    def project_root(self) -> Path:
        """Project root directory."""
        return self._project_root

    @property
    def state(self) -> PipelineState:
        """Underlying :class:`PipelineState`. Intended for tests and the
        :class:`DependencyTracker` only; worker code should mutate via
        :meth:`track_generated_file` / :meth:`track_pipeline_artifact`.
        """
        return self._state

    @property
    def dirty(self) -> bool:
        """True iff this manager has mutations not yet persisted via :meth:`save`."""
        return self._dirty

    def set_blueprint_provenance(
        self,
        provenance: Optional[Dict[Tuple[str, str], "BlueprintProvenance"]],
    ) -> None:
        """Forward the blueprint provenance map to the internal tracker.

        The provenance map does not cross the spawn boundary, so the
        worker reinjects it here.
        """
        self._tracker.set_blueprint_provenance(provenance)

    def track_generated_file(
        self,
        generated_path: Path,
        source_yaml: Path,
        flowgroup: str,
    ) -> None:
        """Record a freshly-generated ``.py`` file in this pipeline's shard.

        Delegates to the underlying tracker, which computes checksums,
        resolves per-file dependencies (presets/templates/substitution refs),
        and inserts a :class:`FileState` into
        ``self._state.environments[self._environment]``.

        Args:
            generated_path: Path to the generated ``.py`` file. May be
                relative to ``project_root`` or absolute; both are accepted.
            source_yaml: Path to the YAML flowgroup that produced it.
            flowgroup: FlowGroup name (the YAML's ``flowgroup`` field).
        """
        self._tracker.track_generated_file(
            self._state,
            generated_path,
            source_yaml,
            self._environment,
            self._pipeline_name,
            flowgroup,
        )
        self._dirty = True

    def untrack_generated_file(self, generated_path: Path) -> bool:
        """Drop a previously-tracked file from this pipeline's shard.

        Used by the empty-content cleanup path inside the worker: when a
        flowgroup's actions collapse to no code (e.g. a test-only flowgroup
        regenerated without ``--include-tests``), the worker unlinks the
        ``.py`` file and must also strip its entry from
        ``environments[self._environment]`` so the next run does not
        treat the missing file as drift.

        Args:
            generated_path: Path to the previously-tracked file. May be
                relative to ``project_root`` or absolute; both are normalised
                identically to :meth:`track_generated_file`'s lookup key.

        Returns:
            ``True`` when an entry was found and removed (caller should
            persist via :meth:`save`); ``False`` when the path was not
            tracked (idempotent no-op).
        """
        key = state_key(self._project_root, generated_path)
        env_files = self._state.environments.get(self._environment)
        if not env_files or key not in env_files:
            return False
        del env_files[key]
        self._dirty = True
        return True

    def track_pipeline_artifact(
        self,
        generated_path: Path,
        artifact_type: str,
    ) -> None:
        """Record a pipeline-level artifact (not tied to a single flowgroup).

        Today's representation: the artifact lands as a :class:`FileState`
        entry with ``flowgroup="__test_reporting__"`` (sentinel) and
        ``artifact_type`` set. See the design note in
        :meth:`DependencyTracker.track_pipeline_artifact`.

        Args:
            generated_path: Path to the generated artifact file.
            artifact_type: Identifier for the artifact kind (e.g.
                ``"test_reporting_hook"``).
        """
        self._tracker.track_pipeline_artifact(
            self._state,
            generated_path,
            self._environment,
            self._pipeline_name,
            artifact_type,
        )
        self._dirty = True

    def save(self) -> None:
        """Atomically write the shard to ``<state_dir>/<pipeline>.json``.

        No-op when ``self._dirty`` is False (no mutations since the last
        save or since construction). Otherwise serializes the in-memory
        :class:`PipelineState` to a :class:`PipelineStatePayload` and hands
        it to :meth:`StatePersistence.save_pipeline_shard`.

        Multi-env entries present at load time are preserved by
        :func:`_state_to_payload` — workers mutate only their env's slice,
        and the round-trip is lossless for the other envs.
        """
        if not self._dirty:
            return
        payload = _state_to_payload(self._state)
        StatePersistence.save_pipeline_shard(
            self._state_dir, self._pipeline_name, payload, logger=logger
        )
        self._dirty = False
