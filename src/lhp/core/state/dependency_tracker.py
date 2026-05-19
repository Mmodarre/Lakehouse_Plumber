"""Dependency tracking service for LakehousePlumber state management."""

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

from ..services.blueprint_expander import BlueprintProvenance
from ..state_models import FileState, GlobalDependencies, ProjectState

if TYPE_CHECKING:
    from .checksum_cache import ChecksumCache


@runtime_checkable
class _StateLike(Protocol):
    """Structural type for state objects accepted by :class:`DependencyTracker`.

    Both :class:`ProjectState` and :class:`PipelineState` expose an
    ``environments`` mapping of the same shape, which is the only attribute
    the tracker's internal methods touch. Using a Protocol avoids a circular
    import on :class:`PipelineState` (defined in ``pipeline_state_manager``).
    """

    environments: Dict[str, Dict[str, FileState]]


def state_key(project_root: Path, p: Path) -> str:
    """Normalised POSIX-form key for storing ``p`` in the state.

    Path is made relative to ``project_root`` when possible; absolute or
    unrelated paths fall back to their string form. The ``as_posix``
    conversion keeps state files stable across platforms.
    """
    try:
        rel = p.relative_to(project_root)
    except ValueError:
        rel = p
    return Path(str(rel)).as_posix()


class DependencyTracker:
    """
    Service for tracking file dependencies and checksums.

    Handles file registration, dependency resolution, checksum calculation,
    and provides file lookup functionality for the state management system.
    """

    def __init__(
        self,
        project_root: Path,
        state: Optional[_StateLike] = None,
        checksum_cache: Optional["ChecksumCache"] = None,
        pipeline_scope: bool = False,
    ):
        """
        Initialize dependency tracker.

        New callers should construct via :meth:`for_pipeline` or
        :meth:`for_project` — those factories make the scope explicit. The
        constructor itself remains usable directly for existing
        non-scope-specific call sites (the legacy ``StateManager``/``StateAnalyzer``
        constructions, which pass neither a state nor a cache).

        Args:
            project_root: Root directory of the LakehousePlumber project.
            state: Optional state object the tracker is bound to. Stored as
                ``self._state`` for callers that prefer the state-on-self
                pattern (used by :class:`PipelineStateManager`). The existing
                instance methods still accept ``state`` as an explicit
                parameter; this attribute is informational, not required.
            checksum_cache: Optional :class:`ChecksumCache` instance. When
                provided, :meth:`calculate_checksum` deduplicates reads via
                the cache. Construction-time-only — there is no setter
                (workers do not have access to the cache, so
                :meth:`for_pipeline` does not accept one).
            pipeline_scope: When True, :meth:`track_generated_file` skips the
                project-wide ``update_global_dependencies`` refresh — that
                field lives on :class:`ProjectState` only and the main
                thread's ``ProjectStateManager.save_global`` performs the
                refresh once per batch. :meth:`for_pipeline` sets this True;
                :meth:`for_project` and direct construction leave it False.
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        self._state: Optional[_StateLike] = state
        self._checksum_cache = checksum_cache
        self._pipeline_scope = pipeline_scope
        # Used by track_generated_file to set FileState.synthetic=True so
        # cleanup's slow path can skip blueprint-expanded flowgroups safely.
        self._blueprint_provenance: Dict[Tuple[str, str], BlueprintProvenance] = {}

        # Initialize dependency resolver with the same cache.
        from ..state_dependency_resolver import StateDependencyResolver

        self.dependency_resolver = StateDependencyResolver(
            project_root, checksum_cache=checksum_cache
        )


    @classmethod
    def for_pipeline(
        cls,
        state: _StateLike,
        project_root: Path,
    ) -> "DependencyTracker":
        """Construct a tracker scoped to one pipeline's :class:`PipelineState`.

        Used inside workers via :class:`PipelineStateManager`. The worker
        gets a fresh :class:`ChecksumCache` so a multi-doc source YAML
        referenced by N flowgroups is hashed once, not N times. The cache
        is process-local — it is never pickled across the spawn boundary.

        Args:
            state: A :class:`PipelineState` (or any object exposing the
                :class:`_StateLike` ``environments`` mapping).
            project_root: Project root directory.

        Returns:
            A :class:`DependencyTracker` bound to ``state``.
        """
        from .checksum_cache import ChecksumCache

        return cls(
            project_root=project_root,
            state=state,
            checksum_cache=ChecksumCache(),
            pipeline_scope=True,
        )

    @classmethod
    def for_project(
        cls,
        state: _StateLike,
        project_root: Path,
        checksum_cache: Optional["ChecksumCache"] = None,
    ) -> "DependencyTracker":
        """Construct a tracker scoped to the project-wide :class:`ProjectState`.

        Used on the main thread by :class:`ProjectStateManager` for aggregate
        consumers (statistics, staleness, orphan scan). May share a
        :class:`ChecksumCache` across calls to deduplicate source-side
        checksum reads.

        Args:
            state: A :class:`ProjectState` (or any object exposing the
                :class:`_StateLike` ``environments`` mapping).
            project_root: Project root directory.
            checksum_cache: Optional shared :class:`ChecksumCache`.

        Returns:
            A :class:`DependencyTracker` bound to ``state`` and ``cache``.
        """
        return cls(
            project_root=project_root, state=state, checksum_cache=checksum_cache
        )

    def set_blueprint_provenance(
        self, provenance: Optional[Dict[Tuple[str, str], BlueprintProvenance]]
    ) -> None:
        """Forward the blueprint provenance map to the resolver.

        Stored locally too because track_generated_file uses it to set
        FileState.synthetic=True for blueprint-expanded flowgroups.
        """
        self._blueprint_provenance = provenance or {}
        self.dependency_resolver.set_blueprint_provenance(provenance)

    def track_generated_file(
        self,
        state: ProjectState,
        generated_path: Path,
        source_yaml: Path,
        environment: str,
        pipeline: str,
        flowgroup: str,
    ) -> None:
        """
        Track a generated file in the state with dependency resolution.

        Args:
            state: ProjectState to update
            generated_path: Path to the generated file
            source_yaml: Path to the source YAML file
            environment: Environment name
            pipeline: Pipeline name
            flowgroup: FlowGroup name
        """
        key_generated = state_key(self.project_root, generated_path)
        key_source = state_key(self.project_root, source_yaml)

        # Resolve absolute paths for checksum calculation.
        resolved_generated_path = (
            self.project_root / generated_path
            if not generated_path.is_absolute()
            else generated_path
        )
        resolved_source_yaml = (
            self.project_root / source_yaml
            if not source_yaml.is_absolute()
            else source_yaml
        )

        generated_checksum = self.calculate_checksum(resolved_generated_path)
        source_checksum = self.calculate_checksum(resolved_source_yaml)

        file_dependencies = self.dependency_resolver.resolve_file_dependencies(
            Path(key_source), environment, pipeline, flowgroup
        )

        # Mark the FileState as synthetic when its (pipeline, flowgroup)
        # is in the blueprint provenance map. The cleanup service's slow path
        # uses this flag to skip orphan-checks that would otherwise delete
        # legitimate synthetic files (the slow path can't reverse-resolve
        # blueprint expansion).
        is_synthetic = (pipeline, flowgroup) in self._blueprint_provenance

        file_state = FileState(
            source_yaml=key_source,
            generated_path=key_generated,
            checksum=generated_checksum,
            source_yaml_checksum=source_checksum,
            timestamp=datetime.now().isoformat(),
            environment=environment,
            pipeline=pipeline,
            flowgroup=flowgroup,
            file_dependencies=file_dependencies,
            synthetic=is_synthetic,
        )

        if environment not in state.environments:
            state.environments[environment] = {}

        state.environments[environment][key_generated] = file_state

        # Update global dependencies for this environment. Skip in worker
        # (pipeline-scope) trackers — PipelineState has no global_dependencies
        # field, and ProjectStateManager.save_global() performs the per-env
        # refresh once at end of batch on the main thread.
        if not self._pipeline_scope:
            self.update_global_dependencies(state, environment)

        self.logger.debug(
            f"Tracked generated file: {key_generated} from {key_source} with "
            f"{len(file_dependencies)} dependencies (synthetic={is_synthetic})"
        )

    def track_pipeline_artifact(
        self,
        state: ProjectState,
        generated_path: Path,
        environment: str,
        pipeline: str,
        artifact_type: str,
    ) -> None:
        """Track a pipeline-level artifact (not tied to a single flowgroup).

        Simpler than track_generated_file — no dependency resolution or
        substitution key tracking. Uses ``lhp.yaml`` as source and a reserved
        sentinel flowgroup so it never collides with user flowgroups.

        Args:
            state: ProjectState to update
            generated_path: Path to the generated artifact file
            environment: Environment name
            pipeline: Pipeline name
            artifact_type: Identifier for the artifact kind (e.g. "test_reporting_hook")
        """
        key_generated = state_key(self.project_root, generated_path)

        resolved_path = (
            self.project_root / generated_path
            if not generated_path.is_absolute()
            else generated_path
        )
        checksum = self.calculate_checksum(resolved_path)

        source_yaml_path = self.project_root / "lhp.yaml"
        source_checksum = self.calculate_checksum(source_yaml_path)

        file_state = FileState(
            source_yaml="lhp.yaml",
            generated_path=key_generated,
            checksum=checksum,
            source_yaml_checksum=source_checksum,
            timestamp=datetime.now().isoformat(),
            environment=environment,
            pipeline=pipeline,
            flowgroup="__test_reporting__",
            artifact_type=artifact_type,
        )

        if environment not in state.environments:
            state.environments[environment] = {}

        state.environments[environment][key_generated] = file_state

        self.logger.debug(
            f"Tracked pipeline artifact: {key_generated} (type={artifact_type})"
        )

    def update_global_dependencies(self, state: ProjectState, environment: str) -> None:
        """
        Update global dependencies for an environment.

        Args:
            state: ProjectState to update
            environment: Environment name
        """
        try:
            # Resolve global dependencies
            global_deps = self.dependency_resolver.resolve_global_dependencies(
                environment
            )

            # Convert to GlobalDependencies object
            substitution_file = None
            project_config = None

            for dep_path, dep_info in global_deps.items():
                if dep_info.type == "substitution":
                    substitution_file = dep_info
                elif dep_info.type == "project_config":
                    project_config = dep_info

            # Ensure global_dependencies exists in state
            if state.global_dependencies is None:
                state.global_dependencies = {}

            # Update global dependencies for this environment
            state.global_dependencies[environment] = GlobalDependencies(
                substitution_file=substitution_file, project_config=project_config
            )

            self.logger.debug(
                f"Updated global dependencies for environment: {environment}"
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to update global dependencies for {environment}: {e}"
            )

    def get_generated_files(
        self, state: ProjectState, environment: str
    ) -> Dict[str, FileState]:
        """
        Get all generated files for an environment.

        Args:
            state: ProjectState to query
            environment: Environment name

        Returns:
            Dictionary mapping file paths to FileState objects
        """
        return state.environments.get(environment, {})

    def get_files_by_source(
        self, state: ProjectState, source_yaml: Path, environment: str
    ) -> List[FileState]:
        """
        Get all files generated from a specific source YAML.

        Args:
            state: ProjectState to query
            source_yaml: Path to the source YAML file
            environment: Environment name

        Returns:
            List of FileState objects for files generated from this source
        """
        try:
            rel_source = str(source_yaml.relative_to(self.project_root))
        except ValueError as e:
            self.logger.debug(
                f"Source YAML not relative to project root, using absolute: {e}"
            )
            rel_source = str(source_yaml)

        env_files = state.environments.get(environment, {})
        # Normalize paths for comparison to handle cross-platform differences
        return [
            file_state
            for file_state in env_files.values()
            if Path(file_state.source_yaml).as_posix() == Path(rel_source).as_posix()
        ]

    def calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate SHA256 checksum of a file.

        Uses shared ChecksumCache when available for deduplication.

        Args:
            file_path: Path to file for checksum calculation

        Returns:
            SHA256 hexdigest string, empty string if calculation fails
        """
        if self._checksum_cache is not None:
            return self._checksum_cache.get(file_path)

        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return ""

    def get_file_dependencies_summary(
        self, state: ProjectState, environment: str
    ) -> Dict[str, int]:
        """
        Get summary of dependencies across all tracked files.

        Args:
            state: ProjectState to analyze
            environment: Environment name

        Returns:
            Dictionary with dependency statistics
        """
        env_files = state.environments.get(environment, {})
        dependency_counts = {}

        for file_state in env_files.values():
            if file_state.file_dependencies:
                dep_count = len(file_state.file_dependencies)
                dependency_counts[file_state.generated_path] = dep_count

        return dependency_counts

    def get_all_dependency_files(self, state: ProjectState, environment: str) -> set:
        """
        Get all unique dependency files across tracked files.

        Args:
            state: ProjectState to analyze
            environment: Environment name

        Returns:
            Set of unique dependency file paths
        """
        all_deps = set()
        env_files = state.environments.get(environment, {})

        # Add source YAML files
        for file_state in env_files.values():
            all_deps.add(file_state.source_yaml)

            # Add file-specific dependencies
            if file_state.file_dependencies:
                all_deps.update(file_state.file_dependencies.keys())

        # Add global dependencies
        if state.global_dependencies and environment in state.global_dependencies:
            global_deps = state.global_dependencies[environment]
            if global_deps.substitution_file:
                all_deps.add(global_deps.substitution_file.path)
            if global_deps.project_config:
                all_deps.add(global_deps.project_config.path)

        return all_deps
