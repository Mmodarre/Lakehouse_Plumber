"""
Bundle manager for LHP Databricks Asset Bundle integration.

This module provides the main BundleManager class that coordinates bundle
resource operations including resource file synchronization and management.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..core.services.monitoring_pipeline_builder import (
    resolve_monitoring_pipeline_name,
)
from ..utils.error_formatter import ErrorCategory, LHPConfigError, LHPError
from ..utils.performance_timer import perf_timer, record_count
from ..utils.template_renderer import TemplateRenderer
from .exceptions import BundleResourceError

logger = logging.getLogger(__name__)


# Top-level pipeline_config keys that pipeline_resource.yml.j2 renders explicitly.
# Anything NOT in this set is passed through as-is via the `toyaml` filter, so
# users can use new Databricks Pipelines API fields (run_as, …) without waiting
# for LHP to explicitly support them.
#
# Kept here (not inside BundleManager) so templates and tests can reference it
# via a single source of truth.
EXPLICITLY_RENDERED_PIPELINE_CONFIG_KEYS = frozenset(
    {
        "catalog",
        "schema",
        "serverless",
        "clusters",
        "configuration",
        "continuous",
        "photon",
        "edition",
        "channel",
        "notifications",
        "tags",
        "event_log",
        "environment",
        "permissions",
    }
)


class BundleManager:
    """
    Manages Databricks Asset Bundle resource files.

    Wipe-and-regenerate contract: ``resources/lhp/`` is fully wiped by the CLI
    before sync, and BundleManager re-renders one resource file per pipeline
    under ``generated/<env>/``. databricks.yml is never read or mutated.
    Catalog and schema must come from pipeline_config.yaml (per-pipeline or
    via the top-level ``project_defaults`` block).
    """

    def __init__(
        self,
        project_root: Union[Path, str],
        pipeline_config_path: Optional[str] = None,
        project_config: Optional[Any] = None,
    ):
        """
        Initialize the bundle manager.

        Args:
            project_root: Path to the project root directory
            pipeline_config_path: Optional path to custom pipeline config file (relative to project_root)
            project_config: Optional ProjectConfig with project-level settings (e.g., event_log)

        Raises:
            TypeError: If project_root is None
        """
        if project_root is None:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="028",
                title="BundleManager requires a project root",
                details="project_root cannot be None when initializing BundleManager.",
                suggestions=[
                    "Ensure a valid project root path is provided",
                    "Run from within an LHP project directory",
                ],
            )

        # Convert string to Path if necessary
        if isinstance(project_root, str):
            project_root = Path(project_root)

        self.project_root = project_root
        self.project_config = project_config
        self.resources_dir = project_root / "resources" / "lhp"
        self.logger = logging.getLogger(__name__)

        self.template_renderer = TemplateRenderer.from_package()

        from ..core.services.pipeline_config_loader import PipelineConfigLoader

        self.config_loader = PipelineConfigLoader(
            self.project_root,
            pipeline_config_path,
            monitoring_pipeline_name=self._get_monitoring_pipeline_name(),
        )

        # Cached per env to avoid re-reading + re-expanding substitutions/<env>.yaml
        # once per pipeline during a sync. Built lazily on first lookup.
        self._sub_mgr_cache: Dict[str, Optional[Any]] = {}

    def _get_substitution_manager(self, env: str) -> Optional[Any]:
        """Return a cached EnhancedSubstitutionManager for env, or None if no file."""
        if env in self._sub_mgr_cache:
            return self._sub_mgr_cache[env]

        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
        if substitution_file.exists():
            from ..utils.substitution import EnhancedSubstitutionManager

            sub_mgr: Optional[Any] = EnhancedSubstitutionManager(substitution_file, env)
        else:
            self.logger.debug(
                f"No substitution file found at {substitution_file}, using raw config"
            )
            sub_mgr = None

        self._sub_mgr_cache[env] = sub_mgr
        return sub_mgr

    def sync_resources_with_generated_files(
        self,
        output_dir: Path,
        env: str,
    ) -> int:
        """
        Write one bundle resource file per current pipeline directory.

        Wipe-and-regenerate contract: callers must clear ``resources/lhp/``
        before invoking this method. BundleManager only writes the resource
        files for the pipelines that exist under ``output_dir`` — it does not
        preserve, back up, or delete any pre-existing files.

        Args:
            output_dir: Directory containing generated Python files
            env: Environment name for template processing

        Returns:
            Number of resource files written

        Raises:
            BundleResourceError: If synchronization fails
        """
        self.logger.info("Syncing bundle resources for environment: %s", env)

        with perf_timer("bundle_sync_resources"):
            current_pipeline_dirs = self._setup_sync_environment(output_dir)
            record_count("pipelines_synced", len(current_pipeline_dirs))

            written_count = self._process_current_pipelines(current_pipeline_dirs, env)

            self._log_sync_summary(written_count)
            return written_count

    def _sync_pipeline_resource(
        self,
        pipeline_name: str,
        pipeline_dir: Path,
        env: str,
    ) -> bool:
        """
        Write a single pipeline's resource file unconditionally.

        Callers are expected to have wiped ``resources/lhp/`` before invoking
        sync, so this method always (re)creates the resource file for the
        pipeline.

        Args:
            pipeline_name: Name of the pipeline
            pipeline_dir: Directory containing pipeline Python files
            env: Environment name

        Returns:
            True (a file was written)
        """
        self._create_new_resource_file(pipeline_name, pipeline_dir.parent, env)
        return True

    def ensure_resources_directory(self):
        """Create resources/lhp directory if it doesn't exist."""
        self._safe_directory_create(self.resources_dir, "LHP resources directory")

    def get_pipeline_directories(self, output_dir: Path) -> List[Path]:
        """
        Get list of pipeline directories in the output directory.

        Args:
            output_dir: Directory to scan for pipeline directories

        Returns:
            List of pipeline directory paths in sorted order

        Raises:
            BundleResourceError: If directory access fails
        """
        # Validate directory access using utility
        self._safe_directory_access(output_dir, "output directory")

        try:
            pipeline_dirs = []
            # Sort directories to ensure deterministic processing order across platforms
            for item in sorted(output_dir.iterdir()):
                if item.is_dir():
                    pipeline_dirs.append(item)
                    self.logger.debug("Found pipeline directory: %s", item.name)

            return pipeline_dirs

        except (OSError, PermissionError) as e:
            raise BundleResourceError(
                f"Error scanning output directory {output_dir}: {e}", e
            ) from e

    def get_resource_file_path(self, pipeline_name: str) -> Path:
        """Return the path where this pipeline's resource file is written.

        Under the wipe-and-regenerate contract, ``resources/lhp/`` is empty
        when sync starts and BundleManager always writes the canonical
        ``<pipeline>.pipeline.yml`` filename, so no existence probing is needed.
        """
        return self.resources_dir / f"{pipeline_name}.pipeline.yml"

    def _inject_project_event_log(
        self, pipeline_config: Dict[str, Any], pipeline_name: str
    ) -> Dict[str, Any]:
        """Inject project-level event_log into pipeline config if applicable.

        Injection rules:
        - No project_config or no event_log → return unchanged
        - event_log disabled → return unchanged
        - pipeline_config has event_log: false → delete key, return (opt-out)
        - pipeline_config has event_log dict → return unchanged (full replace)
        - Otherwise → inject event_log block from project config

        Args:
            pipeline_config: Raw pipeline config dict (pre-substitution)
            pipeline_name: Name of the pipeline (used for event_log name generation)

        Returns:
            Pipeline config dict, potentially with event_log injected
        """
        # No project config or no event_log configured
        if not self.project_config or not getattr(
            self.project_config, "event_log", None
        ):
            return pipeline_config

        event_log_cfg = self.project_config.event_log

        # Project-level event_log is disabled
        if not event_log_cfg.enabled:
            return pipeline_config

        # Check pipeline-level override
        if "event_log" in pipeline_config:
            pipeline_event_log = pipeline_config["event_log"]

            # Explicit opt-out: event_log: false
            if pipeline_event_log is False:
                del pipeline_config["event_log"]
                logger.debug(
                    f"Pipeline '{pipeline_name}' opted out of project-level event_log"
                )
                return pipeline_config

            # Pipeline has its own event_log dict → full replace, leave unchanged
            if isinstance(pipeline_event_log, dict):
                logger.debug(
                    f"Pipeline '{pipeline_name}' has its own event_log config, "
                    f"skipping project-level injection"
                )
                return pipeline_config

        # Inject project-level event_log
        event_log_name = (
            f"{event_log_cfg.name_prefix}{pipeline_name}{event_log_cfg.name_suffix}"
        )
        pipeline_config["event_log"] = {
            "name": event_log_name,
            "catalog": event_log_cfg.catalog,
            "schema": event_log_cfg.schema_,
        }
        logger.debug(
            f"Injected project-level event_log for pipeline '{pipeline_name}': "
            f"name={event_log_name}"
        )
        return pipeline_config

    def _get_monitoring_pipeline_name(self) -> Optional[str]:
        return resolve_monitoring_pipeline_name(self.project_config)

    def generate_resource_file_content(
        self, pipeline_name: str, output_dir: Path, env: str
    ) -> str:
        """
        Generate content for a bundle resource file using Jinja2 template.

        Applies LHP token substitution to ALL fields in pipeline_config.yaml, enabling
        environment-specific configuration for node types, policies, emails, and all other
        pipeline settings. Catalog and schema MUST be defined in pipeline_config.yaml
        (either per-pipeline or via the top-level ``project_defaults`` block); they are
        never read from ``databricks.yml``.

        Catalog/schema validation is performed upstream by
        ``bundle.preflight.validate_catalog_schema`` before any wipes occur.
        The guard here is a defense-in-depth assertion that fires only if a
        non-CLI caller invokes this method without running preflight first.

        Args:
            pipeline_name: Name of the pipeline
            output_dir: Output directory
            env: Environment name for token resolution (REQUIRED)

        Returns:
            YAML content for the resource file with fully substituted pipeline config

        Raises:
            LHPConfigError: ``LHP-GEN-001`` if preflight was bypassed and
                catalog/schema is still missing/empty at the bundle-write phase.
        """
        pipeline_config_raw = self.config_loader.get_pipeline_config(pipeline_name)

        # Skip event_log injection for the monitoring pipeline (no self-reference)
        monitoring_name = self._get_monitoring_pipeline_name()
        if pipeline_name != monitoring_name:
            pipeline_config_raw = self._inject_project_event_log(
                pipeline_config_raw, pipeline_name
            )

        sub_mgr = self._get_substitution_manager(env)
        if sub_mgr is not None:
            pipeline_config_resolved = sub_mgr.substitute_yaml(pipeline_config_raw)
        else:
            pipeline_config_resolved = pipeline_config_raw

        catalog = pipeline_config_resolved.get("catalog")
        schema = pipeline_config_resolved.get("schema")
        if (
            not catalog
            or not schema
            or not str(catalog).strip()
            or not str(schema).strip()
        ):
            raise LHPConfigError(
                category=ErrorCategory.GENERAL,
                code_number="001",
                title="Internal error: preflight bypassed for bundle resource generation",
                details=(
                    f"Pipeline '{pipeline_name}' reached the bundle-write phase "
                    f"with missing/empty resolved catalog/schema "
                    f"(catalog={catalog!r}, schema={schema!r}). "
                    f"This indicates preflight validation did not run."
                ),
                suggestions=[
                    "This is a programming bug; preflight should have caught this. "
                    "Verify generate_command.py calls validate_catalog_schema "
                    "before BundleManager.",
                ],
                context={"pipeline": pipeline_name, "env": env},
            )

        self.logger.info(
            f"Pipeline '{pipeline_name}' using catalog/schema from config: {catalog}.{schema}"
        )

        # Build template context with fully resolved config
        context = {
            "pipeline_name": pipeline_name,
            "pipeline_config": pipeline_config_resolved,  # Fully substituted!
            "catalog": catalog,
            "schema": schema,
            # Scoped here (not on TemplateRenderer.env) so the set stays bound
            # to bundle pipeline rendering and doesn't leak into other templates.
            "explicitly_rendered_keys": EXPLICITLY_RENDERED_PIPELINE_CONFIG_KEYS,
        }

        return self.template_renderer.render_template(
            "bundle/pipeline_resource.yml.j2", context
        )

    # === UTILITY METHODS ===

    def _safe_directory_create(
        self, directory: Path, error_context: str = "directory"
    ) -> None:
        """
        Safely create directory with consistent error handling.

        Args:
            directory: Path to directory to create
            error_context: Context for error messages

        Raises:
            BundleResourceError: If directory creation fails
        """
        try:
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.debug("Ensured %s exists: %s", error_context, directory)
        except OSError as e:
            raise BundleResourceError(
                f"Failed to create {error_context}: {e}", e
            ) from e

    def _safe_directory_access(
        self, directory: Path, error_context: str = "directory"
    ) -> None:
        """
        Safely validate directory access with consistent error handling.

        Args:
            directory: Path to directory to validate
            error_context: Context for error messages

        Raises:
            BundleResourceError: If directory access fails
        """
        try:
            if not directory.exists():
                raise BundleResourceError(
                    f"{error_context.capitalize()} does not exist: {directory}"
                )
        except (OSError, PermissionError) as e:
            raise BundleResourceError(
                f"Cannot access {error_context} {directory}: {e}", e
            ) from e

    def _handle_pipeline_error(
        self, pipeline_name: str, error: Exception, operation: str
    ) -> BundleResourceError:
        """
        Wrap a per-pipeline failure in a consistent BundleResourceError.

        The CLI error boundary handles logging; this method only constructs
        the user-facing exception.
        """
        if isinstance(error, OSError):
            error_msg = f"File system error for pipeline '{pipeline_name}': {error}"
        else:
            error_msg = f"{operation} failed for pipeline '{pipeline_name}': {error}"

        return BundleResourceError(error_msg, error)

    # === MAIN WORKFLOW METHODS ===

    def _setup_sync_environment(self, output_dir: Path) -> List[Path]:
        """Ensure resources/lhp/ exists and return the current pipeline dirs."""
        self.ensure_resources_directory()
        return self.get_pipeline_directories(output_dir)

    def _process_current_pipelines(
        self,
        current_pipeline_dirs: List[Path],
        env: str,
    ) -> int:
        """
        Write a resource file for each current pipeline directory.

        Args:
            current_pipeline_dirs: List of pipeline directories to process
            env: Environment name for template processing

        Returns:
            Number of resource files written

        Raises:
            BundleResourceError: If pipeline processing fails
        """
        written_count = 0

        for pipeline_dir in current_pipeline_dirs:
            pipeline_name = pipeline_dir.name

            try:
                if self._sync_pipeline_resource(pipeline_name, pipeline_dir, env):
                    written_count += 1
                    self.logger.debug("Successfully synced pipeline: %s", pipeline_name)

            except (LHPError, BundleResourceError):
                # Structured errors (LHPError, BundleResourceError and its
                # YAMLProcessingError/YAMLParsingError subclasses) propagate
                # as-is to the CLI error boundary.
                raise
            except Exception as e:
                raise self._handle_pipeline_error(
                    pipeline_name, e, "Pipeline sync"
                ) from e

        return written_count

    def _log_sync_summary(self, written_count: int) -> None:
        """
        Log wipe-and-regenerate sync results summary.

        Args:
            written_count: Number of resource files written
        """
        if written_count > 0:
            self.logger.info(
                f"Wrote {written_count} bundle resource file(s) under resources/lhp/"
            )
        else:
            self.logger.info(
                "No pipelines found under generated/<env>/ — nothing to write"
            )

    def _create_new_resource_file(self, pipeline_name: str, output_dir: Path, env: str):
        """
        Create new resource file for a pipeline.

        Args:
            pipeline_name: Name of the pipeline
            output_dir: Output directory containing generated Python files
            env: Environment name for template context
        """
        with perf_timer("_create_new_resource_file", category="bundle_create_resource"):
            # resources/lhp/ is already created by _setup_sync_environment for
            # the sync flow; safe to assume it exists here.
            resource_file = self.get_resource_file_path(pipeline_name)

            # Generate resource file content from Python files
            content = self.generate_resource_file_content(
                pipeline_name, output_dir, env
            )

            try:
                resource_file.write_text(content, encoding="utf-8")
                self.logger.info(f"Created new resource file: {resource_file}")

            except (OSError, PermissionError) as e:
                raise BundleResourceError(
                    f"Failed to create resource file {resource_file}: {e}", e
                )
