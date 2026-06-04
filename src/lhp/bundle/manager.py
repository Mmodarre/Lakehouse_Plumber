# JUSTIFIED: bundle-manager state-machine + resource-file sync +
# Databricks-CLI delegation form one cohesive runtime; splitting
# requires a typed event bus to maintain manager-state invariants.
# TODO(Phase 9.5): decompose into BundleStateMachine + ResourceFileSyncer + DatabricksCliAdapter

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..core.codegen.template_renderer import TemplateRenderer
from ..core.coordination.monitoring_pipeline_builder import (
    resolve_monitoring_pipeline_name,
)
from ..errors import ErrorFactory, LHPError, codes
from ..utils.performance_timer import perf_timer, record_count
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

    # Subdirectories under ``generated/<env>/`` that LHP reserves for its own
    # use and that must NOT be treated as pipeline directories. Currently just
    # the wheel-packaging staging dir (``generated/<env>/_wheels/<pipeline>/``).
    # A set so future reserved names are trivial to add.
    _RESERVED_GENERATED_SUBDIRS = {"_wheels"}

    def __init__(
        self,
        project_root: Union[Path, str],
        pipeline_config_path: Optional[str] = None,
        project_config: Optional[Any] = None,
    ):
        if project_root is None:
            raise ErrorFactory.config_error(
                codes.CFG_028,
                title="BundleManager requires a project root",
                details="project_root cannot be None when initializing BundleManager.",
                suggestions=[
                    "Ensure a valid project root path is provided",
                    "Run from within an LHP project directory",
                ],
            )

        if isinstance(project_root, str):
            project_root = Path(project_root)

        self.project_root = project_root
        self.project_config = project_config
        self.resources_dir = project_root / "resources" / "lhp"
        self.logger = logging.getLogger(__name__)

        self.template_renderer = TemplateRenderer.from_package()

        from ..core.loaders.pipeline_config_loader import PipelineConfigLoader

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
            from ..core.processing.substitution import EnhancedSubstitutionManager

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
        """Wipe-and-regenerate contract: callers must clear ``resources/lhp/`` before
        invoking this method. BundleManager only writes files for pipelines that
        exist under ``output_dir`` — it does not preserve, back up, or delete
        any pre-existing files.
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
        """Callers are expected to have wiped ``resources/lhp/`` before invoking
        sync, so this method always (re)creates the resource file for the pipeline.
        """
        self._create_new_resource_file(pipeline_name, pipeline_dir.parent, env)
        return True

    def ensure_resources_directory(self):
        self._safe_directory_create(self.resources_dir, "LHP resources directory")

    def get_pipeline_directories(self, output_dir: Path) -> List[Path]:
        self._safe_directory_access(output_dir, "output directory")

        try:
            pipeline_dirs = []
            # Sort directories to ensure deterministic processing order across platforms
            for item in sorted(output_dir.iterdir()):
                if item.is_dir():
                    if item.name in self._RESERVED_GENERATED_SUBDIRS:
                        self.logger.debug(
                            "Skipping reserved generated subdirectory: %s", item.name
                        )
                        continue
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
        """
        if not self.project_config or not getattr(
            self.project_config, "event_log", None
        ):
            return pipeline_config

        event_log_cfg = self.project_config.event_log

        if not event_log_cfg.enabled:
            return pipeline_config

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

        # R8: ``packaging`` is an LHP-internal toggle consumed by the generator,
        # never by Databricks. Strip it in BOTH modes BEFORE render — the
        # template's pass-through loop would otherwise leak it into the resource
        # YAML (it is not in EXPLICITLY_RENDERED_PIPELINE_CONFIG_KEYS).
        pipeline_config_resolved.pop("packaging", None)

        # Wheel mode: inject this pipeline's wheel artifact as the last
        # ``environment.dependencies`` entry, preserving any user-declared deps
        # (R11). Source mode is unchanged apart from the strip above.
        packaging_mode = self.config_loader.resolve_packaging_modes([pipeline_name])[
            pipeline_name
        ]
        if packaging_mode == "wheel":
            self._inject_wheel_dependency(pipeline_config_resolved, pipeline_name, env)

        catalog = pipeline_config_resolved.get("catalog")
        schema = pipeline_config_resolved.get("schema")
        if (
            not catalog
            or not schema
            or not str(catalog).strip()
            or not str(schema).strip()
        ):
            raise ErrorFactory.general_error(
                codes.GEN_001,
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

    def _resolve_artifact_volume(self, env: str) -> str:
        """Resolve and validate the project's ``wheel.artifact_volume`` for ``env``.

        The raw value (``lhp.yaml`` ``wheel.artifact_volume``) is run through the
        same per-env substitution manager that resolves ``event_log`` and every
        other per-env value, so ``${catalog}``/``${...}`` tokens expand against
        ``substitutions/<env>.yaml``.

        This is the single ``/Volumes/...`` validation point shared by every
        wheel-packaging consumer (the per-pipeline wheel-reference injector and
        the bundle-level ``artifact_path`` writer): serverless installs custom
        wheels only from a UC volume, so the *resolved* value MUST start with
        ``/Volumes/``.

        Raises:
            LHPError: ``LHP-CFG-061`` if the project declares no
                ``wheel.artifact_volume`` (absent/empty) OR if the resolved value
                does not start with ``/Volumes/`` — a wheel-mode pipeline cannot
                resolve a valid install path in either case.
        """
        wheel_cfg = getattr(self.project_config, "wheel", None)
        artifact_volume_raw = getattr(wheel_cfg, "artifact_volume", None)
        if not artifact_volume_raw or not str(artifact_volume_raw).strip():
            raise ErrorFactory.config_error(
                codes.CFG_061,
                title="Wheel packaging requires a /Volumes/... artifact volume",
                details=(
                    "A pipeline is configured for wheel packaging but the project "
                    "defines no 'wheel.artifact_volume' in lhp.yaml, so the wheel's "
                    "install path cannot be resolved."
                ),
                suggestions=[
                    "Add a 'wheel.artifact_volume' (a /Volumes/... path) to lhp.yaml",
                    "Or set the pipeline's 'packaging' back to 'source'",
                ],
                context={"env": env},
            )

        sub_mgr = self._get_substitution_manager(env)
        if sub_mgr is None:
            resolved = str(artifact_volume_raw)
        else:
            # substitute_yaml resolves tokens recursively; wrap the scalar so it
            # rides the exact same path as event_log (no separate string API).
            resolved = str(sub_mgr.substitute_yaml({"v": artifact_volume_raw})["v"])

        if not resolved.startswith("/Volumes/"):
            raise ErrorFactory.config_error(
                codes.CFG_061,
                title="Wheel packaging requires a /Volumes/... artifact volume",
                details=(
                    f"The resolved 'wheel.artifact_volume' for environment "
                    f"'{env}' is {resolved!r}, which is not a Unity Catalog volume "
                    f"path. Serverless compute installs custom wheels only from a "
                    f"/Volumes/... path, so wheel packaging cannot proceed."
                ),
                suggestions=[
                    "Set 'wheel.artifact_volume' in lhp.yaml to a /Volumes/... path",
                    "Verify any ${tokens} resolve to a /Volumes/... path for this env",
                    "Or set the pipeline's 'packaging' back to 'source'",
                ],
                context={"env": env, "resolved_artifact_volume": resolved},
            )
        return resolved

    def _inject_wheel_dependency(
        self, pipeline_config: Dict[str, Any], pipeline_name: str, env: str
    ) -> None:
        """Append this pipeline's wheel artifact to ``environment.dependencies``.

        Mutates ``pipeline_config`` in place. Handles the three shapes of
        ``environment``: absent (create), present without ``dependencies`` (add),
        present with a user ``dependencies`` list (append, wheel_ref last so user
        deps are preserved — R11).

        ``wheel_ref`` is the resolved absolute path
        ``<resolved_artifact_volume>/<wheel_filename>``. The filename is read from
        disk (``generated/<env>/_wheels/<pipeline>/dist/*.whl``): in wheel mode the
        on-disk name IS the content-addressed identity, so it is taken as-is rather
        than recomputed.

        Raises:
            LHPError: ``LHP-GEN-001`` if the built wheel is not found on disk
                (generation should have produced exactly one ``.whl``).
        """
        resolved_volume = self._resolve_artifact_volume(env)
        wheel_filename = self._find_wheel_filename(pipeline_name, env)
        wheel_ref = f"{resolved_volume.rstrip('/')}/{wheel_filename}"

        environment = pipeline_config.get("environment")
        if not isinstance(environment, dict):
            environment = {}
            pipeline_config["environment"] = environment

        dependencies = environment.get("dependencies")
        if not isinstance(dependencies, list):
            dependencies = []
        else:
            dependencies = list(dependencies)
        dependencies.append(wheel_ref)
        environment["dependencies"] = dependencies

        self.logger.debug(
            f"Injected wheel dependency for pipeline '{pipeline_name}': {wheel_ref}"
        )

    def _find_wheel_filename(self, pipeline_name: str, env: str) -> str:
        """Return the single ``.whl`` filename built for this pipeline under env.

        Globs ``generated/<env>/_wheels/<pipeline>/dist/*.whl`` and requires
        exactly one match.

        Raises:
            LHPError: ``LHP-GEN-001`` if zero or more than one wheel is found.
        """
        dist_dir = (
            self.project_root / "generated" / env / "_wheels" / pipeline_name / "dist"
        )
        matches = sorted(dist_dir.glob("*.whl"))
        if len(matches) != 1:
            raise ErrorFactory.general_error(
                codes.GEN_001,
                title="Wheel artifact not found for wheel-mode pipeline",
                details=(
                    f"Pipeline '{pipeline_name}' is configured for wheel packaging "
                    f"but {len(matches)} wheel file(s) were found under "
                    f"{dist_dir} (expected exactly one). Generation should have "
                    f"built the wheel before the bundle-write phase."
                ),
                suggestions=[
                    "Run 'lhp generate' so the wheel is built before bundle sync",
                    "Verify the pipeline's wheel build did not fail",
                ],
                context={
                    "pipeline": pipeline_name,
                    "env": env,
                    "dist_dir": str(dist_dir),
                    "matches": [m.name for m in matches],
                },
            )
        return matches[0].name

    def emit_wheels_bundle_file(self, output_dir: Path, env: str) -> None:
        """Write the LHP-owned ``resources/lhp/_wheels.bundle.yml`` for ``env``.

        Self-derives the wheel-mode pipeline list from ``output_dir`` (symmetric
        with ``_inject_wheel_dependency`` — the API layer passes nothing extra):
        every generated pipeline directory whose resolved packaging mode is
        ``"wheel"``. The emitted fragment declares, for each such pipeline, a
        ``<pipeline>_whl`` artifact, sets ``targets.<env>.workspace.artifact_path``
        to the resolved UC volume, and excludes the ``_wheels/`` staging dir from
        the bundle file sync (the wheels travel as uploaded artifacts, R2).

        No-op when there are zero wheel pipelines: nothing is written and the
        method returns early, so source-only projects gain no bundle file.

        ``${bundle.target}`` in the sync-exclude is a Databricks bundle runtime
        variable, emitted literally — it is NOT an LHP token and is never
        substituted here.

        Raises:
            LHPError: ``LHP-CFG-061`` (via ``_resolve_artifact_volume``) if a
                wheel pipeline exists but the resolved ``wheel.artifact_volume``
                is absent/empty or not a ``/Volumes/...`` path.
        """
        pipeline_dirs = self.get_pipeline_directories(output_dir)
        modes = self.config_loader.resolve_packaging_modes(
            [p.name for p in pipeline_dirs]
        )
        wheel_pipelines = [p.name for p in pipeline_dirs if modes[p.name] == "wheel"]

        if not wheel_pipelines:
            self.logger.debug(
                "No wheel-mode pipelines under %s; skipping _wheels.bundle.yml",
                output_dir,
            )
            return

        artifact_path = self._resolve_artifact_volume(env)

        context = {
            "env": env,
            "wheel_pipelines": wheel_pipelines,
            "artifact_path": artifact_path,
        }
        content = self.template_renderer.render_template(
            "bundle/wheels_bundle.yml.j2", context
        )

        self.ensure_resources_directory()
        wheels_file = self.resources_dir / "_wheels.bundle.yml"
        try:
            wheels_file.write_text(content, encoding="utf-8")
        except (OSError, PermissionError) as e:
            raise BundleResourceError(
                f"Failed to write wheels bundle file {wheels_file}: {e}", e
            ) from e

        self.logger.info(
            f"Wrote wheel packaging bundle file for {len(wheel_pipelines)} "
            f"pipeline(s): {wheels_file}"
        )

    def _safe_directory_create(
        self, directory: Path, error_context: str = "directory"
    ) -> None:
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

    def _setup_sync_environment(self, output_dir: Path) -> List[Path]:
        """Ensure resources/lhp/ exists and return the current pipeline dirs."""
        self.ensure_resources_directory()
        return self.get_pipeline_directories(output_dir)

    def _process_current_pipelines(
        self,
        current_pipeline_dirs: List[Path],
        env: str,
    ) -> int:
        written_count = 0

        for pipeline_dir in current_pipeline_dirs:
            pipeline_name = pipeline_dir.name

            try:
                if self._sync_pipeline_resource(pipeline_name, pipeline_dir, env):
                    written_count += 1
                    self.logger.debug("Successfully synced pipeline: %s", pipeline_name)

            except (LHPError, BundleResourceError):
                # Structured errors (LHPError, BundleResourceError and its
                # YAMLProcessingError subclasses) propagate
                # as-is to the CLI error boundary.
                raise
            except Exception as e:
                raise self._handle_pipeline_error(
                    pipeline_name, e, "Pipeline sync"
                ) from e

        return written_count

    def _log_sync_summary(self, written_count: int) -> None:
        if written_count > 0:
            self.logger.info(
                f"Wrote {written_count} bundle resource file(s) under resources/lhp/"
            )
        else:
            self.logger.info(
                "No pipelines found under generated/<env>/ — nothing to write"
            )

    def _create_new_resource_file(self, pipeline_name: str, output_dir: Path, env: str):
        with perf_timer("_create_new_resource_file", category="bundle_create_resource"):
            # resources/lhp/ is already created by _setup_sync_environment for
            # the sync flow; safe to assume it exists here.
            resource_file = self.get_resource_file_path(pipeline_name)

            content = self.generate_resource_file_content(
                pipeline_name, output_dir, env
            )

            try:
                resource_file.write_text(content, encoding="utf-8")
                self.logger.info(f"Created new resource file: {resource_file}")

            except (OSError, PermissionError) as e:
                raise BundleResourceError(
                    f"Failed to create resource file {resource_file}: {e}", e
                ) from e
