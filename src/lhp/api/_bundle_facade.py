"""Private module — implementation of the public :class:`BundleFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`BundleFacade` from :mod:`lhp.api` (re-exported via
:mod:`lhp.api.facade`). This split exists purely to keep
``lhp/api/facade.py`` under the constitution §3.3 soft cap (500 lines)
while the bundle surface absorbs C5's three methods plus their helper
converters and the bundle scaffolding logic.

:stability: internal
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional, Sequence

from lhp.api.events import (
    BundleSyncCompleted,
    ErrorEmitted,
    LHPEvent,
    OperationStarted,
)
from lhp.api.responses import (
    BundleEnableResult,
    BundleSyncResult,
    BundleValidationResult,
)
from lhp.errors import LHPConfigError, LHPError
from lhp.utils.performance_timer import perf_timer

if TYPE_CHECKING:
    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10, §9.13).
    _Orchestrator = Any
    from lhp.models import FlowGroup


def _lhp_error_code_and_message(exc: BaseException) -> tuple[Optional[str], str]:
    """Extract ``(error_code, error_message)`` from an arbitrary exception.

    Returns the LHP error code (e.g. ``"LHP-CFG-026"``) and ``title``
    when ``exc`` is an :class:`LHPError`, otherwise ``(None, str(exc))``.
    Kept in-module so bundle failures don't have to round-trip through
    :mod:`lhp.api._converters_common` for the trivial conversion.
    """
    from lhp.errors import LHPError

    if isinstance(exc, LHPError):
        return exc.code, exc.title
    return None, str(exc)


def _bundle_sync_to_result(
    *,
    synced_file_count: int,
    deleted_file_count: int,
    bundle_path: Optional[Path],
    exc: Optional[BaseException] = None,
) -> BundleSyncResult:
    """Build a :class:`BundleSyncResult` for the C5 sync_resources path."""
    if exc is None:
        return BundleSyncResult(
            success=True,
            synced_file_count=synced_file_count,
            deleted_file_count=deleted_file_count,
            bundle_path=bundle_path,
        )
    code, message = _lhp_error_code_and_message(exc)
    return BundleSyncResult(
        success=False,
        synced_file_count=synced_file_count,
        deleted_file_count=deleted_file_count,
        bundle_path=bundle_path,
        error_message=message,
        error_code=code,
    )


def _bundle_validate_to_result(
    *,
    issues: Sequence[str] = (),
    exc: Optional[BaseException] = None,
) -> BundleValidationResult:
    """Build a :class:`BundleValidationResult` for the C5 preflight path.

    Preflight's aggregate :class:`LHPError` carries a structured
    ``context`` mapping per the internal ``_PreflightFailures`` shape;
    the human findings live in ``exc.details`` (already formatted by
    ``_build_aggregated_error``). We expose them as the frozen
    ``issues`` tuple while still surfacing the LHP code on the DTO.
    """
    from lhp.errors import LHPError

    if exc is None:
        return BundleValidationResult(
            success=True,
            issues=tuple(issues),
        )
    code, message = _lhp_error_code_and_message(exc)
    derived_issues: tuple[str, ...] = tuple(issues)
    if not derived_issues and isinstance(exc, LHPError) and exc.details:
        derived_issues = tuple(
            line for line in exc.details.splitlines() if line.strip()
        )
    return BundleValidationResult(
        success=False,
        issues=derived_issues,
        error_message=message,
        error_code=code,
    )


def _bundle_enable_to_result(
    *,
    target_dir: Path,
    created_files: Sequence[Path] = (),
    created_dirs: Sequence[Path] = (),
    exc: Optional[BaseException] = None,
) -> BundleEnableResult:
    """Build a :class:`BundleEnableResult` for the C5 enable_bundle path."""
    if exc is None:
        return BundleEnableResult(
            success=True,
            target_dir=target_dir,
            created_files=tuple(created_files),
            created_dirs=tuple(created_dirs),
        )
    code, message = _lhp_error_code_and_message(exc)
    return BundleEnableResult(
        success=False,
        target_dir=target_dir,
        created_files=tuple(created_files),
        created_dirs=tuple(created_dirs),
        error_message=message,
        error_code=code,
    )


def _wipe_resources_lhp(project_root: Path) -> int:
    """Wipe ``resources/lhp/`` and return the number of files removed.

    The wipe-and-regenerate contract on :class:`BundleManager` requires
    callers to clear ``resources/lhp/`` before sync. Centralising the
    wipe here keeps :class:`BundleFacade.sync_resources` lean and the
    contract enforced for non-CLI callers. Refuses to follow a symlink
    (matches the CLI's existing safety check).
    """
    import shutil

    from lhp.bundle.exceptions import BundleResourceError

    resources_lhp = project_root / "resources" / "lhp"
    if resources_lhp.is_symlink():
        raise BundleResourceError(
            f"resources/lhp is a symlink; refusing to delete: {resources_lhp}. "
            f"Remove the symlink and let LHP manage this directory directly."
        )
    deleted_count = 0
    if resources_lhp.exists():
        deleted_count = sum(1 for _ in resources_lhp.rglob("*") if _.is_file())
        shutil.rmtree(resources_lhp)
    resources_lhp.mkdir(parents=True, exist_ok=True)
    return deleted_count


def _missing_pipeline_config_error() -> LHPConfigError:
    """Build the structured ``LHP-CFG-023`` for missing pipeline_config.

    Mirrors the CLI's ``--pipeline-config`` requirement (the same
    ``LHP-CFG-023`` raised inline by the ``generate``/``validate`` commands)
    so :meth:`BundleFacade.validate_bundle_assets` can surface a stable
    error code when invoked without a configured ``pipeline_config_path``.
    """
    from lhp.errors import ErrorFactory, codes

    return ErrorFactory.config_error(
        codes.CFG_023,
        title="pipeline_config is required for bundle validation",
        details=(
            "Bundle preflight requires a pipeline_config.yaml that defines "
            "`catalog` and `schema` either per-pipeline or under "
            "`project_defaults`, but no pipeline_config_path was configured "
            "on the facade."
        ),
        suggestions=[
            "Pass pipeline_config_path to LakehousePlumberApplicationFacade.for_project",
            "Or use the CLI which threads --pipeline-config through automatically",
        ],
    )


def _scaffold_bundle_assets(
    target_dir: Path,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    """Scaffold ``databricks.yml`` + ``resources/lhp/`` into ``target_dir``.

    One-time scaffolding for an existing LHP project: writes
    ``databricks.yml`` and the ``resources/lhp/`` directory layout the
    bundle sync expects. Returns ``(created_files, created_dirs)`` —
    diffed against the pre-call filesystem state so callers see only
    new entries.

    Safe to invoke repeatedly: existing ``databricks.yml`` /
    directories are preserved, and the returned tuples contain only
    entries this call actually created.
    """
    from lhp.core.loaders.init_template_context import InitTemplateContext
    from lhp.core.loaders.init_template_loader import InitTemplateLoader

    target_dir.mkdir(parents=True, exist_ok=True)
    pre_files = {p.resolve() for p in target_dir.rglob("*") if p.is_file()}
    pre_dirs = {p.resolve() for p in target_dir.rglob("*") if p.is_dir()}

    for relative in ("resources", "resources/lhp"):
        (target_dir / relative).mkdir(parents=True, exist_ok=True)

    databricks_yml = target_dir / "databricks.yml"
    if not databricks_yml.exists():
        loader = InitTemplateLoader()
        context = InitTemplateContext.create(
            project_name=target_dir.name,
            bundle_enabled=True,
            author="",
        )
        content = loader.render_template("bundle/databricks.yml.j2", context)
        databricks_yml.write_text(content, encoding="utf-8")

    post_files = {p.resolve() for p in target_dir.rglob("*") if p.is_file()}
    post_dirs = {p.resolve() for p in target_dir.rglob("*") if p.is_dir()}
    created_files = tuple(sorted(post_files - pre_files))
    created_dirs = tuple(sorted(post_dirs - pre_dirs))
    return created_files, created_dirs


class BundleFacade:
    """Databricks Asset Bundle operations on a constructed project.

    Three public methods:

    - ``sync_resources`` — write per-pipeline resource YAML after a
      successful generation; optionally wipe ``resources/lhp/`` first.
    - ``validate_bundle_assets`` — preflight catalog/schema validation
      across all bundle pipelines.
    - ``enable_bundle`` — one-time scaffolding to flip an existing
      project to bundle mode (creates ``databricks.yml`` + ``bundle/``).

    All return frozen DTOs from :mod:`lhp.api.responses`. Failures are
    captured and surfaced on the DTO (``success=False`` plus
    ``error_message`` / ``error_code``) rather than raised, per §4.8.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def sync_resources(
        self, env: str, output_dir: Path, *, wipe: bool = False
    ) -> Iterator[LHPEvent]:
        """Stream-protocol wrapper around bundle resource sync.

        Yields :class:`OperationStarted` first, then on success exactly
        one :class:`BundleSyncCompleted` carrying the
        :class:`BundleSyncResult`. If the underlying bundle manager
        raises an :class:`LHPError`, an :class:`ErrorEmitted` is
        yielded and the error is re-raised (constitution §1.4, §5.7).

        Callers that don't need event-level visibility use
        :func:`lhp.api.collect_response` to walk the stream and return
        the terminal response DTO.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-BND-*`` from
            :class:`BundleResourceError` / :class:`BundleConfigurationError`
            (resource sync, bundle manifest), plus ``LHP-CFG-*`` and
            ``LHP-VAL-*`` propagated from the underlying bundle manager.
            An :class:`ErrorEmitted` event is yielded before the
            exception escapes (§1.4 stream protocol).
        """
        yield OperationStarted(operation_name="sync_resources", env=env)
        try:
            response = self._do_sync_resources(env, output_dir, wipe=wipe)
        except LHPError as exc:
            yield ErrorEmitted(lhp_error=exc)
            raise
        yield BundleSyncCompleted(response=response)

    def _do_sync_resources(
        self, env: str, output_dir: Path, *, wipe: bool = False
    ) -> BundleSyncResult:
        """Sync resource files for a generated project into the Databricks Asset Bundle.

        Wraps :meth:`BundleManager.sync_resources_with_generated_files`.
        When ``wipe=True`` the ``resources/lhp/`` directory is cleared
        before the sync (the wipe-and-regenerate contract from
        :class:`BundleManager` is honored here so non-CLI callers do
        not have to know about that invariant).

        Internal: invoked by the public generator wrapper
        :meth:`sync_resources`. The graceful-DTO failure path (catch
        ``Exception`` → return DTO with ``error_code``) is intentional
        and orthogonal to the stream-protocol ``ErrorEmitted``
        rendezvous, which is reserved for genuinely catastrophic
        structured failures that this body does not choose to swallow.
        """
        from lhp.bundle.manager import BundleManager

        project_root = self._orchestrator.project_root
        deleted_count = 0
        synced_count = 0
        try:
            with perf_timer("facade.sync_resources"):
                if wipe:
                    deleted_count = _wipe_resources_lhp(project_root)
                bundle_manager = BundleManager(
                    project_root,
                    self._orchestrator.pipeline_config_path,
                    project_config=self._orchestrator.project_config,
                )
                synced_count = bundle_manager.sync_resources_with_generated_files(
                    output_dir, env
                )
            return _bundle_sync_to_result(
                synced_file_count=synced_count,
                deleted_file_count=deleted_count,
                bundle_path=project_root / "resources" / "lhp",
            )
        except Exception as exc:
            if isinstance(exc, LHPError):
                self._logger.debug(f"Bundle sync failed: {exc.code}")
            else:
                self._logger.exception("Bundle sync failed")
            return _bundle_sync_to_result(
                synced_file_count=synced_count,
                deleted_file_count=deleted_count,
                bundle_path=project_root / "resources" / "lhp",
                exc=exc,
            )

    def validate_bundle_assets(self, env: str) -> BundleValidationResult:
        """Validate the bundle's assets are well-formed and consistent.

        Wraps the preflight checks in :mod:`lhp.bundle.preflight`:
        ensures every pipeline declares a resolvable
        ``catalog``/``schema`` after substitution. Failures are
        aggregated and surfaced as a frozen :class:`BundleValidationResult`
        (``success=False`` + ``issues`` tuple + ``error_code``) rather
        than raising.

        :stability: provisional
        :raises: None — failures (including ``LHP-BND-*`` and unexpected
            exceptions) are surfaced on the result's ``error_code`` and
            ``issues`` fields (§4.8).
        """
        from lhp.bundle.preflight import validate_catalog_schema
        from lhp.core.coordination.monitoring_pipeline_builder import (
            resolve_monitoring_pipeline_name,
        )

        project_root = self._orchestrator.project_root
        pipeline_config_path = self._orchestrator.pipeline_config_path
        project_config = self._orchestrator.project_config
        if pipeline_config_path is None:
            return _bundle_validate_to_result(
                exc=_missing_pipeline_config_error(),
            )
        try:
            with perf_timer("facade.validate_bundle_assets"):
                flowgroups: Sequence["FlowGroup"] = (
                    self._orchestrator.bootstrap.discover_all_flowgroups()
                )
                pipeline_names = sorted({fg.pipeline for fg in flowgroups})
                monitoring_name: Optional[str] = (
                    resolve_monitoring_pipeline_name(project_config)
                    if project_config is not None
                    else None
                )
                validate_catalog_schema(
                    project_root=project_root,
                    pipeline_config_path=pipeline_config_path,
                    pipeline_names=pipeline_names,
                    env=env,
                    monitoring_pipeline_name=monitoring_name,
                )
            return _bundle_validate_to_result()
        except Exception as exc:
            from lhp.errors import LHPError

            if isinstance(exc, LHPError):
                self._logger.debug(f"Bundle validation failed: {exc.code}")
            else:
                self._logger.exception("Bundle validation failed")
            return _bundle_validate_to_result(exc=exc)

    def enable_bundle(self, target_dir: Optional[Path] = None) -> BundleEnableResult:
        """Enable Databricks Asset Bundle support on an existing project.

        Creates ``databricks.yml`` and the ``bundle/`` resource directory.
        One-time scaffolding — distinct from :meth:`sync_resources` which
        runs after every generation.

        ``target_dir`` defaults to the project root of the facade's
        orchestrator. If supplied, scaffolding is performed at that
        location (used primarily by tests and by callers that have not
        yet constructed an orchestrator for the project).

        :stability: provisional
        :raises: None — failures (``LHP-BND-*``, ``LHP-FILE-*``, or
            unexpected exceptions during scaffolding) are surfaced on
            the result's ``error_code`` field (§4.8).
        """
        resolved_target = (
            target_dir if target_dir is not None else self._orchestrator.project_root
        ).resolve()
        try:
            with perf_timer("facade.enable_bundle"):
                created_files, created_dirs = _scaffold_bundle_assets(resolved_target)
            return _bundle_enable_to_result(
                target_dir=resolved_target,
                created_files=created_files,
                created_dirs=created_dirs,
            )
        except Exception as exc:
            from lhp.errors import LHPError

            if isinstance(exc, LHPError):
                self._logger.debug(f"Bundle enable failed: {exc.code}")
            else:
                self._logger.exception("Bundle enable failed")
            return _bundle_enable_to_result(
                target_dir=resolved_target,
                exc=exc,
            )
