"""Application facade — single entry point for LHP runtime operations.

Composes five sub-facades (generation, validation, inspection, bundle,
wheel) that group related operations. Constructed exclusively via
:meth:`LakehousePlumberApplicationFacade.for_project`; the composition
root lives in :mod:`lhp.core.coordination.layers`.

:stability: provisional
"""

from __future__ import annotations

from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    List,
    Optional,
    Sequence,
)

from lhp.api._bundle_facade import BundleFacade as BundleFacade  # re-export (§1.10)
from lhp.api._generation_facade import (
    GenerationFacade as GenerationFacade,  # re-export (§1.10)
)
from lhp.api._inspection_facade import (
    InspectionFacade as InspectionFacade,  # re-export (§1.10)
)
from lhp.api._progress import ProgressSink
from lhp.api._validation_facade import (
    ValidationFacade as ValidationFacade,  # re-export (§1.10)
)
from lhp.api._wheel_facade import WheelFacade as WheelFacade  # re-export (§1.10)
from lhp.api.events import LHPEvent

if TYPE_CHECKING:
    from lhp.models import FlowGroup

    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10,
    # §9.13). Composition is delegated to ``core/coordination/layers``.
    _Orchestrator = Any


class LakehousePlumberApplicationFacade:
    """Top-level application facade.

    Composes five sub-facades that group related operations:

    - ``generation`` — batch generation runs.
    - ``validation`` — config validation.
    - ``inspection`` — read-only project introspection.
    - ``bundle`` — Asset Bundle operations.
    - ``wheel`` — built-wheel inspection / extraction.

    Constructed exclusively via :meth:`for_project`. The bare
    ``__init__(...)`` form is internal — external callers must route
    through the classmethod so the composition-root logic (single
    ``ConfigValidator``, threaded collaborators) is enforced (§4.5).

    The public ``orchestrator`` attribute is intentionally absent
    (§1.10, §9.23); callers must use the sub-facade methods.

    :stability: stable
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self.generation = GenerationFacade(orchestrator)
        self.validation = ValidationFacade(orchestrator)
        self.inspection = InspectionFacade(orchestrator)
        self.bundle = BundleFacade(orchestrator)
        self.wheel = WheelFacade(orchestrator)

    @classmethod
    def for_project(
        cls,
        project_root: Path,
        *,
        pipeline_config_path: Optional[str] = None,
        enforce_version: bool = True,
        max_workers: Optional[int] = None,
        no_cache: bool = False,
    ) -> "LakehousePlumberApplicationFacade":
        """Construct a fully-wired facade from a project root.

        Centralises service-graph construction so no service reaches
        into another's private attributes; builds the single
        ``ConfigValidator`` that threads through validation and
        flowgroup-resolution (closes the §9.24 leak).

        ``no_cache`` disables the persistent on-disk parse cache
        (``<project_root>/.lhp/cache/parse``) for this facade; the cache
        is also disabled when the ``LHP_NO_CACHE`` environment variable
        is truthy (``"1"``, ``"true"``, ``"yes"``). The cache is
        delete-safe: results are identical with or without it.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` if ``lhp.yaml`` is
            absent, malformed, or fails project-config validation;
            ``LHP-VAL-*`` if ``enforce_version`` is set and the
            ``lhp_version`` constraint rejects the installed package;
            ``LHP-FILE-*`` for missing-path conditions discovered
            during composition.
        """
        # Composition is delegated to an internal helper so the
        # orchestrator class name never appears in :mod:`lhp.api`
        # source (§1.10, §9.13). Lazy import.
        from lhp.core.coordination.layers import build_facade_orchestrator

        # Wire the per-action generators into the core ``ActionRegistry``
        # at this single composition point. ``core`` must not import
        # ``generators`` (layering), so registration is pushed from
        # ``api`` (the legal downward ``api -> generators`` edge). Lazy so
        # bare ``import lhp`` stays light; idempotent (later calls update).
        from lhp.generators.registration import register_all

        register_all()

        orchestrator = build_facade_orchestrator(
            project_root,
            pipeline_config_path=pipeline_config_path,
            enforce_version=enforce_version,
            max_workers=max_workers,
            no_cache=no_cache,
        )
        return cls(orchestrator)

    def generate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        apply_formatting: bool | None = None,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        max_workers: Optional[int] = None,
        progress: ProgressSink | None = None,
        sandbox: bool = False,
    ) -> Iterator[LHPEvent]:
        """Shortcut for ``self.generation.generate_pipelines(...)``.

        Restates the canonical signature (§4.2) and forwards every
        parameter unchanged via ``yield from`` (§1.4, §5.7).

        ``apply_formatting`` is the tri-state formatting override
        (``None`` = use the project's ``lhp.yaml`` ``apply_formatting``
        setting; ``True`` / ``False`` override it), resolved downstream
        in the orchestrator.

        ``progress`` is an optional :class:`~lhp.api.ProgressSink` the run
        advances as flowgroups complete; read its ``total`` / ``done``
        fields while iterating the stream to observe live progress.

        ``sandbox`` switches the run to developer-sandbox mode: scope and
        namespace come from the personal ``.lhp/profile.yaml`` plus the
        team ``sandbox:`` policy in ``lhp.yaml``. CANNOT be combined with
        ``pipeline_filter`` / ``pipeline_fields``.

        :stability: provisional
        :raises ValueError: if ``sandbox=True`` is combined with
            ``pipeline_filter`` or ``pipeline_fields`` (API misuse).
        :raises lhp.errors.LHPError: same families as
            :meth:`GenerationFacade.generate_pipelines` — ``LHP-VAL-*``,
            ``LHP-CFG-*``, ``LHP-FILE-*``, ``LHP-MULT-*``, ``LHP-TPL-*``;
            plus the sandbox preflight errors on a ``sandbox=True`` run —
            ``LHP-IO-025``, ``LHP-CFG-064``, ``LHP-CFG-065``,
            ``LHP-VAL-064``.
        """
        yield from self.generation.generate_pipelines(
            pipeline_filter=pipeline_filter,
            pipeline_fields=pipeline_fields,
            env=env,
            output_dir=output_dir,
            specific_flowgroups=specific_flowgroups,
            include_tests=include_tests,
            apply_formatting=apply_formatting,
            bundle_enabled=bundle_enabled,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            max_workers=max_workers,
            progress=progress,
            sandbox=sandbox,
        )

    def validate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        include_tests: bool = True,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        progress: ProgressSink | None = None,
        sandbox: bool = False,
    ) -> Iterator[LHPEvent]:
        """Shortcut for ``self.validation.validate_pipelines(...)``.

        Restates the canonical signature (§4.2) and forwards every
        parameter unchanged via ``yield from`` (§1.4, §5.7).

        ``progress`` is an optional :class:`~lhp.api.ProgressSink` the run
        advances as flowgroups complete; read its ``total`` / ``done``
        fields while iterating the stream to observe live progress.

        ``sandbox`` switches the run to developer-sandbox mode: scope and
        namespace come from the personal ``.lhp/profile.yaml`` plus the
        team ``sandbox:`` policy in ``lhp.yaml``. CANNOT be combined with
        ``pipeline_filter`` / ``pipeline_fields``.

        :stability: provisional
        :raises ValueError: if ``sandbox=True`` is combined with
            ``pipeline_filter`` or ``pipeline_fields`` (API misuse).
        :raises lhp.errors.LHPError: same families as
            :meth:`ValidationFacade.validate_pipelines` — ``LHP-VAL-*``,
            ``LHP-CFG-*``, ``LHP-FILE-*``, ``LHP-MULT-*``, ``LHP-TPL-*``;
            plus the sandbox preflight errors on a ``sandbox=True`` run —
            ``LHP-IO-025``, ``LHP-CFG-064``, ``LHP-CFG-065``,
            ``LHP-VAL-064``.
        """
        yield from self.validation.validate_pipelines(
            pipeline_filter=pipeline_filter,
            pipeline_fields=pipeline_fields,
            env=env,
            max_workers=max_workers,
            include_tests=include_tests,
            bundle_enabled=bundle_enabled,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            progress=progress,
            sandbox=sandbox,
        )
