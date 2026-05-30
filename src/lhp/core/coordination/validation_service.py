"""Single external validation surface (§9.24).

Composes :class:`ConfigValidator` (``core/validators/config_validator.py``)
and the action/compatibility validators under ``core/validators/``. Per
constitution §9.24, every external caller — :class:`ActionOrchestrator`,
the CLI via the application facade, and worker subprocesses — must route
through this class. No other module outside ``core/validators/`` may
import those modules directly.

This class is the sole composer of the cross-flowgroup validators
(:class:`TableCreationValidator`,
:class:`CdcFanInCompatibilityValidator`). :class:`ConfigValidator` sees
one flowgroup at a time and therefore does not — and must not — delegate
to them.

Caller contract:

* MAY call :meth:`validate_duplicates` and :meth:`validate_cross_flowgroup`.
* MAY NOT import anything from ``lhp.core.validators/*`` (including
  ``ConfigValidator``) directly.

The class is picklable so :class:`concurrent.futures.ProcessPoolExecutor`
workers can carry an instance across the ``spawn`` boundary.

:stability: provisional
"""

from pathlib import Path
from typing import List, Optional, Sequence

from lhp.core._interfaces import (
    BaseValidationService,
    CrossFlowgroupCheckResult,
)
from lhp.core.validators import ConfigValidator
from lhp.core.validators.cdc_fanin_compatibility_validator import (
    CdcFanInCompatibilityValidator,
)
from lhp.core.validators.load_validator import LoadActionValidator
from lhp.core.validators.table_creation_validator import TableCreationValidator
from lhp.core.validators.test_validator import TestActionValidator
from lhp.core.validators.transform_validator import TransformActionValidator
from lhp.core.validators.write_validator import WriteActionValidator
from lhp.errors import ErrorCategory, LHPError, LHPValidationError
from lhp.models import FlowGroup, ProjectConfig


class ValidationService(BaseValidationService):
    """Single external validation surface.

    Wraps :class:`ConfigValidator` plus every action/compatibility validator
    in ``core/validators/`` behind a small public surface. External callers
    MUST go through this class — per §9.24, direct imports of
    ``core/validators/*`` from anywhere else are prohibited.

    Cross-flowgroup validators (:class:`TableCreationValidator`,
    :class:`CdcFanInCompatibilityValidator`) are composed exclusively here.
    The action validator class slots held in :meth:`__init__` exist so
    external code never needs to import those class names directly —
    :class:`ConfigValidator` composes the live action validator instances
    with its own constructor args.

    :stability: provisional
    """

    def __init__(
        self,
        project_root: Path,
        project_config: Optional[ProjectConfig] = None,
        *,
        config_validator: Optional[ConfigValidator] = None,
    ) -> None:
        """Initialize the validation service.

        Args:
            project_root: Project root directory; forwarded to
                :class:`ConfigValidator` when one is constructed internally.
            project_config: Loaded project config, or ``None`` for early
                bootstrap paths that pre-date config loading (matches
                :class:`ConfigValidator`'s tolerance for ``None``).
            config_validator: Optional pre-built :class:`ConfigValidator`
                injected by :meth:`LakehousePlumberApplicationFacade.for_project`
                so the same instance is shared with
                :class:`FlowgroupResolutionService` (closes the §9.24
                ``self.validation._config_validator`` reach-through on the
                orchestrator construction path). When ``None`` (legacy
                direct callers), a fresh validator is constructed here.
        """
        self._project_root = project_root
        self._project_config = project_config

        # Primary delegate. ConfigValidator's __init__ wires up every
        # action validator internally, so per-flowgroup dispatch flows
        # through it rather than duplicating the type-keyed branching here.
        if config_validator is not None:
            self._config_validator = config_validator
        else:
            self._config_validator = ConfigValidator(project_root, project_config)

        # Action validator class slots — held only so external callers never
        # need to import these names directly. ConfigValidator composes the
        # live instances with its own constructor args (registries, field
        # validators, project paths).
        self._load_validator_cls = LoadActionValidator
        self._transform_validator_cls = TransformActionValidator
        self._write_validator_cls = WriteActionValidator
        self._test_validator_cls = TestActionValidator

        # Cross-flowgroup validators — single composition site (§9.24).
        self._table_creation_validator = TableCreationValidator()
        self._fanin_validator = CdcFanInCompatibilityValidator()

    def build_duplicate_issue(
        self, flowgroups: Sequence[FlowGroup]
    ) -> Optional[LHPError]:
        """Build the ``LHP-VAL-009`` duplicate-pair error, or ``None`` if clean.

        SINGLE source of the ``LHP-VAL-009`` "Duplicate pipeline+flowgroup
        combinations found" construction (§9.24). Runs detection via the
        list-returning :meth:`ConfigValidator.validate_duplicate_pipeline_flowgroup`
        surface and, when it reports duplicates, BUILDS and RETURNS the
        :class:`LHPValidationError`; returns ``None`` when no duplicates exist.

        Mirrors :func:`lhp.core.coordination._cross_flowgroup_issues.build_cross_flowgroup_issues`:
        construction lives here, and callers decide how to surface the result
        (:meth:`validate_duplicates` raises it; the preflight path folds it
        into an issue list).
        """
        errors = self._config_validator.validate_duplicate_pipeline_flowgroup(
            list(flowgroups)
        )
        if not errors:
            return None
        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="009",
            title="Duplicate pipeline+flowgroup combinations found",
            details="Duplicate pipeline+flowgroup combinations found:\n"
            + "\n".join(f"  - {e}" for e in errors),
            suggestions=[
                "Ensure each pipeline+flowgroup combination is unique",
                "Check for duplicate flowgroup names within the same pipeline",
                "Rename one of the duplicate flowgroups",
            ],
            context={"Duplicates": len(errors)},
        )

    def validate_duplicates(self, flowgroups: Sequence[FlowGroup]) -> None:
        """Raise :class:`LHPValidationError` if duplicate pipeline+flowgroup pairs exist.

        Mirrors :meth:`ActionOrchestrator.validate_duplicate_pipeline_flowgroup_combinations`
        verbatim — same error code, title, suggestions, and context — so the
        orchestrator pass-through is a one-line delegation. The error itself is
        BUILT by :meth:`build_duplicate_issue` (§9.24: single construction
        site); this method only re-raises it.
        """
        err = self.build_duplicate_issue(flowgroups)
        if err is not None:
            raise err

    def validate_cross_flowgroup(
        self,
        flowgroups: Sequence[FlowGroup],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> CrossFlowgroupCheckResult:
        """Run only the cross-flowgroup compatibility checks.

        Runs the :class:`TableCreationValidator` and
        :class:`CdcFanInCompatibilityValidator` only. The per-flowgroup
        :class:`ConfigValidator` pass is intentionally omitted so the
        per-pipeline generation worker (:class:`PipelineProcessor`) sees
        the same validation footprint it had before §9.24 routing —
        per-flowgroup validation is already handled by
        :class:`FlowgroupResolutionService` during Phase A.

        LHPError raised by the underlying validators is NOT caught here;
        it propagates so the worker's catch-and-convert boundary can
        translate it into a :class:`PipelineDelta` failure (§5.6).

        Args:
            flowgroups: Flowgroups to check (typically a single
                pipeline's set).
            pipeline_filter: If supplied, restrict checks to flowgroups
                whose ``pipeline`` field equals this value.

        Returns:
            :class:`CrossFlowgroupCheckResult` with separate
            ``table_creation_errors`` and ``cdc_fanin_errors`` so the
            caller can re-raise with the right error code/category.
        """
        target_flowgroups: List[FlowGroup] = (
            [fg for fg in flowgroups if fg.pipeline == pipeline_filter]
            if pipeline_filter is not None
            else list(flowgroups)
        )

        table_errors = list(self._table_creation_validator.validate(target_flowgroups))
        cdc_errors = list(self._fanin_validator.validate(target_flowgroups))

        return CrossFlowgroupCheckResult(
            table_creation_errors=table_errors,
            cdc_fanin_errors=cdc_errors,
        )

