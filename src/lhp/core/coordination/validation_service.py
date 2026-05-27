"""Single external validation surface (§9.24).

Composes :class:`ConfigValidator` (``core/validator.py``) and the
action/compatibility validators under ``core/validators/``. Per constitution
§9.24, after Week 1-2 every external caller — :class:`ActionOrchestrator`,
the CLI via the application facade, and worker subprocesses — must route
through this class. No other module outside ``core/validator.py`` and
``core/validators/`` may import those modules directly.

Internal duplication BETWEEN :class:`ConfigValidator` (which already
composes the action validators internally) and the standalone
``core/validators/`` classes (e.g. overlapping CDC fan-in checks) is a
known §9.24-INTERNAL violation. The external surface — this class — is
exactly one; untangling the internal redundancy is deferred to Week 3.

Caller contract:

* MAY call :meth:`validate_flowgroups`, :meth:`validate_duplicates`, and
  :meth:`validate_cross_flowgroup`.
* MAY NOT (after Phase D) import anything from ``lhp.core.validators/*``
  (including ``ConfigValidator``) directly.

The class is picklable so :class:`concurrent.futures.ProcessPoolExecutor`
workers can carry an instance across the ``spawn`` boundary.

:stability: provisional
"""

from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple

from lhp.api.views import ValidationIssueView
from lhp.core.coordination._interfaces import (
    BaseValidationService,
    CrossFlowgroupCheckResult,
)
from lhp.core.validators import ConfigValidator
from lhp.core.validators._base import ValidationError
from lhp.core.validators.cdc_fanin_compatibility_validator import (
    CdcFanInCompatibilityValidator,
)
from lhp.core.validators.load_validator import LoadActionValidator
from lhp.core.validators.table_creation_validator import TableCreationValidator
from lhp.core.validators.test_validator import TestActionValidator
from lhp.core.validators.transform_validator import TransformActionValidator
from lhp.core.validators.write_validator import WriteActionValidator
from lhp.models.config import FlowGroup, ProjectConfig
from lhp.errors import (
    ErrorCategory,
    LHPError,
    LHPValidationError,
)


class ValidationService(BaseValidationService):
    """Single external validation surface.

    Wraps :class:`ConfigValidator` plus every action/compatibility validator
    in ``core/validators/`` behind two methods. External callers MUST go
    through this class — per §9.24, direct imports of ``core/validator.py``
    or ``core/validators/*`` from anywhere else are prohibited after Phase D.

    The constructor holds explicit references to each action/compatibility
    validator class to make the §9.24 surface contract visible at the type
    level, even though :class:`ConfigValidator` already composes the
    per-flowgroup action validators internally. The duplication is a known
    §9.24-INTERNAL violation; see module docstring.

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

        # Held to make the §9.24 surface explicit at this layer. The first
        # four are also instantiated inside ConfigValidator with different
        # constructor args (registries, field validators, project paths);
        # this slot exists only so external code never needs to import the
        # class names directly.
        self._load_validator_cls = LoadActionValidator
        self._transform_validator_cls = TransformActionValidator
        self._write_validator_cls = WriteActionValidator
        self._test_validator_cls = TestActionValidator

        # Cross-flowgroup validators. Owned here AND inside ConfigValidator;
        # ConfigValidator's copies remain only because
        # validate_table_creation_rules / validate_cdc_fanin_compatibility
        # delegate to them.
        self._table_creation_validator = TableCreationValidator()
        self._fanin_validator = CdcFanInCompatibilityValidator()

    # --- BaseValidationService implementations ------------------------------

    def validate_flowgroups(
        self,
        flowgroups: Sequence[FlowGroup],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> Tuple[ValidationIssueView, ...]:
        """Return all validation issues across the given flowgroups.

        Composition order:

        1. Per-flowgroup checks via :meth:`ConfigValidator.validate_flowgroup`
           — which already runs the load/transform/write/test action
           validators internally.
        2. Cross-flowgroup compatibility checks via
           :class:`TableCreationValidator` and
           :class:`CdcFanInCompatibilityValidator` — invoked here directly
           rather than through ``ConfigValidator`` to keep the §9.24 surface
           obvious.
        3. Dedup workaround for §9.24-INTERNAL violations (see module
           docstring). Same (code, severity, title, details, pipeline_name)
           tuple appearing twice is collapsed to a single issue.

        Args:
            flowgroups: All flowgroups to validate.
            pipeline_filter: If supplied, restrict every check to flowgroups
                whose ``pipeline`` field equals this value.

        Returns:
            Deduplicated tuple of :class:`ValidationIssueView`. Empty
            tuple on success.
        """
        target_flowgroups: List[FlowGroup] = (
            [fg for fg in flowgroups if fg.pipeline == pipeline_filter]
            if pipeline_filter is not None
            else list(flowgroups)
        )

        issues: List[ValidationIssueView] = []

        # Per-flowgroup checks (also covers per-action validation
        # through ConfigValidator's internal dispatch).
        for fg in target_flowgroups:
            try:
                fg_errors = self._config_validator.validate_flowgroup(fg)
            except LHPError as exc:
                # ConfigValidator.validate_action re-raises LHPError from
                # field-validation (see validator.py:148). Wrap it as an
                # issue so the surface stays uniform.
                issues.append(self._issue_from_lhp_error(exc, location=fg.flowgroup))
                continue

            for err in fg_errors:
                issues.append(
                    self._issue_from_validation_error(err, location=fg.flowgroup)
                )

        # Cross-flowgroup checks (whole-set, after pipeline_filter).
        # Both validators return List[str]; CdcFanInCompatibilityValidator may
        # additionally raise LHPError for shared-field mismatches.
        try:
            table_errors = self._table_creation_validator.validate(target_flowgroups)
            for err in table_errors:
                issues.append(
                    self._issue_from_validation_error(
                        err, location=pipeline_filter
                    )
                )
        except LHPError as exc:
            issues.append(self._issue_from_lhp_error(exc, location=pipeline_filter))

        try:
            cdc_errors = self._fanin_validator.validate(target_flowgroups)
            for err in cdc_errors:
                issues.append(
                    self._issue_from_validation_error(
                        err, location=pipeline_filter
                    )
                )
        except LHPError as exc:
            issues.append(self._issue_from_lhp_error(exc, location=pipeline_filter))

        # Dedup workaround: once ConfigValidator stops delegating to
        # TableCreationValidator / CdcFanInCompatibilityValidator internally,
        # the dedup pass will become unnecessary and should be removed
        # alongside the duplicate composition.
        return self._dedupe_issues(issues)

    def validate_duplicates(self, flowgroups: Sequence[FlowGroup]) -> None:
        """Raise :class:`LHPValidationError` if duplicate pipeline+flowgroup pairs exist.

        Mirrors :meth:`ActionOrchestrator.validate_duplicate_pipeline_flowgroup_combinations`
        verbatim — same error code, title, suggestions, and context — so the
        Phase D orchestrator pass-through is a one-line delegation.
        """
        errors = self._config_validator.validate_duplicate_pipeline_flowgroup(
            list(flowgroups)
        )
        if errors:
            raise LHPValidationError(
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


    def validate_cross_flowgroup(
        self,
        flowgroups: Sequence[FlowGroup],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> CrossFlowgroupCheckResult:
        """Run only the cross-flowgroup compatibility checks.

        Narrower than :meth:`validate_flowgroups` — runs the
        :class:`TableCreationValidator` and
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

    # --- helpers ------------------------------------------------------------

    @staticmethod
    def _issue_from_validation_error(
        err: ValidationError, *, location: Optional[str]
    ) -> ValidationIssueView:
        """Adapt a ``Union[str, LHPError]`` validator output into a ``ValidationIssueView``."""
        if isinstance(err, LHPError):
            return ValidationService._issue_from_lhp_error(err, location=location)
        return ValidationIssueView(
            code="",
            category="VAL",
            severity="error",
            title=str(err),
            pipeline_name=location,
        )

    @staticmethod
    def _issue_from_lhp_error(
        exc: LHPError, *, location: Optional[str]
    ) -> ValidationIssueView:
        """Adapt a live :class:`LHPError` into a ``ValidationIssueView``.

        Decomposes the exception's payload into the flat fields of the
        frozen public view (§4.8) — the live exception instance is not
        retained.
        """
        return ValidationIssueView(
            code=exc.code,
            category=exc.category.value,
            severity="error",
            title=exc.title,
            details=exc.details or None,
            pipeline_name=location,
            flowgroup_name=None,
            suggestions=tuple(exc.suggestions or ()),
            context=dict(exc.context or {}),
            doc_link=exc.doc_link,
        )

    @staticmethod
    def _dedupe_issues(
        issues: Sequence[ValidationIssueView],
    ) -> Tuple[ValidationIssueView, ...]:
        """Collapse identical issues using a fully-qualified key tuple.

        The key includes ``code``, ``severity``, ``title``, ``details``,
        and ``pipeline_name`` — the identity-bearing subset of the
        :class:`ValidationIssueView` surface. ``suggestions``,
        ``context``, ``doc_link``, and ``flowgroup_name`` are incidental
        to identity and are dropped from the key; the first occurrence
        wins.
        """
        seen: Set[Tuple[str, str, str, Optional[str], Optional[str]]] = set()
        deduped: List[ValidationIssueView] = []
        for issue in issues:
            key = (
                issue.code,
                issue.severity,
                issue.title,
                issue.details,
                issue.pipeline_name,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(issue)
        return tuple(deduped)
