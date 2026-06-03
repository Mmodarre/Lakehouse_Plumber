"""Shared project-preflight checks for the validate and generate paths.

§9.24 — single source of project-preflight logic. Both
:meth:`ValidationFacade.validate_pipelines` and
:meth:`GenerationFacade.generate_pipelines` compose their project-level
preflight from :func:`_run_project_preflight`; the two paths differ ONLY
in how they SURFACE the returned issues (validate aggregates them onto a
non-zero-exit batch DTO; generate raises them into the generation pool).
The checks themselves live here exactly once.

:stability: internal
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Tuple

from lhp.api._converters_common import _lhp_error_to_issue_view
from lhp.api.views import ValidationIssueView
from lhp.errors import ErrorCategory, LHPConfigError

if TYPE_CHECKING:
    from lhp.models import FlowGroup

logger = logging.getLogger(__name__)

# CONFIG-category code carried by test-reporting preflight failures.
# ``TestReportingHookGenerator.validate`` returns plain message strings with
# no structured code, so the code is assigned here. This is the canonical,
# pre-existing owner of LHP-CFG-032; the generated-code formatter codes
# (CFG-031 parse guard, CFG-033 ruff-format exit, CFG-034 ruff-not-found)
# deliberately skip 032 so test-reporting keeps sole ownership of it.
_TEST_REPORTING_CODE_NUMBER = "032"


def _run_project_preflight(
    orchestrator: Any,
    *,
    env: str,
    bundle_enabled: bool,
    include_tests: bool,
    pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
) -> Tuple[ValidationIssueView, ...]:
    """Run the shared project-level preflight checks; return issues, never raise.

    Composes three backend checks, each of which contributes zero or more
    :class:`ValidationIssueView` entries to the returned tuple:

    1. **Duplicate (pipeline, flowgroup)** — via the list-returning
       ``ConfigValidator.validate_duplicate_pipeline_flowgroup`` surface;
       a non-empty result yields one ``LHP-VAL-009`` issue.
    2. **Test-reporting file existence** — via
       :meth:`TestReportingHookGenerator.validate`. The file-existence
       portion (missing provider module / config file) fires INDEPENDENT
       of ``include_tests``; the ``include_tests``-gated portion adds the
       "no ``test_id``" check. Each message becomes a
       ``LHP-CFG-032`` issue.
    3. **Bundle catalog/schema preflight** — ONLY when ``bundle_enabled``;
       via :meth:`BundleFacade.validate_bundle_assets`. Each failure
       becomes an ``LHP-CFG-026`` issue.

    The flowgroup set used by checks (1) and (2) is resolved EXACTLY ONCE
    here — from ``pre_discovered_all_flowgroups`` when the caller supplies
    it, otherwise from ``orchestrator.bootstrap.discover_all_flowgroups()`` — and the
    same resolved list is threaded into both, so neither re-discovers
    independently. The bundle check (3) does not consume flowgroups.

    The function NEVER raises: every backend failure — including a backend
    that itself raises — is converted to an issue in the returned tuple.
    Validate and generate differ only in how they SURFACE these issues
    (§9.24).
    """
    issues: List[ValidationIssueView] = []

    all_flowgroups: List["FlowGroup"] = (
        list(pre_discovered_all_flowgroups)
        if pre_discovered_all_flowgroups is not None
        else orchestrator.bootstrap.discover_all_flowgroups()
    )

    issues.extend(_check_duplicate_flowgroups(orchestrator, all_flowgroups))
    issues.extend(
        _check_test_reporting(orchestrator, all_flowgroups, include_tests=include_tests)
    )
    if bundle_enabled:
        issues.extend(_check_bundle_assets(orchestrator, env=env))

    return tuple(issues)


def _check_duplicate_flowgroups(
    orchestrator: Any,
    all_flowgroups: List["FlowGroup"],
) -> List[ValidationIssueView]:
    # §9.24: routes through ValidationService.build_duplicate_issue — single
    # source of LHP-VAL-009 — rather than re-building the error here.
    try:
        err = orchestrator.validation.build_duplicate_issue(all_flowgroups)
    except Exception:
        logger.exception("Duplicate pipeline+flowgroup preflight failed")
        return []

    if err is None:
        return []

    return [_lhp_error_to_issue_view(err)]


def _check_test_reporting(
    orchestrator: Any,
    all_flowgroups: List["FlowGroup"],
    *,
    include_tests: bool,
) -> List[ValidationIssueView]:
    # File-existence checks run regardless of ``include_tests``; ``include_tests``
    # only gates the "no ``test_id``" check inside TestReportingHookGenerator.validate.
    from lhp.core.codegen import TestReportingHookGenerator

    try:
        project_config = orchestrator.project_config
        if project_config is None:
            return []
        generator = TestReportingHookGenerator(
            project_config, orchestrator.project_root
        )
        messages: List[str] = generator.validate(
            processed_flowgroups=all_flowgroups,
            include_tests=include_tests,
        )
    except Exception:
        logger.exception("Test-reporting preflight failed")
        return []

    return [
        _lhp_error_to_issue_view(
            LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number=_TEST_REPORTING_CODE_NUMBER,
                title="Test reporting configuration is invalid",
                details=message,
                suggestions=[
                    "Ensure the test_reporting.module_path file exists",
                    "Ensure the test_reporting.config_file (if set) exists",
                    "Set test_id on at least one test action when using "
                    "--include-tests",
                ],
            )
        )
        for message in messages
    ]


def _check_bundle_assets(
    orchestrator: Any,
    *,
    env: str,
) -> List[ValidationIssueView]:
    # ``BundleValidationResult`` carries no ``doc_link`` (§4.8 — no live LHPError
    # retained), so the catalog/schema slug is re-attached here for LHP-CFG-026,
    # single-sourced from lhp.bundle.preflight._CATALOG_SCHEMA_DOC_LINK.
    from lhp.api._bundle_facade import BundleFacade
    from lhp.bundle.preflight import _CATALOG_SCHEMA_DOC_LINK

    try:
        result = BundleFacade(orchestrator).validate_bundle_assets(env)
    except Exception:
        logger.exception("Bundle catalog/schema preflight failed")
        return []

    if result.success:
        return []

    # ``error_code`` is the fully-qualified ``LHP-CFG-026`` (or another
    # ``LHP-...`` code on unexpected failure); strip the ``LHP-<CAT>-``
    # prefix back to the bare code_number that ``LHPConfigError`` expects.
    code_number = _code_number_from_full_code(result.error_code)
    detail_lines = list(result.issues)
    if result.error_message:
        detail_lines.insert(0, result.error_message)
    details = "\n".join(detail_lines) if detail_lines else "Bundle validation failed"

    # Only the catalog/schema failure (``LHP-CFG-026``) has a doc page;
    # the slug is the same constant the bundle layer attaches to its own
    # aggregated error. Leave ``doc_link`` unset for any other code.
    doc_link = _CATALOG_SCHEMA_DOC_LINK if code_number == "026" else None

    return [
        _lhp_error_to_issue_view(
            LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number=code_number,
                title="Bundle catalog/schema validation failed",
                details=details,
                suggestions=[
                    "Define `catalog` and `schema` under `project_defaults` "
                    "in pipeline_config.yaml",
                    "Or define them per-pipeline (overrides project_defaults)",
                ],
                doc_link=doc_link,
            )
        )
    ]


def _code_number_from_full_code(full_code: str | None) -> str:
    # ``"LHP-CFG-026"`` → ``"026"``. Falls back to ``"026"`` — bundle catalog/schema
    # is the only failure mode that reaches here.
    if not full_code:
        return "026"
    parts = full_code.split("-")
    return parts[-1] if parts else "026"
