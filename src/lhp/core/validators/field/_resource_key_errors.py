"""``LHP-CFG-072`` builders shared by the pipeline and job resource-key checks (#351)."""

from typing import Any, Dict, List, Optional

from lhp.errors import ErrorFactory, LHPConfigError, codes


def owned_key_error(
    config_kind: str,
    key: str,
    reason: str,
    fix: str,
    job_name: Optional[str] = None,
) -> LHPConfigError:
    """Build the error for a key LHP writes itself and the user may not set."""
    context: Dict[str, Any] = {"Key": key}
    if job_name is not None:
        context["Job"] = job_name
    return resource_key_error(
        title=f"'{key}' cannot be set in {config_kind}",
        details=f"'{key}' is set by LHP and cannot be overridden. {reason}",
        suggestions=[fix],
        context=context,
    )


def resource_key_error(
    *,
    title: str,
    details: str,
    suggestions: List[str],
    example: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> LHPConfigError:
    """Build an ``LHP-CFG-072`` error."""
    return ErrorFactory.config_error(
        codes.CFG_072,
        title=title,
        details=details,
        suggestions=suggestions,
        example=example,
        context=context,
    )
