"""Load the personal sandbox profile from gitignored ``.lhp/profile.yaml``.

Sandbox mode is explicit opt-in: each developer declares their ``namespace``
and the ``pipelines`` they work on in a personal, gitignored
``.lhp/profile.yaml`` at the project root. Nothing is auto-detected.
"""

import logging
from pathlib import Path

from pydantic import ValidationError

from lhp.errors import ErrorFactory, LHPError, codes
from lhp.models import SandboxProfile

logger = logging.getLogger(__name__)

_PROFILE_EXAMPLE = """sandbox:
  namespace: alice
  pipelines:
    - acmi_edw_bronze
    - acmi_edw_silv*"""


def load_sandbox_profile(project_root: Path) -> SandboxProfile:
    """Load and validate the personal sandbox profile.

    Raises:
        LHPError: ``LHP-IO-025`` when ``.lhp/profile.yaml`` does not exist;
            ``LHP-CFG-064`` when the file is unreadable, is not valid YAML,
            has a non-mapping root, lacks the ``sandbox:`` key, or fails
            :class:`SandboxProfile` validation.
    """
    profile_path = project_root / ".lhp" / "profile.yaml"

    if not profile_path.exists():
        raise ErrorFactory.io_error(
            codes.IO_025,
            title="Sandbox profile not found",
            details=(
                f"Sandbox mode requires a personal profile at: {profile_path}\n"
                "This gitignored file declares your namespace and the "
                "pipelines you want to generate in sandbox mode."
            ),
            suggestions=[
                "Create .lhp/profile.yaml in the project root (the .lhp/ directory is gitignored)",
                "Set 'namespace' to your personal identifier (lowercase letter first, then lowercase letters, digits, underscores; max 64 chars)",
                "List the pipelines you work on under 'pipelines' (glob patterns allowed)",
            ],
            example=_PROFILE_EXAMPLE,
            context={"Expected Path": str(profile_path)},
        )

    logger.debug(f"Loading sandbox profile from: {profile_path}")

    from ...parsers.yaml_loader import load_yaml_file

    try:
        data = load_yaml_file(profile_path, error_context="sandbox profile")
    except LHPError as e:
        raise ErrorFactory.config_error(
            codes.CFG_064,
            title="Invalid sandbox profile",
            details=f"Could not read sandbox profile '{profile_path}': {e.details}",
            suggestions=[
                "Check YAML syntax (indentation, colons, dashes)",
                "The file must contain a single YAML document",
            ],
            example=_PROFILE_EXAMPLE,
            context={"File": str(profile_path)},
        ) from e

    if not isinstance(data, dict):
        raise ErrorFactory.config_error(
            codes.CFG_064,
            title="Invalid sandbox profile",
            details=(
                f"Sandbox profile '{profile_path}' must be a YAML mapping, "
                f"got {type(data).__name__}"
            ),
            suggestions=[
                "The profile root must be a mapping with a top-level 'sandbox' key"
            ],
            example=_PROFILE_EXAMPLE,
            context={"File": str(profile_path)},
        )

    if "sandbox" not in data:
        raise ErrorFactory.config_error(
            codes.CFG_064,
            title="Invalid sandbox profile",
            details=(
                f"Sandbox profile '{profile_path}' is missing the top-level "
                "'sandbox' key"
            ),
            suggestions=[
                "Nest 'namespace' and 'pipelines' under a top-level 'sandbox' key"
            ],
            example=_PROFILE_EXAMPLE,
            context={"File": str(profile_path)},
        )

    try:
        profile = SandboxProfile.model_validate(data["sandbox"])
    except ValidationError as e:
        raise ErrorFactory.config_error(
            codes.CFG_064,
            title="Invalid sandbox profile",
            details=f"Sandbox profile '{profile_path}' failed validation: {e}",
            suggestions=[
                "namespace must start with a lowercase letter and contain only lowercase letters, digits, and underscores (max 64 chars)",
                "pipelines must be a non-empty list of pipeline names or glob patterns",
            ],
            example=_PROFILE_EXAMPLE,
            context={"File": str(profile_path)},
        ) from e

    logger.debug(
        f"Loaded sandbox profile: namespace={profile.namespace}, "
        f"pipelines={profile.pipelines}"
    )
    return profile
