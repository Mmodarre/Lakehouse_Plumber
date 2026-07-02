"""Parse and validate ``sandbox`` section of lhp.yaml."""

import logging
from typing import Any

from pydantic import ValidationError

from lhp.errors import ErrorFactory, codes
from lhp.models import SandboxConfig

logger = logging.getLogger(__name__)


def parse_sandbox_config(sandbox_data: Any) -> SandboxConfig:
    """Parse and validate the ``sandbox`` mapping into a :class:`SandboxConfig`."""
    if not isinstance(sandbox_data, dict):
        raise ErrorFactory.config_error(
            codes.CFG_062,
            title="Invalid sandbox configuration",
            details=f"sandbox must be a mapping, got {type(sandbox_data).__name__}",
            suggestions=[
                "Define sandbox as a YAML mapping with keys: strategy, table_pattern, allowed_envs",
                'Example: sandbox:\\n  strategy: table\\n  table_pattern: "{namespace}_{table}"',
            ],
        )

    try:
        config = SandboxConfig(
            strategy=sandbox_data.get("strategy", "table"),
            table_pattern=sandbox_data.get("table_pattern", "{namespace}_{table}"),
            allowed_envs=sandbox_data.get("allowed_envs"),
        )
    except ValidationError as e:
        # CFG_063 is reserved for a bad table_pattern alone; if any OTHER
        # field also failed, the block as a whole is invalid -> CFG_062.
        pattern_errors = [
            err for err in e.errors() if err["loc"] and err["loc"][0] == "table_pattern"
        ]
        if pattern_errors and len(pattern_errors) == len(e.errors()):
            reasons = "; ".join(
                err["msg"].removeprefix("Value error, ") for err in pattern_errors
            )
            raise ErrorFactory.config_error(
                codes.CFG_063,
                title="Invalid sandbox table_pattern",
                details=(
                    f"table_pattern {sandbox_data.get('table_pattern')!r} is invalid: "
                    f"{reasons}"
                ),
                suggestions=[
                    "table_pattern must contain both {namespace} and {table}, with no conversions or format specs",
                    "Literal text between placeholders may only contain letters, digits, and underscores",
                    'Example: table_pattern: "{namespace}__{table}"',
                ],
            ) from e
        raise ErrorFactory.config_error(
            codes.CFG_062,
            title="Error parsing sandbox configuration",
            details=f"Failed to parse sandbox configuration: {e}",
            suggestions=[
                "strategy must be 'table' (the only supported strategy)",
                "allowed_envs must be a list of environment names, or omitted to allow all environments",
            ],
        ) from e

    _validate_sandbox_config(config)
    return config


def _validate_sandbox_config(config: SandboxConfig) -> None:
    """``allowed_envs`` may be ``None`` (unrestricted) but never an empty list."""
    if config.allowed_envs is not None and len(config.allowed_envs) == 0:
        raise ErrorFactory.config_error(
            codes.CFG_062,
            title="Invalid sandbox configuration",
            details=(
                "allowed_envs is an empty list ([]), which forbids sandbox mode in "
                "every environment. Omit allowed_envs to allow all environments, "
                "or list at least one environment name."
            ),
            suggestions=[
                "Remove allowed_envs to leave sandbox mode unrestricted",
                "Or list the permitted environments, e.g. allowed_envs: [dev, tst]",
            ],
        )
