"""REPLACE WHERE (``mode: replace_where``) config validation for streaming tables.

Validates the ``replace_where_config`` block and the write-level settings a
REPLACE WHERE flow cannot honour. The flow is a single
``@dp.table(..., replace_where=...)`` over a batch read, so the predicate is SQL
text baked verbatim into the generated code. The key name matches the
Databricks API parameter (``replace_where`` <-> ``replace_where=``), mirroring
``replace_config``.
"""

import logging
from typing import Any, Dict, List

from lhp.models import Action

logger = logging.getLogger(__name__)

_ALLOWED_CONFIG_KEYS = frozenset({"replace_where"})

# The substitution pass rewrites ``${secret:scope/key}`` to this placeholder
# (core/processing/substitution.py), so a resolved predicate carries it.
_SECRET_PLACEHOLDER_PREFIX = "__SECRET_"

_EXPECTATIONS_LIMITATION = (
    "Expectations are not supported on tables targeted by REPLACE WHERE flows."
)


class ReplaceWhereConfigValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating replace_where configuration for {prefix}")

        write_target = action.write_target
        if not isinstance(write_target, dict):
            return []

        errors = _validate_config_block(
            write_target.get("replace_where_config"), prefix
        )
        errors.extend(_validate_write_target_settings(write_target, prefix))
        errors.extend(_validate_action_settings(action, prefix))
        return errors


def _validate_config_block(replace_where_config: Any, prefix: str) -> List[str]:
    if replace_where_config is None:
        return [f"{prefix}: replace_where mode requires 'replace_where_config'"]
    if not isinstance(replace_where_config, dict):
        return [f"{prefix}: 'replace_where_config' must be a dictionary"]

    errors = _validate_predicate(replace_where_config.get("replace_where"), prefix)

    unknown = sorted(
        str(k) for k in replace_where_config if k not in _ALLOWED_CONFIG_KEYS
    )
    if unknown:
        listed = ", ".join(f"'{k}'" for k in unknown)
        errors.append(
            f"{prefix}: replace_where_config has unknown key(s) {listed}; "
            f"the only allowed key is 'replace_where'"
        )
    return errors


def _validate_predicate(predicate: Any, prefix: str) -> List[str]:
    if predicate is None:
        return [f"{prefix}: replace_where_config must have 'replace_where'"]
    if not isinstance(predicate, str):
        return [
            f"{prefix}: 'replace_where' must be a SQL predicate string, "
            f"got {type(predicate).__name__}"
        ]
    if not predicate.strip():
        return [f"{prefix}: 'replace_where' cannot be blank"]
    if _SECRET_PLACEHOLDER_PREFIX in predicate:
        # The predicate is emitted verbatim as SQL text in the flow definition;
        # a secret there would be resolved into the query the platform runs.
        return [
            f"{prefix}: 'replace_where' cannot reference a secret "
            f"(${{secret:...}}); the predicate is SQL text written verbatim into "
            f"the generated flow definition. Use a literal value or an "
            f"${{env_token}} instead."
        ]
    return []


def _validate_write_target_settings(
    write_target: Dict[str, Any], prefix: str
) -> List[str]:
    errors: List[str] = []

    if write_target.get("create_table") is False:
        errors.append(
            f"{prefix}: replace_where mode does not support 'create_table: false'; "
            f"a REPLACE WHERE flow defines its target table and the flow in a "
            f"single @dp.table statement, so it always creates the table"
        )

    if write_target.get("temporary"):
        errors.append(
            f"{prefix}: replace_where mode does not support 'temporary'; "
            f"use 'private: true' for a pipeline-private table"
        )

    return errors


def _validate_action_settings(action: Action, prefix: str) -> List[str]:
    errors: List[str] = []

    if action.readMode == "stream":
        errors.append(
            f"{prefix}: replace_where mode requires a batch source; readMode "
            f"'stream' is not supported (REPLACE WHERE reads via spark.read, and "
            f"Databricks rejects a streaming source with "
            f"REPLACE_WHERE_WITH_STREAMING_SOURCE)"
        )

    if action.expectations:
        errors.append(
            f"{prefix}: replace_where mode does not support expectations; remove "
            f"the action's 'expectations'. Databricks: "
            f'"{_EXPECTATIONS_LIMITATION}"'
        )

    return errors
