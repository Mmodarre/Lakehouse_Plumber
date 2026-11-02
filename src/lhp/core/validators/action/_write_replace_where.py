"""Write-action rules for streaming_table ``mode: replace_where`` (REPLACE WHERE).

A REPLACE WHERE flow is one ``@dp.table(..., replace_where=...)`` statement over
a batch read of a single source view: the flow and its table share one name, so
the target cannot be fed by several flows. This module owns the write-level
source rule and delegates the ``replace_where_config`` block and the remaining
write/action settings to ``ReplaceWhereConfigValidator``.
"""

import logging
from typing import List

from lhp.models import Action

from ..compatibility import ReplaceWhereConfigValidator

logger = logging.getLogger(__name__)


def validate_replace_where_write(
    action: Action, prefix: str, config_validator: ReplaceWhereConfigValidator
) -> List[str]:
    logger.debug(f"Validating replace_where write for {prefix}")
    errors: List[str] = []

    # Mirrors the replace rule: a missing or non-string/list source is already
    # reported by the generic streaming-table source check, and a one-element
    # list is one view, so only a multi-view list needs a mode rule here.
    if isinstance(action.source, list) and len(action.source) > 1:
        errors.append(
            f"{prefix}: replace_where mode does not support multiple source views "
            f"in a single action. A REPLACE WHERE target must be served by a "
            f"single flow reading one batch source."
        )

    errors.extend(config_validator.validate(action, prefix))
    return errors
