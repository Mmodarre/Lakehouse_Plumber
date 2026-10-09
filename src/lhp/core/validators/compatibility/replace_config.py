"""REPLACE USING (``mode: replace``) config validation for streaming tables.

Validates the ``replace_config`` block that drives ``@dp.replace_flow``: a
non-empty ``replace_using`` key-column list and exactly one ``sequence_by``
column. Field names match the Databricks API parameters verbatim
(``replace_using`` <-> ``replace_using=``), mirroring ``cdc_config``.
"""

import logging
from typing import Any, Dict, List

from lhp.models import Action

logger = logging.getLogger(__name__)


class ReplaceFlowConfigValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating replace configuration for {prefix}")
        errors: List[str] = []

        if not action.write_target:
            return errors

        replace_config = action.write_target.get("replace_config")
        if replace_config is None:
            errors.append(f"{prefix}: replace mode requires 'replace_config'")
            return errors

        if not isinstance(replace_config, dict):
            errors.append(f"{prefix}: 'replace_config' must be a dictionary")
            return errors

        # An explicitly-empty block ({}) falls through to the required-field
        # checks below, so the user is told which keys are missing rather than
        # that the whole block is absent.

        errors.extend(self._validate_replace_using(replace_config, prefix))
        errors.extend(self._validate_sequence_by(replace_config, prefix))

        return errors

    def _validate_replace_using(
        self, replace_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors: List[str] = []

        replace_using = replace_config.get("replace_using")
        if replace_using is None:
            errors.append(f"{prefix}: replace_config must have 'replace_using'")
        elif not isinstance(replace_using, list):
            errors.append(f"{prefix}: 'replace_using' must be a list")
        elif not replace_using:
            errors.append(f"{prefix}: 'replace_using' cannot be empty")
        else:
            for i, col in enumerate(replace_using):
                if not isinstance(col, str):
                    errors.append(f"{prefix}: replace_using[{i}] must be a string")

        return errors

    def _validate_sequence_by(
        self, replace_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors: List[str] = []

        # REPLACE USING takes exactly one sequence_by column (a single string),
        # unlike cdc_config which also accepts a list.
        sequence_by = replace_config.get("sequence_by")
        if sequence_by is None or sequence_by == "":
            errors.append(f"{prefix}: replace_config must have 'sequence_by'")
        elif not isinstance(sequence_by, str):
            errors.append(
                f"{prefix}: 'sequence_by' must be a single string column "
                f"(REPLACE USING allows exactly one sequence column)"
            )

        return errors
