import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._cdc_helpers import validate_column_lists, validate_scd_options

logger = logging.getLogger(__name__)


class CdcConfigValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating CDC configuration for {prefix}")
        errors = []

        if not action.write_target:
            return errors

        cdc_config = action.write_target.get("cdc_config")
        if not cdc_config:
            errors.append(f"{prefix}: cdc mode requires 'cdc_config'")
            return errors

        if not isinstance(cdc_config, dict):
            errors.append(f"{prefix}: 'cdc_config' must be a dictionary")
            return errors

        errors.extend(self._validate_required_fields(cdc_config, prefix))
        errors.extend(self._validate_sequence_options(cdc_config, prefix))
        errors.extend(validate_scd_options(cdc_config, prefix))
        errors.extend(validate_column_lists(cdc_config, prefix))
        errors.extend(self._validate_other_options(cdc_config, prefix))

        return errors

    def _validate_required_fields(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors = []

        keys = cdc_config.get("keys")
        if not keys:
            errors.append(f"{prefix}: cdc_config must have 'keys'")
        elif not isinstance(keys, list):
            errors.append(f"{prefix}: 'keys' must be a list")
        elif not keys:
            errors.append(f"{prefix}: 'keys' cannot be empty")
        else:
            for i, key in enumerate(keys):
                if not isinstance(key, str):
                    errors.append(f"{prefix}: keys[{i}] must be a string")

        return errors

    def _validate_sequence_options(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors = []

        sequence_by = cdc_config.get("sequence_by")
        if sequence_by is not None:
            if isinstance(sequence_by, str):
                pass
            elif isinstance(sequence_by, list):
                if not sequence_by:
                    errors.append(f"{prefix}: 'sequence_by' list cannot be empty")
                else:
                    for i, col in enumerate(sequence_by):
                        if not isinstance(col, str):
                            errors.append(
                                f"{prefix}: sequence_by[{i}] must be a string"
                            )
            else:
                errors.append(
                    f"{prefix}: 'sequence_by' must be a string or list of strings"
                )

        return errors

    def _validate_other_options(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        return []
