import logging
from pathlib import Path
from typing import List, Optional

from lhp.core.loaders import resolve_table_schema
from lhp.errors import LHPError
from lhp.models import Action

logger = logging.getLogger(__name__)


class CdcSchemaValidator:
    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = project_root

    def validate(self, action: Action, prefix: str) -> List[str]:
        errors: List[str] = []

        if not action.write_target:
            return errors

        schema_value = action.write_target.get("table_schema")
        if not schema_value:
            return errors

        # Only SCD Type 2 carries the __START_AT/__END_AT validity columns; a Type 1
        # target must not declare them (the runtime rejects it), so the presence check
        # does not apply. Mirror the generator, which derives stored_as_scd_type from
        # cdc_config['scd_type'] and defaults to 1 (templates/write/streaming_table.py.j2).
        cdc_config = action.write_target.get("cdc_config") or {}
        if cdc_config.get("scd_type", 1) != 2:
            return errors

        # Resolve a file-based table_schema (schemas/*.yaml, *.ddl, ...) to its
        # actual schema text before checking for the SCD2 history columns; a raw
        # path string would otherwise never contain __START_AT/__END_AT.
        try:
            resolved = resolve_table_schema(schema_value, self.project_root)
        except LHPError as e:
            # Missing/invalid schema file — the generator surfaces the real IO
            # error later; skip here rather than false-positive or double-report.
            logger.debug("CDC schema resolution deferred to generator: %s", e)
            return errors

        schema = resolved.text
        if schema is None:
            # File path with no project_root to resolve against — defer.
            return errors

        if "__START_AT" not in schema:
            errors.append(
                f"{prefix}: CDC schema must include '__START_AT' column with same type as sequence_by"
            )

        if "__END_AT" not in schema:
            errors.append(
                f"{prefix}: CDC schema must include '__END_AT' column with same type as sequence_by"
            )

        return errors
