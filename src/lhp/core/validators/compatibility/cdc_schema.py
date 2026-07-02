from typing import List

from lhp.models import Action
from lhp.parsers import SchemaParser


class CdcSchemaValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        errors = []

        if not action.write_target:
            return errors

        schema = action.write_target.get("table_schema")
        if not schema:
            return errors

        # An inline dict schema stores column names inside 'columns', so a
        # substring check against the dict would inspect its keys, not the
        # columns. Convert it to the DDL hint string first (§3.5: stateless,
        # instantiate locally) so the __START_AT / __END_AT checks below run
        # against the actual column names.
        if isinstance(schema, dict):
            if "columns" not in schema:
                # A structurally invalid inline schema (missing 'columns') is
                # reported by the table-options validator; with no columns there
                # is nothing to check for __START_AT / __END_AT here.
                return errors
            schema = SchemaParser().to_schema_hints(schema)

        if "__START_AT" not in schema:
            errors.append(
                f"{prefix}: CDC schema must include '__START_AT' column with same type as sequence_by"
            )

        if "__END_AT" not in schema:
            errors.append(
                f"{prefix}: CDC schema must include '__END_AT' column with same type as sequence_by"
            )

        return errors
