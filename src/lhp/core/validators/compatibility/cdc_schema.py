"""Validator for CDC schema requirements."""

from typing import List

from lhp.models import Action


class CdcSchemaValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        errors = []

        if not action.write_target:
            return errors

        schema = action.write_target.get("table_schema")
        if not schema:
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
