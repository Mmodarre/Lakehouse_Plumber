"""Normalize namespace fields from legacy 'database' format to explicit catalog/schema.

# REMOVE_AT_V1.0.0: Delete this entire module once the deprecated 'database' field
# is removed. Also remove the import + call in flowgroup_processor.py.

Converts write targets and delta load sources from the packed format:
    database: "my_catalog.my_schema"
to the explicit 3-level namespace:
    catalog: "my_catalog"
    schema: "my_schema"

This runs post-substitution, pre-Pydantic, so all tokens are already resolved.
"""

import logging
import re
from typing import Any, Dict

from ...utils.error_formatter import ErrorCategory, LHPConfigError

logger = logging.getLogger(__name__)

_DEPRECATION_MSG = (
    "DEPRECATION: The 'database' field (e.g., database: \"catalog.schema\") is "
    "deprecated and will be removed in v1.0.0. Use explicit 'catalog' and 'schema' "
    "fields instead. Auto-converted for now."
)

# Pattern to detect DDL-like values in the 'schema' field.
# Matches SQL type keywords that appear in column definitions.
_DDL_PATTERN = re.compile(
    r"\b(BIGINT|STRING|INT|INTEGER|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|"
    r"BOOLEAN|DATE|TIMESTAMP|BINARY|ARRAY|MAP|STRUCT|LONG|SHORT|BYTE|VOID)\b",
    re.IGNORECASE,
)


def normalize_namespace_fields(substituted_dict: dict) -> dict:
    """Normalize legacy 'database' fields to explicit catalog/schema across all actions.

    Operates on the raw substituted flowgroup dict (post-token-resolution, pre-Pydantic).

    Args:
        substituted_dict: The fully-substituted flowgroup dictionary.

    Returns:
        The dict with database fields split into catalog/schema where applicable.
    """
    actions = substituted_dict.get("actions", [])
    for action in actions:
        action_name = action.get("name", "<unknown>")
        action_type = action.get("type")

        # Normalize write target (streaming_table / materialized_view)
        write_target = action.get("write_target")
        if isinstance(write_target, dict):
            target_type = write_target.get("type", "")
            if target_type in ("streaming_table", "materialized_view"):
                _normalize_write_target(write_target, action_name)

        # Normalize delta load source
        if action_type == "load":
            source = action.get("source")
            if isinstance(source, dict) and source.get("type") == "delta":
                _normalize_delta_source(source, action_name)

    return substituted_dict


def _normalize_write_target(wt: Dict[str, Any], action_name: str) -> None:
    """Normalize a write target dict in-place.

    Cases:
    1. catalog + schema already present → new format, validate schema isn't DDL.
    2. database present:
       a. If 'schema' also present → DDL collision: redirect schema → table_schema.
       b. Split database on first dot → catalog, schema.
       c. No dot → error with migration instructions.
    """
    if wt.get("catalog") and wt.get("schema"):
        # New format — but check if 'schema' looks like DDL (common migration mistake)
        _check_schema_not_ddl(wt, action_name)
        return  # Already in new format

    database = wt.get("database")
    if not database:
        return  # No database field, nothing to normalize

    # Handle DDL collision: if 'schema' is present alongside 'database',
    # it's the DDL table schema (old alias for table_schema), not the namespace.
    if "schema" in wt and wt["schema"] is not None:
        logger.warning(
            f"Action '{action_name}': write_target has both 'database' and 'schema'. "
            f"Interpreting 'schema' as DDL table_schema (use 'table_schema' field instead)."
        )
        wt["table_schema"] = wt.pop("schema")

    # Split database on first dot
    if "." in str(database):
        catalog, schema = str(database).split(".", 1)
        wt["catalog"] = catalog
        wt["schema"] = schema
        del wt["database"]  # REMOVE_AT_V1.0.0: database field removal
        logger.warning(
            f"Action '{action_name}': {_DEPRECATION_MSG} "
            f"(database: \"{database}\" → catalog: \"{catalog}\", schema: \"{schema}\")"
        )
    else:
        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="011",
            title=f"Invalid 'database' field in write target for action '{action_name}'",
            details=(
                f"The 'database' field value '{database}' does not contain a dot separator. "
                f"It must be in 'catalog.schema' format, or use the new explicit fields."
            ),
            suggestions=[
                "Migrate to the new format with explicit 'catalog' and 'schema' fields:",
                f"  catalog: \"your_catalog\"",
                f"  schema: \"{database}\"",
                "Or fix the database field to include both: database: \"catalog.{database}\"",
            ],
            context={
                "Action": action_name,
                "Database Value": database,
            },
        )


def _normalize_delta_source(source: Dict[str, Any], action_name: str) -> None:
    """Normalize a delta load source dict in-place.

    Cases:
    1. catalog + schema already present → skip.
    2. catalog present + database present (no dot) → Format A: rename database → schema.
    3. database present without catalog → split on first dot.
    4. database has no dot and no catalog → error.
    """
    if source.get("catalog") and source.get("schema"):
        return  # Already in new format

    catalog = source.get("catalog")
    database = source.get("database")

    if not database:
        return  # No database field, nothing to normalize

    # Format A: catalog is explicit, database is actually the schema name
    if catalog and "." not in str(database):
        source["schema"] = database
        del source["database"]  # REMOVE_AT_V1.0.0
        logger.warning(
            f"Action '{action_name}': Delta source 'database' field renamed to 'schema'. "
            f"{_DEPRECATION_MSG}"
        )
        return

    # Standard format: database contains "catalog.schema"
    if "." in str(database):
        cat, schema = str(database).split(".", 1)
        source["catalog"] = cat
        source["schema"] = schema
        del source["database"]  # REMOVE_AT_V1.0.0
        logger.warning(
            f"Action '{action_name}': {_DEPRECATION_MSG} "
            f"(database: \"{database}\" → catalog: \"{cat}\", schema: \"{schema}\")"
        )
    else:
        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="012",
            title=f"Invalid 'database' field in delta source for action '{action_name}'",
            details=(
                f"The 'database' field value '{database}' does not contain a dot separator "
                f"and no 'catalog' field is present. It must be in 'catalog.schema' format, "
                f"or use the new explicit 'catalog' and 'schema' fields."
            ),
            suggestions=[
                "Migrate to the new format with explicit 'catalog' and 'schema' fields:",
                f"  catalog: \"your_catalog\"",
                f"  schema: \"{database}\"",
                "Or fix the database field to include both: database: \"catalog.{database}\"",
            ],
            context={
                "Action": action_name,
                "Database Value": database,
            },
        )


def _check_schema_not_ddl(config: Dict[str, Any], action_name: str) -> None:
    """Detect when 'schema' contains DDL instead of a UC namespace name.

    Common migration mistake: using 'schema' for table DDL definitions
    (e.g., "id BIGINT, name STRING") instead of the UC schema name.
    The correct field for DDL is 'table_schema'.
    """
    schema_val = config.get("schema")
    if not isinstance(schema_val, str):
        return

    if " " in schema_val and _DDL_PATTERN.search(schema_val):
        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="013",
            title=f"'schema' field contains DDL in action '{action_name}'",
            details=(
                f"The 'schema' field value looks like a DDL column definition:\n"
                f"  schema: \"{schema_val}\"\n\n"
                f"Since v0.7.8, the 'schema' field in write_target represents the "
                f"Unity Catalog schema name (namespace), not a table DDL definition."
            ),
            suggestions=[
                "Rename 'schema' to 'table_schema' for DDL definitions:",
                f"  table_schema: \"{schema_val}\"",
                "And set 'schema' to the UC schema name:",
                "  schema: \"your_schema_name\"",
            ],
            example=(
                "write_target:\n"
                "  type: streaming_table\n"
                "  catalog: \"my_catalog\"\n"
                "  schema: \"my_schema\"            # UC namespace\n"
                "  table: \"my_table\"\n"
                "  table_schema: \"id BIGINT, ...\"  # DDL definition"
            ),
            context={
                "Action": action_name,
                "Schema Value": schema_val[:80],
            },
        )
