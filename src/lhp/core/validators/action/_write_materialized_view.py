"""Materialized-view validation for write actions.

A module-level function split out of :class:`WriteActionValidator` to keep
that class small (constitution §3.3).
"""

from typing import List

from lhp.models import Action


def validate_materialized_view(action: Action, prefix: str) -> List[str]:
    errors = []

    if (
        not action.source
        and not action.write_target.get("sql")
        and not action.write_target.get("sql_path")
    ):
        errors.append(
            f"{prefix}: Materialized view must have either 'source', 'sql', or 'sql_path' in write_target"
        )
    elif action.source and not isinstance(action.source, (str, list)):
        errors.append(
            f"{prefix}: Materialized view source must be a string or list of view names"
        )

    return errors
