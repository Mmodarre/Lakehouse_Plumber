"""Materialized-view validation for write actions.

A module-level function split out of :class:`WriteActionValidator` to keep
that class small (constitution §3.3).
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from lhp.models import Action

from .._base import ValidationError
from ..field import validate_expectations_file


def validate_materialized_view(
    action: Action, prefix: str, project_root: Optional[Path]
) -> List[ValidationError]:
    """Check a materialized view's query source and its expectations file.

    The view needs ``source``, ``write_target.sql`` or
    ``write_target.sql_path``. A ``write_target.expectations_file`` must load,
    hold valid rules (any of warn, drop and fail) and give each rule its own
    name, since the generator keys the ``@dp.expect_all*`` decorators by name.
    """
    errors: List[ValidationError] = []
    # WriteActionValidator.validate has already rejected a non-dict write_target.
    write_target = cast(Dict[str, Any], action.write_target)

    if (
        not action.source
        and not write_target.get("sql")
        and not write_target.get("sql_path")
    ):
        errors.append(
            f"{prefix}: Materialized view must have either 'source', 'sql', or 'sql_path' in write_target"
        )
    elif action.source and not isinstance(action.source, (str, list)):
        errors.append(
            f"{prefix}: Materialized view source must be a string or list of view names"
        )

    errors.extend(
        validate_expectations_file(
            write_target.get("expectations_file"),
            project_root,
            prefix,
            unique_names=True,
        )
    )
    return errors
