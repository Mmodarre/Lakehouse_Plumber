"""The SQL text a SQL load or SQL transform renders.

The read-mode checks read a SQL action's body exactly as its generator renders
it; :func:`rendered_sql` exposes that one reading (``_read_modes``) to callers
that parse the body themselves, such as the REPLACE WHERE stream-read facts.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from lhp.models import Action

from ._read_modes import _sql_text

if TYPE_CHECKING:
    from ..processing import EnhancedSubstitutionManager


def rendered_sql(
    action: Action,
    *,
    project_root: Optional[Path] = None,
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
) -> Optional[str]:
    """The SQL ``action`` (a SQL load or SQL transform) renders, or ``None``.

    As ``generators/load/sql.py`` and ``generators/transform/sql.py`` read it:
    inline SQL (a load's ``source.sql`` winning over ``source.sql_path``, a
    transform's non-empty ``sql`` over ``sql_path``) substituted through
    ``substitution_mgr``, else the ``sql_path`` file under ``project_root``
    through the generators' loader. Secret references are not recorded on
    ``substitution_mgr``. ``None`` when the SQL cannot be read or substituted.
    Placeholders such as ``$source`` are left in the text.
    """
    return _sql_text(action, project_root, substitution_mgr)
