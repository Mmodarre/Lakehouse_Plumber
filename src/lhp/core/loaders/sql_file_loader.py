"""Read ``sql_path`` files for template expansion and code generation.

The SQL load, SQL transform and materialized-view generators read their
``sql_path`` file through :func:`load_sql_file`, and the flowgroup-resolution
validator reuses it, so ``lhp validate`` and ``lhp generate`` read, substitute
and reject a file the same way. Template expansion reads a template action's
file through :func:`read_sql_file` before any substitution, to render it with
the template parameters.
"""

import logging
from pathlib import Path
from typing import Optional, Union

from ..processing import (
    EnhancedSubstitutionManager,
    find_unresolved_tokens,
    unresolved_tokens_error,
)
from .external_file_loader import load_external_file_text

logger = logging.getLogger(__name__)


def load_sql_file(
    sql_path: Union[str, Path],
    project_root: Path,
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    pipeline: Optional[str] = None,
    flowgroup: Optional[str] = None,
    action: Optional[str] = None,
) -> str:
    """Return the stripped text of ``sql_path`` with environment tokens substituted.

    ``sql_path`` resolves against ``project_root``. Without ``substitution_mgr``
    the file is returned unsubstituted (no environment is known). With one,
    ``${token}`` and ``${secret:...}`` references are substituted, recording
    secret references on the manager; a plain ``${name}`` still left afterwards
    is an error unless the manager was built with ``skip_validation=True``.
    ``pipeline``, ``flowgroup`` and ``action`` name the referencing action in
    that error.

    :raises LHPFileError: ``LHP-IO-001`` when the file does not exist;
        ``LHP-IO-004`` when it is not valid UTF-8 text; ``LHP-IO-005`` when
        permission to read it is denied.
    :raises LHPConfigError: ``LHP-CFG-010`` when a plain ``${name}`` is
        unresolved, titled with the tokens and the file so a one-line report
        names them; ``LHP-CFG-008`` / ``LHP-CFG-059`` for a secret reference
        without a scope or with an empty scope or key.
    """
    text = load_external_file_text(sql_path, project_root, file_type="SQL file").strip()
    if substitution_mgr is None:
        return text

    text = substitution_mgr._process_string(text)
    if not substitution_mgr.skip_validation:
        tokens = find_unresolved_tokens(text)
        if tokens:
            raise unresolved_tokens_error(
                tokens,
                file=sql_path,
                project_root=project_root,
                env=substitution_mgr.env,
                pipeline=pipeline,
                flowgroup=flowgroup,
                action=action,
                located_title=True,
            )
    logger.debug(f"Loaded SQL file '{sql_path}' ({len(text)} chars)")
    return text


def read_sql_file(project_root: Path, sql_path: str) -> Optional[str]:
    """Return the text of ``sql_path`` as template expansion reads it, or ``None``.

    ``sql_path`` resolves against ``project_root`` exactly as :func:`load_sql_file`
    resolves it. A file that does not exist returns ``None`` so expansion leaves
    the action unchanged and generation reports the missing file as before.

    :raises LHPFileError: ``LHP-IO-004`` when the file is not valid UTF-8 text;
        ``LHP-IO-005`` when permission to read it is denied.
    """
    candidate = Path(sql_path)
    if not (
        candidate if candidate.is_absolute() else project_root / candidate
    ).is_file():
        return None
    return load_external_file_text(sql_path, project_root, file_type="SQL file")
