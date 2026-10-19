"""Load a ``sql_path`` file as the SQL code generation renders.

The SQL load, SQL transform and materialized-view generators read their
``sql_path`` file here, and the flowgroup-resolution validator reuses this
loader, so ``lhp validate`` and ``lhp generate`` read, render, substitute and
reject a file the same way.
"""

import logging
from pathlib import Path
from typing import Any, Mapping, Optional, Union

from ..processing import (
    EnhancedSubstitutionManager,
    find_unresolved_tokens,
    render_sql_parameters,
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
    parameters: Optional[Mapping[str, Any]] = None,
) -> str:
    """Return the stripped text of ``sql_path`` with environment tokens substituted.

    ``sql_path`` resolves against ``project_root``. When ``parameters`` is not
    ``None`` (the action's ``sql_parameters``) the file is first rendered as a
    Jinja template with them. Without ``substitution_mgr`` the file is returned
    unsubstituted (no environment is known). With one, ``${token}`` and
    ``${secret:...}`` references are substituted, recording secret references
    on the manager; a plain ``${name}`` still left afterwards is an error unless
    the manager was built with ``skip_validation=True``. ``pipeline``,
    ``flowgroup`` and ``action`` name the referencing action in those errors.

    :raises LHPFileError: ``LHP-IO-001`` when the file does not exist;
        ``LHP-IO-004`` when it is not valid UTF-8 text; ``LHP-IO-005`` when
        permission to read it is denied.
    :raises LHPConfigError: ``LHP-CFG-071`` when ``parameters`` is given and
        the file is not a valid Jinja template or uses a name it does not
        define; ``LHP-CFG-010`` when a plain ``${name}`` is unresolved, titled
        with the tokens and the file so a one-line report names them;
        ``LHP-CFG-008`` / ``LHP-CFG-059`` for a secret reference without a
        scope or with an empty scope or key.
    """
    text = load_external_file_text(sql_path, project_root, file_type="SQL file")
    if parameters is not None:
        text = render_sql_parameters(
            text,
            parameters,
            file=sql_path,
            project_root=project_root,
            pipeline=pipeline,
            flowgroup=flowgroup,
            action=action,
        )
    text = text.strip()
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
