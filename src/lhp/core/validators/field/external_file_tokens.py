"""Reject ``${name}`` tokens left unresolved in the files a flowgroup's code renders.

Runs during flowgroup resolution, which ``lhp validate`` and ``lhp generate``
share, and reads each file through the loader and the tags check code
generation uses, so both commands raise the same ``LHP-CFG-010``. Checked: the
``sql_path`` of a SQL load, a SQL transform and a materialized view, and the
UC ``tags_file`` the tagging hook reads. Python files are not checked.
"""

import copy
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional

from lhp.models import Action, ActionType, FlowGroup

from ...registry import determine_action_subtype

if TYPE_CHECKING:
    from ...processing import EnhancedSubstitutionManager

logger = logging.getLogger(__name__)

#: Reads one ``sql_path`` file as generation renders it:
#: ``(sql_path, project_root, substitution_mgr, pipeline, flowgroup, action,
#: sql_parameters)`` to the rendered and substituted text, raising
#: ``LHP-CFG-071`` for a file that does not render and ``LHP-CFG-010`` for a
#: leftover token. The composition root injects
#: ``lhp.core.loaders.load_sql_file``.
SqlFileLoader = Callable[
    [
        str,
        Path,
        Optional["EnhancedSubstitutionManager"],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[Mapping[str, Any]],
    ],
    str,
]

#: Checks the ``tags_file`` the tagging hook reads for one action:
#: ``(action, substitution_mgr, pipeline, flowgroup)``, a no-op for actions the
#: hook does not tag. The composition root injects
#: ``lhp.core.codegen.check_tags_file_tokens`` bound to the project's
#: configuration and root, so this package does not import ``core/codegen``.
TagsFileTokenCheck = Callable[
    [Action, "EnhancedSubstitutionManager", Optional[str], Optional[str]], None
]


class ExternalFileTokenValidator:
    """Check the ``sql_path`` and ``tags_file`` files a flowgroup's code renders.

    Holds the project root and two picklable callables, so it crosses the
    worker ``spawn`` boundary with :class:`FlowgroupResolutionService`.
    """

    def __init__(
        self,
        project_root: Path,
        load_sql_file: SqlFileLoader,
        check_tags_file: TagsFileTokenCheck,
    ) -> None:
        self._project_root = project_root
        self._load_sql_file = load_sql_file
        self._check_tags_file = check_tags_file

    def validate(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: "EnhancedSubstitutionManager",
    ) -> None:
        """Read and substitute every rendered file of ``flowgroup`` in this environment.

        A manager built with ``skip_validation=True`` skips the check, like the
        flowgroup-level unresolved-token check. Secret references found while
        substituting are not recorded on ``substitution_mgr``; generation
        records them when it reads the same files.

        :raises LHPConfigError: ``LHP-CFG-071`` when a ``sql_path`` file does
            not render with the action's ``sql_parameters``;
            ``LHP-CFG-010`` when a plain ``${name}`` is
            unresolved in a ``sql_path`` file or a ``tags_file``;
            ``LHP-CFG-008`` / ``LHP-CFG-059`` for a ``${secret:...}`` in a
            ``sql_path`` file or tags file without a default scope or with an
            empty scope or key; ``LHP-CFG-009`` for a tags file that is not
            valid YAML; ``LHP-CFG-067`` for an invalid tags file.
        :raises LHPFileError: ``LHP-IO-001`` when a referenced file is missing;
            ``LHP-IO-004`` when a ``sql_path`` file is not valid UTF-8 text;
            ``LHP-IO-005`` when permission to read a ``sql_path`` file is
            denied; ``LHP-IO-002`` when a tags file cannot be read;
            ``LHP-IO-003`` when a tags file holds no YAML document or more
            than one.
        """
        if substitution_mgr.skip_validation:
            return
        probe = _without_secret_recording(substitution_mgr)
        for action in flowgroup.actions:
            sql_path = _rendered_sql_path(action)
            if sql_path:
                self._load_sql_file(
                    sql_path,
                    self._project_root,
                    probe,
                    flowgroup.pipeline,
                    flowgroup.flowgroup,
                    action.name,
                    action.sql_parameters,
                )
                logger.debug(f"sql_path tokens resolve for action '{action.name}'")
            self._check_tags_file(
                action, probe, flowgroup.pipeline, flowgroup.flowgroup
            )


def _rendered_sql_path(action: Action) -> Optional[str]:
    """The ``sql_path`` the action's generator reads, or ``None``.

    Mirrors the SQL load, SQL transform and materialized-view generators: an
    inline ``sql`` wins over ``sql_path``, which is then never read.
    """
    sub_type = determine_action_subtype(action)
    if action.type == ActionType.LOAD and sub_type == "sql":
        source = action.source
        if isinstance(source, dict) and "sql" not in source:
            return source.get("sql_path")
        return None
    if action.type == ActionType.TRANSFORM and sub_type == "sql":
        return None if action.sql else action.sql_path
    if action.type == ActionType.WRITE and sub_type == "materialized_view":
        target = action.write_target
        if isinstance(target, dict) and "sql" not in target:
            return target.get("sql_path")
    return None


def _without_secret_recording(
    substitution_mgr: "EnhancedSubstitutionManager",
) -> "EnhancedSubstitutionManager":
    """A shallow copy of ``substitution_mgr`` with its own secret-reference set.

    Substituting a file records its ``${secret:...}`` references on the
    manager; the resolver then validates every recorded reference. Generation
    records a ``sql_path`` file's secrets only when it renders the file, so the
    check substitutes through this copy to leave that behaviour unchanged.
    """
    probe = copy.copy(substitution_mgr)
    probe.secret_references = set()
    return probe
