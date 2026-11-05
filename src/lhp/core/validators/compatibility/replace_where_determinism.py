"""Warn when non-deterministic operational metadata feeds a REPLACE WHERE table.

Databricks refreshes a ``mode: replace_where`` table incrementally only while
the query behind it is deterministic. A non-deterministic operational-metadata
column (``current_timestamp()``, ``uuid()``, ...) computed on any action
upstream of the write inside the flowgroup makes every refresh a
``COMPLETE_REPLACE_WHERE``: the whole window is rewritten and incremental
refresh is silently lost. The pipeline still runs, so this is a warning
(``LHP-VAL-073``) recorded on the worker scope, never an error.
"""

import logging
import re
from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

from lhp.errors import codes
from lhp.models import Action, FlowGroup, ProjectConfig
from lhp.models.run_warnings import record_validation_warning

if TYPE_CHECKING:
    from ...processing import EnhancedSubstitutionManager
    from ...registry import ActionRegistry
    from ..field import SelectedColumnsResolver

logger = logging.getLogger(__name__)

#: A resolved metadata expression whose value changes from one refresh to the
#: next: a non-deterministic function call, a bare SQL datetime keyword, or the
#: argument-less ``unix_timestamp()`` (the current time).
_NON_DETERMINISTIC_EXPRESSION = re.compile(
    "|".join(
        (
            r"\b(current_timestamp|current_date|localtimestamp|now|curdate|rand|randn|random|uuid|shuffle|monotonically_increasing_id|input_file_name|input_file_block_(start|length)|spark_partition_id)\s*\(",
            r"\b(current_timestamp|current_date|localtimestamp)\b",
            r"unix_timestamp\(\s*\)",
        )
    ),
    re.IGNORECASE,
)

_DOCS_URL = (
    "https://lakehouse-plumber.readthedocs.io/en/latest/"
    "reference/actions/write/streaming-table-replace-where.html"
)

#: ``(write, upstream action, column, resolved expression)``.
_Finding = Tuple[str, str, str, str]


class ReplaceWhereDeterminismValidator:
    """Record ``LHP-VAL-073`` for non-deterministic metadata upstream of REPLACE WHERE.

    Walks upstream from each ``replace_where`` write over the actions of its
    own flowgroup, and resolves the metadata columns of every visited action
    whose generator renders them, through the same resolver
    :class:`~lhp.core.validators.OperationalMetadataExpressionValidator` uses.
    The finding is not fatal, so :meth:`validate` returns nothing and records
    ONE warning per flowgroup listing every offending (write, upstream action,
    column): the warning channel keeps one record per ``(code, file)``.

    Holds the project configuration, the registry and the bound resolver, all
    picklable, so it crosses the worker ``spawn`` boundary with
    :class:`FlowgroupResolutionService`.
    """

    def __init__(
        self,
        project_config: Optional[ProjectConfig],
        action_registry: "ActionRegistry",
        resolve_columns: "SelectedColumnsResolver",
    ) -> None:
        self._project_config = project_config
        self._action_registry = action_registry
        self._resolve_columns = resolve_columns

    def validate(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: "EnhancedSubstitutionManager",
        preset_config: Mapping[str, Any],
    ) -> None:
        """Record ``LHP-VAL-073`` when ``flowgroup`` has an offending write.

        ``preset_config`` is the flowgroup's resolved preset chain, as for
        :class:`OperationalMetadataExpressionValidator`, which runs first and
        has already rejected any expression that fails to resolve.
        """
        # Lazy: importing lhp.core.dependencies at module level closes a cold
        # import cycle back into this package (see config_validator.py).
        from lhp.core.dependencies import extract_action_sources, is_replace_where_write

        writes = [a for a in flowgroup.actions if is_replace_where_write(a)]
        if not writes:
            return

        producers = {a.target: a for a in flowgroup.actions if a.target}
        resolved: Dict[str, Mapping[str, str]] = {}
        findings: List[_Finding] = []
        for write in writes:
            for upstream in _upstream_actions(write, producers, extract_action_sources):
                if not self._action_registry.renders_operational_metadata(upstream):
                    continue
                if upstream.name not in resolved:
                    resolved[upstream.name] = self._resolve_columns(
                        upstream,
                        flowgroup,
                        dict(preset_config),
                        self._project_config,
                        substitution_mgr,
                    )
                findings.extend(
                    (write.name, upstream.name, column, expression)
                    for column, expression in sorted(resolved[upstream.name].items())
                    if _NON_DETERMINISTIC_EXPRESSION.search(expression)
                )
        if findings:
            logger.debug(
                f"Flowgroup '{flowgroup.flowgroup}': {len(findings)} "
                f"non-deterministic metadata column(s) upstream of replace_where"
            )
            record_validation_warning(codes.VAL_073, message=_message(findings))


def _upstream_actions(
    write: Action,
    producers: Mapping[str, Action],
    extract_sources: Callable[[Action], List[str]],
) -> Iterator[Action]:
    """Yield the flowgroup actions ``write`` reads from, transitively, breadth
    first; the walk stops at a name no action in the flowgroup produces."""
    queue = deque(extract_sources(write))
    seen: set[str] = set()
    while queue:
        name = queue.popleft()
        # A malformed ``sources`` entry may not be a string; it names no view.
        if not isinstance(name, str) or name in seen or name not in producers:
            continue
        seen.add(name)
        producer = producers[name]
        yield producer
        queue.extend(extract_sources(producer))


def _message(findings: List[_Finding]) -> str:
    listed = "\n".join(
        f"  - write '{write}' <- action '{upstream}': column '{column}' = {expression}"
        for write, upstream, column, expression in findings
    )
    return (
        "Non-deterministic operational metadata is computed upstream of a "
        "REPLACE WHERE (mode: replace_where) table. Incremental refresh is "
        "disabled: every refresh rewrites the whole replace_where window "
        f"(COMPLETE_REPLACE_WHERE).\n{listed}\n"
        "Deselect these columns on the listed actions (for example "
        "'operational_metadata: false') or make their expressions "
        f"deterministic. See {_DOCS_URL}"
    )
