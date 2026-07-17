"""UC tagging hook generation, callable from a worker."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from lhp.models import FlowGroup, ProjectConfig

    from ..processing.substitution import EnhancedSubstitutionManager

logger = logging.getLogger(__name__)

_TAGGABLE_SUBTYPES = {"streaming_table", "materialized_view"}
_SCHEMA_FILE_SUFFIXES = (".yaml", ".yml", ".json")


def flowgroup_has_uc_tags(flowgroup: "FlowGroup") -> bool:
    """True if any taggable write action in the flowgroup may carry UC tags.

    Conservative by design: returns True when a streaming-table / MV write
    target declares ``tags``, references an external ``tags_file`` sidecar, OR
    references a structured (YAML/JSON) ``table_schema`` that could hold column
    tags. Used by the pool to decide whether to retain a resolved flowgroup for
    the commit-time tagging hook (so tagging works even when ``include_tests``
    is False). ``tags_file`` is resolved only at commit time, so it must be
    honored here or a file-only-tag flowgroup would be silently dropped. Imports
    nothing heavy, so it is safe to call across the worker spawn boundary.
    """
    from lhp.models import ActionType

    def _get(wt, key):
        return wt.get(key) if isinstance(wt, dict) else getattr(wt, key, None)

    for action in getattr(flowgroup, "actions", None) or []:
        if action.type != ActionType.WRITE or not action.write_target:
            continue
        wt = action.write_target
        if _get(wt, "type") not in _TAGGABLE_SUBTYPES:
            continue
        if _get(wt, "tags") is not None:
            return True
        if _get(wt, "tags_file"):
            return True
        table_schema = _get(wt, "table_schema")
        if isinstance(table_schema, str) and table_schema.lower().endswith(
            _SCHEMA_FILE_SUFFIXES
        ):
            return True
    return False


def build_uc_tagging_hook_files(
    *,
    pipeline_name: str,
    flowgroups: List["FlowGroup"],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
    sandbox_active: bool = False,
) -> Optional[Dict[str, str]]:
    """Build the per-pipeline tagging hook's files IN MEMORY, if applicable.

    Returns ``{"_uc_tagging_hook.py": <content>}`` or ``None`` when ``uc_tagging``
    is disabled or no UC tags are declared. Does NOT touch disk.

    ``sandbox_active`` (True under ``--sandbox``) skips the ``tags_file`` /
    write-target table cross-check, since sandbox renames the tables.
    """
    # Deferred import: pulls in Jinja machinery callers without tags never need.
    from .uc_tagging_hook_generator import UCTaggingHookGenerator

    generator = UCTaggingHookGenerator(
        project_config, project_root, sandbox_active=sandbox_active
    )
    return generator.build_hook_files(
        processed_flowgroups=flowgroups,
        pipeline_name=pipeline_name,
        substitution_mgr=substitution_mgr,
    )


def generate_uc_tagging_hook(
    *,
    pipeline_name: str,
    flowgroups: List["FlowGroup"],
    output_dir: Path,
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
    sandbox_active: bool = False,
) -> int:
    """Generate the per-pipeline tagging hook artifact, if applicable.

    Returns the number of artifacts written (0 when ``uc_tagging`` is disabled or
    no UC tags are declared, 1 when the hook is written).

    ``sandbox_active`` (True under ``--sandbox``) skips the ``tags_file`` /
    write-target table cross-check, since sandbox renames the tables.
    """
    from .uc_tagging_hook_generator import UCTaggingHookGenerator

    generator = UCTaggingHookGenerator(
        project_config, project_root, sandbox_active=sandbox_active
    )
    content = generator.generate(
        processed_flowgroups=flowgroups,
        pipeline_name=pipeline_name,
        output_dir=output_dir,
        substitution_mgr=substitution_mgr,
    )
    return 1 if content else 0
