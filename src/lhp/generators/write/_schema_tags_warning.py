"""Generate-time guard for the UC-tag silent-drop footgun (LHP-CFG-069).

A write action's ``table_schema`` file is read for column types only; any UC tags
it carries are applied only when that same file is ALSO wired as the action's
``tags_file``. This helper is shared by the streaming-table and materialized-view
write generators (§9.24) so the detection lives in exactly one place.
"""

import logging
from pathlib import Path

from ...core.loaders.external_file_loader import resolve_external_file_path
from ...errors import LHPError, codes
from ...parsers import unified_schema_format

logger = logging.getLogger(__name__)


def warn_if_schema_tags_dropped(
    schema_data: dict,
    target_config: dict,
    resolved_schema_path: Path,
    project_root: Path,
) -> None:
    """Warn (LHP-CFG-069) when a ``table_schema`` file carries UC tags but is not
    also the action's ``tags_file``, so those tags are silently dropped.

    No-op when the schema file has no tags, or when ``tags_file`` resolves to the
    same file as ``table_schema`` (equivalent spellings compared via resolved
    paths). Logs at most one warning; never raises.
    """
    if not unified_schema_format.schema_has_tags(schema_data):
        return

    tags_file = target_config.get("tags_file")
    if tags_file:
        try:
            tags_path = resolve_external_file_path(
                tags_file, project_root, file_type="tags file"
            )
        except LHPError:
            # An unresolvable tags_file falls through to the drop warning; its
            # real error (LHP-IO-001) surfaces later in the UC tagging hook.
            tags_path = None
        if (
            tags_path is not None
            and tags_path.resolve() == resolved_schema_path.resolve()
        ):
            return

    logger.warning(
        f"{codes.CFG_069.code}: table schema file '{resolved_schema_path}' declares "
        "UC tags, but it is not wired as this write target's 'tags_file'; those tags "
        "will be dropped. Point 'tags_file' at the same file to apply them."
    )
