"""Generator for the Unity Catalog tagging event hook.

Produces a single ``_tagging_hook.py`` per pipeline that uses
``@dp.on_event_hook`` to apply UC tags to each table (and its columns) as soon
as that table's flow reaches ``COMPLETED``. Tags are applied via the Entity Tag
Assignments REST API (``ALTER TABLE SET TAGS`` is rejected inside pipelines).

This mirrors :mod:`lhp.core.codegen.tst_reporting_hook_generator`.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

from jinja2 import Environment

from lhp.models import ActionType, ProjectConfig, UCTaggingConfig

from ...core.loaders.external_file_loader import (
    is_file_path,
    resolve_external_file_path,
)
from ...parsers.schema_parser import SchemaParser
from ...utils.file_header import write_normalized
from ..processing.substitution import EnhancedSubstitutionManager
from ..validators.compatibility.table_creation import action_creates_table
from .template_renderer import get_lhp_template_loader

logger = logging.getLogger(__name__)

HOOK_FILENAME = "_tagging_hook.py"

_TAGGABLE_SUBTYPES = {"streaming_table", "materialized_view"}
_SCHEMA_FILE_EXTS = {".yaml", ".yml", ".json"}

# Map of fqn -> {tag_key: tag_value}
TableTags = Dict[str, Dict[str, str]]
# Map of fqn -> {column_name: {tag_key: tag_value}}
ColumnTags = Dict[str, Dict[str, Dict[str, str]]]


class TaggingHookGenerator:
    """Generates ``_tagging_hook.py`` per pipeline."""

    def __init__(
        self, project_config: Optional[ProjectConfig], project_root: Path
    ) -> None:
        self.project_config = project_config
        self.project_root = project_root
        self._schema_parser = SchemaParser()
        self._jinja_env = Environment(  # nosec B701 — generates Python, not HTML
            loader=get_lhp_template_loader(),
            keep_trailing_newline=True,
        )

    @property
    def uc_tagging_config(self) -> UCTaggingConfig:
        """Resolved config; an absent block behaves as defaults (enabled)."""
        if self.project_config is None or self.project_config.uc_tagging is None:
            return UCTaggingConfig()
        return self.project_config.uc_tagging

    def build_hook_files(
        self,
        processed_flowgroups,
        pipeline_name: str,
        substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    ) -> Optional[Dict[str, str]]:
        """Render the tagging hook in memory, keyed by source-relative path.

        Returns ``None`` when ``uc_tagging`` is disabled, or when there is
        nothing meaningful to do after the empty-set inclusion rule (no entities
        with non-empty tags when additive; no managed entities at all when
        reconciling). Otherwise returns ``{HOOK_FILENAME: <content>}``.
        """
        config = self.uc_tagging_config
        if not config.enabled:
            return None

        table_tags, column_tags = self._build_tag_maps(
            processed_flowgroups, config.remove_undeclared_tags, substitution_mgr
        )

        if not table_tags and not column_tags:
            logger.debug(
                f"Pipeline '{pipeline_name}': no UC tags declared — "
                f"skipping tagging hook generation"
            )
            return None

        template = self._jinja_env.get_template("tagging/hook.py.j2")
        hook_content = template.render(
            pipeline_name=pipeline_name,
            table_tags_repr=repr(table_tags),
            column_tags_repr=repr(column_tags),
            remove_undeclared_tags=config.remove_undeclared_tags,
        )

        return {HOOK_FILENAME: hook_content}

    def generate(
        self,
        processed_flowgroups,
        pipeline_name: str,
        output_dir: Path,
        substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    ) -> Optional[str]:
        """Write the hook to ``output_dir``; return its content (or ``None``).

        Like the test-reporting hook, the file is written unformatted — the
        coordinator's single terminal ``ruff format`` pass formats it.
        """
        files = self.build_hook_files(
            processed_flowgroups=processed_flowgroups,
            pipeline_name=pipeline_name,
            substitution_mgr=substitution_mgr,
        )
        if files is None:
            return None

        dest = output_dir / HOOK_FILENAME
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_normalized(dest, files[HOOK_FILENAME])
        logger.info(f"Generated tagging hook: {dest}")

        return files[HOOK_FILENAME]

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _build_tag_maps(
        self,
        processed_flowgroups,
        remove_undeclared_tags: bool,
        substitution_mgr: Optional[EnhancedSubstitutionManager],
    ) -> Tuple[TableTags, ColumnTags]:
        table_tags: TableTags = {}
        column_tags: ColumnTags = {}

        for flowgroup in processed_flowgroups:
            for action in getattr(flowgroup, "actions", None) or []:
                if action.type != ActionType.WRITE or not action.write_target:
                    continue

                wt = self._wt_getter(action.write_target)
                if wt("type") not in _TAGGABLE_SUBTYPES:
                    continue
                if not action_creates_table(action) or wt("temporary", False):
                    continue

                catalog, schema, table = wt("catalog"), wt("schema"), wt("table")
                if not (catalog and schema and table):
                    continue
                fqn = self._sub(f"{catalog}.{schema}.{table}", substitution_mgr)

                raw_table_tags = wt("tags")
                if raw_table_tags is not None:
                    normalized = self._normalize_tags(raw_table_tags, substitution_mgr)
                    if normalized or remove_undeclared_tags:
                        table_tags[fqn] = normalized

                cols = self._load_column_tags(wt("table_schema"))
                if cols:
                    kept = {}
                    for col, tags in cols.items():
                        normalized = self._normalize_tags(tags, substitution_mgr)
                        if normalized or remove_undeclared_tags:
                            kept[col] = normalized
                    if kept:
                        column_tags[fqn] = kept

        return table_tags, column_tags

    def _load_column_tags(self, table_schema) -> Dict[str, Dict[str, str]]:
        """Load column tags from a YAML/JSON ``table_schema`` file, else ``{}``.

        Column tags are only honored for structured schema files (.yaml/.yml/
        .json); ``.sql``/``.ddl`` files and inline DDL strings carry no per-column
        tag structure and are skipped.
        """
        if not table_schema or not isinstance(table_schema, str):
            return {}
        if not is_file_path(table_schema):
            return {}
        if Path(table_schema).suffix.lower() not in _SCHEMA_FILE_EXTS:
            return {}

        resolved = resolve_external_file_path(
            table_schema, self.project_root, file_type="table schema file"
        )
        schema_data = self._schema_parser.parse_schema_file(resolved)
        return self._schema_parser.to_column_tags(schema_data)

    def _normalize_tags(
        self, raw, substitution_mgr: Optional[EnhancedSubstitutionManager]
    ) -> Dict[str, str]:
        """Coerce a tag dict to ``{str: str}`` (None -> ""), applying substitution."""
        result: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = self._sub(str(key), substitution_mgr)
            v = "" if value is None else self._sub(str(value), substitution_mgr)
            result[k] = v
        return result

    @staticmethod
    def _sub(text: str, substitution_mgr: Optional[EnhancedSubstitutionManager]) -> str:
        if substitution_mgr and isinstance(text, str) and "${" in text:
            return substitution_mgr._process_string(text)
        return text

    @staticmethod
    def _wt_getter(write_target):
        """Return a ``get(key, default=None)`` over a dict or WriteTarget model."""
        if isinstance(write_target, dict):
            return lambda key, default=None: write_target.get(key, default)
        return lambda key, default=None: getattr(write_target, key, default)
