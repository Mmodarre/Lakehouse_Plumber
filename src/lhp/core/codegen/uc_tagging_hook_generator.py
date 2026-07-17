"""Generator for the Unity Catalog tagging event hook.

Produces a single ``_uc_tagging_hook.py`` per pipeline that uses
``@dp.on_event_hook`` to apply UC tags to managed tables (and their columns)
during the pipeline update — on ``update_progress`` ``RUNNING`` (streaming tables)
and the terminal states (materialized views, which materialise later) — so
tag-write failures surface as event-log warnings while the run is live. Tags are
applied via the Entity Tag Assignments REST API (``ALTER TABLE SET TAGS`` is
rejected inside pipelines).

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
from ...errors import ErrorFactory, codes
from ...parsers.schema_parser import SchemaParser
from ...parsers.tags_file_parser import parse_tags_file
from ...utils.file_header import write_normalized
from ..processing.substitution import EnhancedSubstitutionManager
from ..validators.compatibility.table_creation import action_creates_table
from .template_renderer import get_lhp_template_loader

logger = logging.getLogger(__name__)

HOOK_FILENAME = "_uc_tagging_hook.py"

_TAGGABLE_SUBTYPES = {"streaming_table", "materialized_view"}
_SCHEMA_FILE_EXTS = {".yaml", ".yml", ".json"}

# Map of fqn -> {tag_key: tag_value}
TableTags = Dict[str, Dict[str, str]]
# Map of fqn -> {column_name: {tag_key: tag_value}}
ColumnTags = Dict[str, Dict[str, Dict[str, str]]]


class UCTaggingHookGenerator:
    """Generates ``_uc_tagging_hook.py`` per pipeline."""

    def __init__(
        self,
        project_config: Optional[ProjectConfig],
        project_root: Path,
        sandbox_active: bool = False,
    ) -> None:
        self.project_config = project_config
        self.project_root = project_root
        self._sandbox_active = sandbox_active
        self._schema_parser = SchemaParser()
        self._jinja_env = Environment(  # nosec B701 — generates Python, not HTML
            loader=get_lhp_template_loader(),
            keep_trailing_newline=True,
        )

    @property
    def uc_tagging_config(self) -> UCTaggingConfig:
        """Resolved ``uc_tagging`` config; an absent block defaults to enabled.

        The feature is **on by default**: a missing ``uc_tagging`` block behaves as
        ``UCTaggingConfig()`` (enabled). Auto-detection still gates emission — the
        hook is only written when some table/column declares ``tags``. Set
        ``uc_tagging.enabled: false`` in lhp.yaml to disable.
        """
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

        template = self._jinja_env.get_template("uc_tagging/hook.py.j2")
        hook_content = template.render(
            pipeline_name=pipeline_name,
            table_tags_repr=repr(table_tags),
            column_tags_repr=repr(column_tags),
            remove_undeclared_tags=config.remove_undeclared_tags,
            tag_update_concurrency=config.tag_update_concurrency,
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

    @staticmethod
    def _wt_getter(write_target):
        """Return a ``get(key, default=None)`` over a dict or WriteTarget model."""
        if isinstance(write_target, dict):
            return lambda key, default=None: write_target.get(key, default)
        return lambda key, default=None: getattr(write_target, key, default)

    @staticmethod
    def _sub(text: str, substitution_mgr: Optional[EnhancedSubstitutionManager]) -> str:
        if substitution_mgr and isinstance(text, str) and "${" in text:
            return substitution_mgr._process_string(text)
        return text

    # UC tag KEYS may not contain any of these four characters. ('.' and '/' are
    # permitted: '.' appears in system-governed tag keys and '/' in subdomain keys.)
    _KEY_PROHIBITED_CHARS = ",-=:"
    _TAG_MAX_LEN = 256

    def _normalize_tags(
        self,
        raw,
        substitution_mgr: Optional[EnhancedSubstitutionManager],
        context: Optional[str] = None,
    ) -> Dict[str, str]:
        """Coerce a tag dict to ``{str: str}`` (None -> ""), applying substitution.

        The materialized (post-substitution, post-coercion) key/value of every tag
        is validated against Unity Catalog's charset/length rules; an illegal tag
        raises ``LHP-CFG-066``. ``context`` labels the offending entity in the
        message (table FQN or ``fqn.column``); it degrades to ``"<unknown>"`` when
        absent so other callers keep working.
        """
        label = context or "<unknown>"
        result: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = self._sub(str(key), substitution_mgr)
            v = "" if value is None else self._sub(str(value), substitution_mgr)
            self._validate_tag(k, v, label)
            result[k] = v
        return result

    @classmethod
    def _validate_tag(cls, key: str, value: str, context: str) -> None:
        """Reject a materialized tag key/value that violates UC's rules.

        KEY: illegal if it contains any of ``, - = :``, has leading/trailing
        whitespace, or exceeds 256 characters. VALUE: illegal if it has
        leading/trailing whitespace or exceeds 256 characters (charset is
        unrestricted). Raises ``LHP-CFG-066`` on the first violation found.
        """
        key_reason = cls._tag_violation(key, check_charset=True)
        if key_reason is not None:
            cls._raise_illegal_tag("key", key, context, key_reason)

        value_reason = cls._tag_violation(value, check_charset=False)
        if value_reason is not None:
            cls._raise_illegal_tag("value", value, context, value_reason)

    @classmethod
    def _tag_violation(cls, text: str, *, check_charset: bool) -> Optional[str]:
        """Return the reason ``text`` is an illegal tag key/value, else ``None``."""
        if check_charset:
            for char in text:
                if char in cls._KEY_PROHIBITED_CHARS:
                    return f"contains the prohibited character {char!r}"
        if text != text.strip():
            return "has leading or trailing whitespace"
        if len(text) > cls._TAG_MAX_LEN:
            return "exceeds 256 characters"
        return None

    @staticmethod
    def _raise_illegal_tag(part: str, value: str, context: str, reason: str):
        raise ErrorFactory.config_error(
            codes.CFG_066,
            title="Illegal UC tag key/value",
            details=f"UC tag {part} {value!r} on {context} is illegal: {reason}.",
            suggestions=[
                "UC tag keys may not contain any of: , - = :",
                "Keep keys and values <= 256 chars with no leading/trailing whitespace",
            ],
        )

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

    def _iter_taggable_writes(self, processed_flowgroups, substitution_mgr):
        """Yield ``(write_target_getter, fqn)`` for each WRITE action that creates a
        taggable (streaming_table / materialized_view), non-temporary table with a
        fully-qualified name.
        """
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

                yield wt, self._sub(f"{catalog}.{schema}.{table}", substitution_mgr)

    def _collect_table_tags(self, wt, remove_undeclared_tags, substitution_mgr, fqn):
        """Normalized table tags to embed, or ``None`` to omit the table.

        Tags come from an external ``tags_file`` sidecar when set, else from
        inline ``tags``. Validation guarantees at most one of the two is set
        (§9.24), so no both-set check is needed here. When ``tags_file`` is set,
        the sidecar's literal ``table:`` must equal the write target's table
        name — skipped under ``--sandbox`` (``self._sandbox_active``), where
        tables are renamed.

        Returns ``{}`` (kept) for an explicit empty tag set under reconcile mode;
        ``None`` when no tags are declared, or empty-and-additive (nothing to do).
        """
        tags_file = wt("tags_file")
        if tags_file:
            resolved = resolve_external_file_path(
                tags_file, self.project_root, file_type="tags file"
            )
            declared_table, raw = parse_tags_file(resolved)
            if not self._sandbox_active:
                actual_table = self._sub(str(wt("table") or ""), substitution_mgr)
                if declared_table != actual_table:
                    raise ErrorFactory.config_error(
                        codes.CFG_067,
                        title="Tags file table mismatch",
                        details=(
                            f"Tags file '{tags_file}' declares table "
                            f"'{declared_table}', but the write target's table "
                            f"is '{actual_table}'."
                        ),
                        suggestions=[
                            "Set the tags file's 'table:' to the write target's "
                            "table name",
                            "Or point 'tags_file' at the sidecar for this table",
                        ],
                        example=f"table: {actual_table}",
                        context={
                            "Tags file": tags_file,
                            "Write target table": actual_table,
                        },
                    )
        else:
            raw = wt("tags")
        if raw is None:
            return None
        normalized = self._normalize_tags(raw, substitution_mgr, context=fqn)
        if normalized or remove_undeclared_tags:
            return normalized
        return None

    def _collect_column_tags(self, wt, remove_undeclared_tags, substitution_mgr, fqn):
        """Normalized ``{column: {k: v}}`` for the action's schema file; columns are
        kept only when they have tags (or under reconcile mode, an empty set).
        """
        kept: Dict[str, Dict[str, str]] = {}
        for col, tags in self._load_column_tags(wt("table_schema")).items():
            normalized = self._normalize_tags(
                tags, substitution_mgr, context=f"{fqn}.{col}"
            )
            if normalized or remove_undeclared_tags:
                kept[col] = normalized
        return kept

    def _build_tag_maps(
        self,
        processed_flowgroups,
        remove_undeclared_tags: bool,
        substitution_mgr: Optional[EnhancedSubstitutionManager],
    ) -> Tuple[TableTags, ColumnTags]:
        table_tags: TableTags = {}
        column_tags: ColumnTags = {}

        for wt, fqn in self._iter_taggable_writes(
            processed_flowgroups, substitution_mgr
        ):
            tbl = self._collect_table_tags(
                wt, remove_undeclared_tags, substitution_mgr, fqn
            )
            if tbl is not None:
                table_tags[fqn] = tbl

            cols = self._collect_column_tags(
                wt, remove_undeclared_tags, substitution_mgr, fqn
            )
            if cols:
                column_tags[fqn] = cols

        return table_tags, column_tags
