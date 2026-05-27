# JUSTIFIED: YAML parse + cache + schema validation share a single
# document-tree representation; splitting requires either deep-copy
# at every boundary or a parser-internal mutability contract.
# TODO(Phase 9.5): re-evaluate after schema-versioning lands (Phase 11.7)

import logging
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar


from ..models.config import ActionType, FlowGroup, Preset, Template
from ..errors import (
    ErrorCategory,
    ErrorFormatter,
    LHPConfigError,
    LHPError,
    LHPValidationError,
)
from ..utils.yaml_loader import load_yaml_documents_all, load_yaml_file

_CacheValueT = TypeVar("_CacheValueT")


class YAMLParser:
    """Parse and validate YAML configuration files."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse a single YAML file."""
        self.logger.debug(f"Parsing YAML file: {file_path}")

        try:
            content = load_yaml_file(file_path, error_context=f"YAML file {file_path}")
            return content or {}
        except Exception as e:
            # Check if it's an LHPError that should be re-raised
            if isinstance(e, LHPError):
                raise  # Re-raise LHPError as-is
            elif isinstance(e, ValueError):
                # For backward compatibility, convert back to generic error for non-LHPErrors
                if "File not found" in str(e):
                    raise LHPConfigError(
                        category=ErrorCategory.IO,
                        code_number="004",
                        title="Error reading YAML file",
                        details=f"Error reading {file_path}: {e}",
                        suggestions=[
                            "Check the file path is correct",
                            "Ensure the file exists and is readable",
                        ],
                        context={"file": str(file_path)},
                    ) from e
                raise  # Re-raise ValueError as-is for YAML errors
            else:
                raise LHPConfigError(
                    category=ErrorCategory.IO,
                    code_number="004",
                    title="Error reading YAML file",
                    details=f"Error reading {file_path}: {e}",
                    suggestions=[
                        "Check the file path is correct",
                        "Ensure the file exists and is readable",
                        "Verify the YAML syntax is correct",
                    ],
                    context={"file": str(file_path)},
                ) from e

    def _validate_action_types(self, doc: Dict[str, Any], file_path: Path) -> None:
        """Pre-check ``type:`` on every action before Pydantic constructs FlowGroup.

        Pydantic's own validation rejects unknown action types, but produces a
        generic ValidationError. Doing the check here lets us emit a friendlier
        LHP-ACT-001 with a did-you-mean suggestion sourced from
        ``ErrorFormatter.unknown_type_with_suggestion``.

        Args:
            doc: A single parsed YAML document (one flowgroup's worth, or an
                array-syntax shared-fields document — both expose ``actions:``
                the same way when iterated on a per-flowgroup basis).
            file_path: Source file path, surfaced in the resulting error
                context for user diagnostics.

        Raises:
            LHPConfigError: When an action's ``type:`` value is not a member
                of :class:`ActionType`.
        """
        valid_values = [t.value for t in ActionType]
        actions = doc.get("actions") or []
        if not isinstance(actions, list):
            return  # Let Pydantic emit the structural error for non-list actions
        for action in actions:
            if not isinstance(action, dict):
                continue
            action_type = action.get("type")
            if action_type is None:
                continue  # Missing type: Pydantic enforces required field
            if action_type not in valid_values:
                error = ErrorFormatter.unknown_type_with_suggestion(
                    value_type="action type",
                    provided_value=str(action_type),
                    valid_values=valid_values,
                    example_usage="""actions:
  - name: my_action
    type: load  # Valid types: load, transform, write, test""",
                )
                error.context["file"] = str(file_path)
                action_name = action.get("name")
                if action_name:
                    error.context["action"] = action_name
                raise error

    def parse_flowgroups_from_file(self, file_path: Path) -> List[FlowGroup]:
        """Parse one or more FlowGroups from a YAML file.

        Supports both multi-document syntax (---) and flowgroups array syntax.

        Args:
            file_path: Path to YAML file containing one or more flowgroups

        Returns:
            List of FlowGroup objects

        Raises:
            ValueError: For duplicate flowgroup names, mixed syntax, or parsing errors
        """
        from ..utils.yaml_loader import load_yaml_documents_all
        from .blueprint_parser import BlueprintParser

        try:
            documents = load_yaml_documents_all(
                file_path, error_context=f"flowgroup file {file_path}"
            )
        except ValueError:
            # Re-raise with better context
            raise

        if not documents:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="005",
                title="Empty flowgroup file",
                details=f"No content found in {file_path}",
                suggestions=[
                    "Ensure the file contains valid YAML content",
                    "Check that the file is not empty",
                    "Verify the file has a 'flowgroup' key at the top level",
                ],
                context={"file": str(file_path)},
            )

        is_multi_doc = len(documents) > 1
        self.logger.debug(
            f"Loaded {len(documents)} document(s) from {file_path}"
            f"{' (multi-document)' if is_multi_doc else ''}"
        )

        flowgroups = []
        seen_flowgroup_names = set()
        uses_array_syntax = False
        uses_regular_syntax = False

        # Process each document
        for doc_index, doc in enumerate(documents, start=1):
            # Defensive guard: catch a blueprint *definition* accidentally
            # placed under `include:` (pipelines/) instead of `blueprint_include:`.
            # Without this, the array-syntax path below would attempt to
            # construct a FlowGroup from a BlueprintFlowgroupSpec and crash with
            # an opaque error about missing `actions:` and unresolved %{var}.
            if BlueprintParser.looks_like_blueprint(doc):
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="040",
                    title="Blueprint file in flowgroup directory",
                    details=(
                        f"{file_path} appears to be a blueprint (has 'parameters' "
                        "and 'flowgroups' but no 'actions'). Blueprint files must "
                        "live under blueprints/ (or your configured "
                        "blueprint_include patterns), not in pipelines/."
                    ),
                    suggestions=[
                        f"Move {file_path} to blueprints/",
                        "Or adjust 'include:' / 'blueprint_include:' patterns "
                        "in lhp.yaml",
                    ],
                    context={"file": str(file_path)},
                )

            # Routing: instance files (use_blueprint or legacy blueprint+flat)
            # may live alongside flowgroups under pipelines/. Skip them here so
            # the BlueprintDiscoverer can pick them up via instance_include.
            if BlueprintParser.looks_like_instance(doc):
                self.logger.debug(
                    f"Skipping instance file {file_path} during flowgroup parse "
                    "(routed to BlueprintDiscoverer)"
                )
                return []

            # Check if this document uses array syntax
            if "flowgroups" in doc:
                uses_array_syntax = True

                # Extract document-level shared fields
                shared_fields = {k: v for k, v in doc.items() if k != "flowgroups"}

                # Process each flowgroup in the array
                for fg_config in doc["flowgroups"]:
                    # Apply inheritance: only inherit if key not present in fg_config
                    inheritable_fields = [
                        "pipeline",
                        "use_template",
                        "presets",
                        "operational_metadata",
                        "job_name",
                    ]
                    for field in inheritable_fields:
                        if field not in fg_config and field in shared_fields:
                            fg_config[field] = shared_fields[field]

                    # Check for duplicate flowgroup name
                    fg_name = fg_config.get("flowgroup")
                    if fg_name in seen_flowgroup_names:
                        raise LHPValidationError(
                            category=ErrorCategory.VALIDATION,
                            code_number="013",
                            title=f"Duplicate flowgroup name '{fg_name}'",
                            details=f"Duplicate flowgroup name '{fg_name}' in file {file_path}. Each flowgroup must have a unique name.",
                            suggestions=[
                                f"Rename one of the '{fg_name}' flowgroups to a unique name",
                                "Check for copy-paste errors in the flowgroups array",
                            ],
                            context={"file": str(file_path), "flowgroup": fg_name},
                        )
                    if fg_name:
                        seen_flowgroup_names.add(fg_name)

                    # Pre-check action types (better error than Pydantic's generic
                    # ValidationError; emits LHP-ACT-001 with did-you-mean).
                    self._validate_action_types(fg_config, file_path)

                    # Build the flowgroup. LHPError subclasses (e.g. the pre-check
                    # above, or LHPValidationError raised inside Pydantic validators)
                    # already carry structured context — let them propagate as-is.
                    # Pydantic's ValidationError also propagates and is detailed
                    # enough for the user; no need to wrap it.
                    flowgroups.append(FlowGroup(**fg_config))
            else:
                # Regular syntax (one flowgroup per document)
                uses_regular_syntax = True

                # Check for duplicate flowgroup name
                fg_name = doc.get("flowgroup")
                if fg_name in seen_flowgroup_names:
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="013",
                        title=f"Duplicate flowgroup name '{fg_name}'",
                        details=f"Duplicate flowgroup name '{fg_name}' in file {file_path}. Each flowgroup must have a unique name.",
                        suggestions=[
                            f"Rename one of the '{fg_name}' flowgroups to a unique name",
                            "Check for copy-paste errors in the multi-document YAML",
                        ],
                        context={"file": str(file_path), "flowgroup": fg_name},
                    )
                if fg_name:
                    seen_flowgroup_names.add(fg_name)

                # Pre-check action types (see array-syntax branch above).
                self._validate_action_types(doc, file_path)

                # See array-syntax branch comments above.
                flowgroups.append(FlowGroup(**doc))

        self.logger.debug(
            f"Parsed {len(flowgroups)} flowgroup(s) from {file_path}"
            f" (syntax: {'array' if uses_array_syntax else 'regular'})"
        )

        # Check for mixed syntax
        if uses_array_syntax and uses_regular_syntax:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="014",
                title="Mixed flowgroup syntax",
                details=f"Mixed syntax detected in {file_path}: cannot use both multi-document (---) and flowgroups array syntax in the same file.",
                suggestions=[
                    "Use multi-document syntax (--- separators) OR flowgroups array syntax, not both",
                    "Multi-document: separate flowgroups with '---' on its own line",
                    "Array syntax: use 'flowgroups:' key with a list of flowgroups",
                ],
                context={"file": str(file_path)},
            )

        return flowgroups

    def parse_flowgroup(self, file_path: Path) -> FlowGroup:
        """Parse a FlowGroup YAML file.

        Note: This method only supports single-flowgroup files. If the file contains
        multiple flowgroups (via --- separator or flowgroups array), use
        parse_flowgroups_from_file() instead.
        """
        from ..utils.yaml_loader import load_yaml_documents_all

        # Check if file contains multiple flowgroups
        try:
            documents = load_yaml_documents_all(file_path)
        except ValueError as e:
            # If we can't even load it, fall back to original behavior
            self.logger.debug(
                f"Multi-document load failed for {file_path}, falling back to single-document parse: {e}"
            )
            content = self.parse_file(file_path)
            return FlowGroup(**content)

        # Check for multiple documents
        if len(documents) > 1:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="015",
                title="Multiple documents in single-flowgroup parse",
                details=f"File {file_path} contains multiple flowgroups (multiple documents). Use parse_flowgroups_from_file() instead.",
                suggestions=[
                    "Use parse_flowgroups_from_file() for multi-document YAML files",
                    "Split into separate files if single-flowgroup parsing is needed",
                ],
                context={"file": str(file_path)},
            )

        # Check for array syntax
        if documents and "flowgroups" in documents[0]:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="015",
                title="Array syntax in single-flowgroup parse",
                details=f"File {file_path} contains multiple flowgroups (array syntax). Use parse_flowgroups_from_file() instead.",
                suggestions=[
                    "Use parse_flowgroups_from_file() for array-syntax YAML files",
                    "Split into separate files if single-flowgroup parsing is needed",
                ],
                context={"file": str(file_path)},
            )

        # Single flowgroup - use original parsing
        content = self.parse_file(file_path)
        return FlowGroup(**content)

    def parse_template_raw(self, file_path: Path) -> Template:
        """Parse a Template YAML file with raw actions (no Action object creation).

        This is used during template loading to avoid validation of template syntax
        like {{ table_properties }}. Actions will be validated later during rendering
        when actual parameter values are available.
        """
        content = self.parse_file(file_path)

        # Create template with raw actions
        raw_actions = content.pop("actions", [])
        template = Template(**content, actions=raw_actions)
        template._raw_actions = True  # Set flag after creation
        return template

    def parse_preset(self, file_path: Path) -> Preset:
        """Parse a Preset YAML file."""
        content = self.parse_file(file_path)
        return Preset(**content)

    def discover_presets(self, presets_dir: Path) -> List[Preset]:
        """Discover all Preset files."""
        self.logger.debug(f"Discovering presets in {presets_dir}")
        presets = []
        for yaml_file in presets_dir.glob("*.yaml"):
            if yaml_file.is_file():
                try:
                    preset = self.parse_preset(yaml_file)
                    presets.append(preset)
                except Exception as e:
                    self.logger.warning(f"Could not parse preset {yaml_file}: {e}")
        return presets


class CachingYAMLParser:
    """Thread-safe caching wrapper for YAMLParser.

    Uses file path + modification time as cache key to automatically
    invalidate cache when files change.
    """

    def __init__(
        self, base_parser: Optional["YAMLParser"] = None, max_cache_size: int = 500
    ) -> None:
        """Initialize caching parser.

        Args:
            base_parser: Underlying YAMLParser instance (creates new if None)
            max_cache_size: Maximum number of cached entries
        """
        self._parser: YAMLParser = base_parser or YAMLParser()
        self._cache: Dict[Tuple[str, float], List[FlowGroup]] = {}
        self._documents_cache: Dict[Tuple[str, float], List[Dict[str, Any]]] = {}
        self._max_cache_size: int = max_cache_size
        self._lock: threading.RLock = threading.RLock()
        self._hits: int = 0
        self._misses: int = 0

    def _cached_load(
        self,
        path: Path,
        cache: Dict[Tuple[str, float], _CacheValueT],
        loader: Callable[[], _CacheValueT],
        label: str,
    ) -> _CacheValueT:
        """Cache-shell shared by every sub-cache: mtime-keyed lookup, FIFO
        eviction at the shared ``max_cache_size`` ceiling, hit/miss counters.

        ``loader`` is the no-arg fallback invoked on cache miss (and on the
        OSError fast-path where the key can't be computed).
        """
        resolved_path: Path = path.resolve()
        try:
            mtime: float = resolved_path.stat().st_mtime
        except OSError as e:
            self._parser.logger.debug(
                f"Could not stat {resolved_path}, skipping cache: {e}"
            )
            return loader()

        cache_key: Tuple[str, float] = (str(resolved_path), mtime)

        with self._lock:
            if cache_key in cache:
                self._hits += 1
                self._parser.logger.debug(
                    f"{label} cache hit for {path} (hits={self._hits})"
                )
                return cache[cache_key]

            self._misses += 1

            if len(cache) >= self._max_cache_size:
                # Remove ~10% of entries (FIFO approximation)
                keys_to_remove = list(cache.keys())[: self._max_cache_size // 10]
                for key in keys_to_remove:
                    del cache[key]

            result: _CacheValueT = loader()
            cache[cache_key] = result
            return result

    def parse_flowgroups_from_file(self, path: Path) -> List[FlowGroup]:
        """Parse flowgroups with caching based on file mtime."""
        return self._cached_load(
            path,
            self._cache,
            lambda: self._parser.parse_flowgroups_from_file(path),
            label="Flowgroup",
        )

    def load_documents_all(
        self, path: Path, error_context: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Load all YAML documents from a file with mtime-keyed caching.

        Hit/miss counters are shared with the flowgroup sub-cache;
        ``get_cache_stats()`` reports per-sub-cache sizes separately.
        """
        return self._cached_load(
            path,
            self._documents_cache,
            lambda: load_yaml_documents_all(path, error_context=error_context),
            label="Documents",
        )

    def clear_cache(self) -> None:
        """Clear all cached entries across both sub-caches."""
        with self._lock:
            self._cache.clear()
            self._documents_cache.clear()
            self._hits = 0
            self._misses = 0

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics.

        Hit/miss counters are aggregated across both sub-caches (flowgroup
        parsing and raw-document loading). Sub-cache sizes are reported
        separately for visibility.

        Returns:
            Dictionary with cache hits, misses, hit rate, and per-sub-cache sizes
        """
        with self._lock:
            total: int = self._hits + self._misses
            hit_rate: float = (self._hits / total * 100) if total > 0 else 0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "total": total,
                "hit_rate_percent": round(hit_rate, 1),
                "cache_size": len(self._cache),
                "documents_cache_size": len(self._documents_cache),
            }

    def __getattr__(self, name: str) -> Any:
        """Delegate other methods to base parser.

        Args:
            name: Attribute name

        Returns:
            Attribute from base parser
        """
        return getattr(self._parser, name)
