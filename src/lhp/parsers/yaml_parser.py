# JUSTIFIED: YAML parse + cache + schema validation share a single
# document-tree representation; splitting requires either deep-copy
# at every boundary or a parser-internal mutability contract.

import logging
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from lhp.models import ActionType, FlowGroup, Preset, Template

from ..errors import ErrorFactory, LHPError, codes
from .yaml_loader import load_yaml_documents_all, load_yaml_file

_CacheValueT = TypeVar("_CacheValueT")


class YAMLParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def reserve_capacity(self, n: int) -> None:
        """No-op: a non-caching parser has no cache to size.

        Present so callers can size any parser uniformly without a
        capability check. The caching subclass overrides this.
        """

    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        self.logger.debug(f"Parsing YAML file: {file_path}")

        try:
            content = load_yaml_file(file_path, error_context=f"YAML file {file_path}")
            return content or {}
        except Exception as e:
            if isinstance(e, LHPError):
                raise
            if isinstance(e, ValueError):
                if "File not found" in str(e):
                    raise ErrorFactory.io_error(
                        codes.IO_004,
                        title="Error reading YAML file",
                        details=f"Error reading {file_path}: {e}",
                        suggestions=[
                            "Check the file path is correct",
                            "Ensure the file exists and is readable",
                        ],
                        context={"file": str(file_path)},
                    ) from e
                raise
            raise ErrorFactory.io_error(
                codes.IO_004,
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
        ``ErrorFactory.unknown_type_with_suggestion``.

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
                error = ErrorFactory.unknown_type_with_suggestion(
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
        """Supports both multi-document syntax (---) and flowgroups array syntax."""
        from .yaml_loader import load_yaml_documents_all

        documents = load_yaml_documents_all(
            file_path, error_context=f"flowgroup file {file_path}"
        )

        return self._flowgroups_from_documents(documents, file_path)

    def _flowgroups_from_documents(
        self, documents: List[Dict], file_path: Path
    ) -> List[FlowGroup]:
        if not documents:
            raise ErrorFactory.config_error(
                codes.CFG_005,
                title="Empty flowgroup file",
                details=f"No content found in {file_path}",
                suggestions=[
                    "Ensure the file contains valid YAML content",
                    "Check that the file is not empty",
                    "Verify the file has a 'flowgroup' key at the top level",
                ],
                context={"file": str(file_path)},
            )

        from .blueprint_parser import BlueprintParser

        is_multi_doc = len(documents) > 1
        self.logger.debug(
            f"Loaded {len(documents)} document(s) from {file_path}"
            f"{' (multi-document)' if is_multi_doc else ''}"
        )

        flowgroups = []
        seen_flowgroup_names = set()
        uses_array_syntax = False
        uses_regular_syntax = False

        for _doc_index, doc in enumerate(documents, start=1):
            # Defensive guard: catch a blueprint *definition* accidentally
            # placed under `include:` (pipelines/) instead of `blueprint_include:`.
            # Without this, the array-syntax path below would attempt to
            # construct a FlowGroup from a BlueprintFlowgroupSpec and crash with
            # an opaque error about missing `actions:` and unresolved %{var}.
            if BlueprintParser.looks_like_blueprint(doc):
                raise ErrorFactory.config_error(
                    codes.CFG_040,
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

                shared_fields = {k: v for k, v in doc.items() if k != "flowgroups"}

                for fg_config in doc["flowgroups"]:
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

                    fg_name = fg_config.get("flowgroup")
                    if fg_name in seen_flowgroup_names:
                        raise ErrorFactory.validation_error(
                            codes.VAL_013,
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

                    self._validate_action_types(fg_config, file_path)
                    flowgroups.append(FlowGroup(**fg_config))
            else:
                uses_regular_syntax = True

                fg_name = doc.get("flowgroup")
                if fg_name in seen_flowgroup_names:
                    raise ErrorFactory.validation_error(
                        codes.VAL_013,
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

                self._validate_action_types(doc, file_path)
                flowgroups.append(FlowGroup(**doc))

        self.logger.debug(
            f"Parsed {len(flowgroups)} flowgroup(s) from {file_path}"
            f" (syntax: {'array' if uses_array_syntax else 'regular'})"
        )

        if uses_array_syntax and uses_regular_syntax:
            raise ErrorFactory.validation_error(
                codes.VAL_014,
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

    def parse_template_raw(self, file_path: Path) -> Template:
        """Skip Action object creation so template syntax (e.g. ``{{ table_properties }}``)
        is not validated until rendering when actual parameter values are available.
        """
        content = self.parse_file(file_path)

        raw_actions = content.pop("actions", [])
        template = Template(**content, actions=raw_actions)
        template._raw_actions = True  # Set flag after creation
        return template

    def parse_preset(self, file_path: Path) -> Preset:
        """Parse a Preset YAML file."""
        content = self.parse_file(file_path)
        return Preset(**content)

    def discover_presets(self, presets_dir: Path) -> List[Preset]:
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
        self._parser: YAMLParser = base_parser or YAMLParser()
        self._cache: Dict[Tuple[str, float], List[FlowGroup]] = {}
        self._documents_cache: Dict[Tuple[str, float], List[Dict[str, Any]]] = {}
        self._max_cache_size: int = max_cache_size
        self._lock: threading.RLock = threading.RLock()
        self._hits: int = 0

    def reserve_capacity(self, n: int) -> None:
        """Grow-only size hint (``max(current, n)``): both discovery passes call
        this with their own glob size; a monotonic max ensures neither pass
        can shrink the ceiling mid-workload. FIFO eviction in ``_cached_load``
        still guards against unbounded growth.
        """
        with self._lock:
            self._max_cache_size = max(self._max_cache_size, n)

    def _cached_load(
        self,
        path: Path,
        cache: Dict[Tuple[str, float], _CacheValueT],
        loader: Callable[[], _CacheValueT],
        label: str,
    ) -> _CacheValueT:
        """Mtime-keyed lookup with FIFO eviction at ``max_cache_size``.
        ``loader`` is called on cache miss or when stat fails (OSError fast-path).
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

            if len(cache) >= self._max_cache_size:
                # Remove ~10% of entries (FIFO approximation)
                keys_to_remove = list(cache.keys())[: self._max_cache_size // 10]
                for key in keys_to_remove:
                    del cache[key]

            result: _CacheValueT = loader()
            cache[cache_key] = result
            return result

    def parse_flowgroups_from_file(self, path: Path) -> List[FlowGroup]:
        """On a ``_cache`` miss, documents are obtained via ``load_documents_all``
        so the raw read populates ``_documents_cache`` too; the instance pass then
        hits that entry instead of re-reading the file.
        """
        return self._cached_load(
            path,
            self._cache,
            lambda: self._parser._flowgroups_from_documents(
                self.load_documents_all(path, error_context=f"flowgroup file {path}"),
                path,
            ),
            label="Flowgroup",
        )

    def load_documents_all(
        self, path: Path, error_context: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Mtime-keyed cached load; hit/miss counters shared with the flowgroup sub-cache."""
        return self._cached_load(
            path,
            self._documents_cache,
            lambda: load_yaml_documents_all(path, error_context=error_context),
            label="Documents",
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._parser, name)
