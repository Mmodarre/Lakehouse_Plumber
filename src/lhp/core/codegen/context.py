"""Per-action generator context builder + post-call output collector.

:stability: internal
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from .python_file_copier import CopiedModuleRecord
from lhp.models import FlowGroup
from ..processing.substitution import EnhancedSubstitutionManager


class GenerationContextBuilder:
    """Build the context dict passed into every action generator + collect outputs.

    ``project_config`` and ``project_root`` are stored once on the instance
    (they are project-scoped — they do not change across flowgroups in the
    same run). All other inputs to :meth:`build` are flowgroup-scoped and
    flow through as method arguments.

    :stability: internal
    """

    def __init__(
        self,
        *,
        project_config=None,
        project_root: Optional[Path] = None,
    ) -> None:
        self.project_config = project_config
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        # Per-builder signature cache shared across every flowgroup context
        # this builder produces. Lifecycle is per-builder-instance: the
        # coordinator constructs one builder per pipeline run, and a
        # spawn-pool worker constructs its own builder, so this dict is
        # effectively per-pipeline / per-worker. Phase A is serial, so no
        # lock is needed. Keyed by the *resolved absolute path* (str) of the
        # source-function file; downstream tasks that write into the cache
        # MUST use that same key convention. Value type is the parsed
        # signature record (``FunctionSignature``, defined in
        # ``generators/write/``); it is typed ``Any`` here because importing
        # that type would invert the §5 layering direction (``core/codegen``
        # must not import ``generators/``). This module is ``:stability:
        # internal`` so strict-mypy (§4.3, binds only ``lhp/api/``) does not
        # gate the loosened type.
        self._source_function_signature_cache: Dict[str, Any] = {}

    def build(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        """Build context dictionary for generator execution."""
        project_root = self.project_root or Path.cwd()
        return {
            "flowgroup": flowgroup,
            "substitution_manager": substitution_mgr,
            "spec_dir": project_root,  # For backward compatibility
            "project_root": project_root,  # Explicit project root for external file loading
            "preset_config": preset_config,
            "project_config": self.project_config,
            "output_dir": output_dir,
            "source_yaml": source_yaml,
            "environment": env,
            # Per-flowgroup accumulator for secret references collected by
            # generators that run their own _process_string calls. The
            # substitution manager keeps the canonical set; this mirror is
            # populated by generators for legacy callers that read from
            # the context dict.
            "secret_references": set(),
            # When present, ``copy_user_module_for_pipeline`` appends
            # CopiedModuleRecord entries here instead of writing to disk, so
            # writes can be replayed on the main thread.
            "phase_a_records": phase_a_records,
            # Inline auxiliary Python modules carried on the FlowGroupContext
            # (e.g. monitoring's jobs_stats_loader.py). Read by
            # ``copy_user_module_for_pipeline`` to skip on-disk lookup.
            "auxiliary_files": auxiliary_files or {},
            # Per-builder signature cache (same dict object across every
            # flowgroup context this builder produces). Keyed by the resolved
            # absolute path (str) of the source-function file; parse + signature
            # extraction happens once per unique source file per pipeline, and
            # the per-flowgroup work is the cheap param check. See __init__ for
            # the §4.3 type rationale.
            "source_function_signature_cache": self._source_function_signature_cache,
        }

    def collect_outputs(self, generator) -> Tuple[Set[str], Set[str]]:
        """Collect imports and pre-pipeline statements from a generator."""
        imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        import_manager = getattr(generator, "get_import_manager", lambda: None)()
        if import_manager:
            consolidated_imports = import_manager.get_consolidated_imports()
            imports.update(consolidated_imports)
            self.logger.debug(
                f"Used ImportManager: {len(consolidated_imports)} imports"
            )
        else:
            imports.update(generator.imports)

        # Collect pre-pipeline statements (e.g. cloudpickle registration for
        # custom data sources/sinks).
        get_pre = getattr(generator, "get_pre_pipeline_statements", None)
        if callable(get_pre):
            pre_pipeline_statements.update(get_pre())

        return imports, pre_pipeline_statements
