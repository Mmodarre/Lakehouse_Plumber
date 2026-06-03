from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from ...utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from lhp.models import Action

    from ..codegen.imports.manager import ImportManager


class BaseActionGenerator(ABC):
    def __init__(self, use_import_manager: bool = False):
        # Legacy import collection (backward compatible)
        self._imports: Set[str] = set()

        self._use_import_manager = use_import_manager
        self._import_manager: Optional["ImportManager"] = None

        if self._use_import_manager:
            from ..codegen.imports.manager import ImportManager

            self._import_manager = ImportManager()

        # Statements to emit between the imports block and PIPELINE_ID in the
        # assembled flowgroup file. Used for module-load-time setup that must
        # run after imports but before the pipeline body — e.g. cloudpickle
        # ``register_pickle_by_value`` for custom data sources/sinks.
        self._pre_pipeline_statements: List[str] = []

        # Deferred to avoid registry → codegen → generators → registry cycle.
        # The generator Environment is process-local and shared across all
        # generators so template compilation is amortized (see
        # get_shared_generator_environment). It is read-only after construction;
        # no generator mutates self.env, so sharing is safe.
        from ..codegen.template_renderer import get_shared_generator_environment

        self.env = get_shared_generator_environment()

    @abstractmethod
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        pass

    def add_import(self, import_stmt: str):
        """Routes to ImportManager if enabled, otherwise uses legacy collection."""
        if self._use_import_manager and self._import_manager:
            self._import_manager.add_import(import_stmt)
        else:
            self._imports.add(import_stmt)

    @property
    def imports(self) -> List[str]:
        """Returns ImportManager consolidated imports if enabled, otherwise legacy sorted imports."""
        if self._use_import_manager and self._import_manager:
            return self._import_manager.get_consolidated_imports()
        return sorted(self._imports)

    def add_imports_from_expression(self, expression: str):
        """Only available when ImportManager is enabled."""
        if self._use_import_manager and self._import_manager:
            self._import_manager.add_imports_from_expression(expression)
        else:
            pass

    def add_pre_pipeline_statement(self, stmt: str) -> None:
        """Add a statement to be emitted between imports and ``PIPELINE_ID``.

        Used for module-load-time setup that must run after imports but before
        the pipeline body — e.g. ``cloudpickle.register_pickle_by_value(...)``
        for custom data sources/sinks. The assembler dedupes across all
        generators, so the same statement collected from multiple actions
        within a flowgroup appears exactly once in the assembled file.
        """
        if stmt and stmt.strip():
            self._pre_pipeline_statements.append(stmt.strip())

    def get_pre_pipeline_statements(self) -> List[str]:
        return list(self._pre_pipeline_statements)

    def get_import_manager(self) -> Optional["ImportManager"]:
        """Returns None if ImportManager is not enabled."""
        return self._import_manager if self._use_import_manager else None

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        with perf_timer(f"jinja_render [{template_name}]", category="jinja_render"):
            template = self.env.get_template(template_name)
            return template.render(**context)

    def _get_operational_metadata(
        self, action: Action, context: Dict[str, Any], target_type: str = "view"
    ) -> tuple:
        """Centralized operational metadata lookup via OperationalMetadataService."""
        from ..codegen.operational_metadata import OperationalMetadataService

        flowgroup = context.get("flowgroup")
        preset_config = context.get("preset_config", {})
        project_config = context.get("project_config")

        service = OperationalMetadataService()
        add_metadata, metadata_columns, metadata_imports = (
            service.get_metadata_and_imports(
                action=action,
                flowgroup=flowgroup,
                preset_config=preset_config,
                project_config=project_config,
                target_type=target_type,
                import_manager=self.get_import_manager(),
            )
        )

        for import_stmt in metadata_imports:
            self.add_import(import_stmt)

        # If using ImportManager, also register expressions for semantic tracking
        # Maintains consistency: files→_file_imports, expressions→_expression_imports
        if self._use_import_manager and self._import_manager and metadata_columns:
            for _col_name, expression in metadata_columns.items():
                self.add_imports_from_expression(expression)

        return add_metadata, metadata_columns
