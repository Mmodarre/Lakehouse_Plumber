from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from typing import Dict, Any, Set, List, TYPE_CHECKING, Optional
import yaml
import json

if TYPE_CHECKING:
    from ..models.config import Action
    from ..utils.import_manager import ImportManager


class BaseActionGenerator(ABC):
    """Base class for all action generators."""

    def __init__(self, use_import_manager: bool = False):
        # Legacy import collection (backward compatible)
        self._imports: Set[str] = set()
        
        # Optional ImportManager integration (new functionality)
        self._use_import_manager = use_import_manager
        self._import_manager: Optional['ImportManager'] = None
        
        if self._use_import_manager:
            from ..utils.import_manager import ImportManager
            self._import_manager = ImportManager()
        
        # Template setup
        pkg_dir = Path(__file__).parent.parent
        template_dir = pkg_dir / "templates"
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        # Add filters
        self.env.filters["tojson"] = json.dumps
        self.env.filters["toyaml"] = yaml.dump

    @abstractmethod
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate code for the action."""
        pass

    def add_import(self, import_stmt: str):
        """
        Add import statement (backward compatible).
        
        Routes to ImportManager if enabled, otherwise uses legacy collection.
        """
        if self._use_import_manager and self._import_manager:
            self._import_manager.add_import(import_stmt)
        else:
            self._imports.add(import_stmt)

    @property
    def imports(self) -> List[str]:
        """
        Get sorted imports (backward compatible).
        
        Returns ImportManager consolidated imports if enabled, 
        otherwise returns legacy sorted imports.
        """
        if self._use_import_manager and self._import_manager:
            return self._import_manager.get_consolidated_imports()
        else:
            return sorted(self._imports)
    
    def add_imports_from_expression(self, expression: str):
        """
        Add imports from PySpark expressions (new functionality).
        
        Only available when ImportManager is enabled.
        """
        if self._use_import_manager and self._import_manager:
            self._import_manager.add_imports_from_expression(expression)
        else:
            # Graceful fallback - ignore if not using ImportManager
            pass
    
    def add_imports_from_file(self, source_code: str) -> str:
        """
        Extract imports from file and return cleaned source (new functionality).
        
        Only available when ImportManager is enabled.
        Returns original source if ImportManager not enabled.
        """
        if self._use_import_manager and self._import_manager:
            return self._import_manager.add_imports_from_file(source_code)
        else:
            # Graceful fallback - return source unchanged
            return source_code
    
    def get_import_manager(self) -> Optional['ImportManager']:
        """
        Get the ImportManager instance (if enabled).
        
        Returns None if ImportManager not enabled.
        """
        return self._import_manager if self._use_import_manager else None

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render Jinja2 template."""
        template = self.env.get_template(template_name)
        return template.render(**context)
