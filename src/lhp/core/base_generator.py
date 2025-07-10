from abc import ABC, abstractmethod
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from typing import Dict, Any, Set, List, TYPE_CHECKING
import yaml
import json

if TYPE_CHECKING:
    from ..models.config import Action


class BaseActionGenerator(ABC):
    """Base class for all action generators."""

    def __init__(self):
        self._imports: Set[str] = set()
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
    def generate(self, action: "Action", context: Dict[str, Any]) -> str:
        """Generate code for the action."""
        pass

    def add_import(self, import_stmt: str):
        """Add import statement."""
        self._imports.add(import_stmt)

    @property
    def imports(self) -> List[str]:
        """Get sorted imports."""
        return sorted(self._imports)

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render Jinja2 template."""
        template = self.env.get_template(template_name)
        return template.render(**context)
