"""Template context for project initialization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class InitTemplateContext:
    """Context object containing all variables for init template rendering.
    
    Attributes:
        project_name: Name of the project being initialized
        current_date: ISO formatted current date/time
        author: Author name (empty by default)
        bundle_enabled: Whether bundle support is enabled
    """
    project_name: str
    current_date: str
    author: str = ""
    bundle_enabled: bool = False
    
    @classmethod
    def create(cls, project_name: str, bundle_enabled: bool = False, author: str = "") -> InitTemplateContext:
        """Create a new template context with current timestamp.
        
        Args:
            project_name: Name of the project
            bundle_enabled: Whether to enable bundle support
            author: Author name (optional)
            
        Returns:
            InitTemplateContext instance with current date
        """
        return cls(
            project_name=project_name,
            current_date=datetime.now().isoformat(),
            author=author,
            bundle_enabled=bundle_enabled
        ) 