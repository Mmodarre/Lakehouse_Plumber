"""Template context for project initialization."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass
class InitTemplateContext:
    """Context object containing all variables for init template rendering."""

    project_name: str
    current_date: str
    author: str = ""
    bundle_enabled: bool = False
    bundle_uuid: str = ""

    @classmethod
    def create(
        cls, project_name: str, bundle_enabled: bool = False, author: str = ""
    ) -> InitTemplateContext:
        """Create a new template context with current timestamp."""
        return cls(
            project_name=project_name,
            current_date=datetime.now().isoformat(),
            author=author,
            bundle_enabled=bundle_enabled,
            bundle_uuid=str(uuid.uuid4()),
        )
