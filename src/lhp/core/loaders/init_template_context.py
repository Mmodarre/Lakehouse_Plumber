"""Template context for project initialization."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class InitTemplateContext:
    """Context object containing all variables for init template rendering."""

    project_name: str
    current_date: str
    author: str = ""
    bundle_enabled: bool = False
    bundle_uuid: str = ""
    project_id: str = ""

    @classmethod
    def create(
        cls, project_name: str, bundle_enabled: bool = False, author: str = ""
    ) -> InitTemplateContext:
        """Create a new template context with current timestamp.

        One token is minted per scaffold and rendered into both
        ``databricks.yml`` (``bundle.uuid``) and ``lhp.yaml``
        (``project_id``), so a bundle project has a single identity.
        """
        identity = str(uuid.uuid4())
        return cls(
            project_name=project_name,
            current_date=datetime.now().isoformat(),
            author=author,
            bundle_enabled=bundle_enabled,
            bundle_uuid=identity,
            project_id=identity,
        )
