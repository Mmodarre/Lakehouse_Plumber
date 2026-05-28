"""Config-loader services for LHP.

Public re-exports:
- PipelineConfigLoader  loads pipeline-config.yml files
- ProjectConfigLoader   loads lhp.yaml + .lhp.local.yaml (project root)
- InitTemplateLoader    discovers / instantiates init templates
- InitTemplateContext   DTO for init-template rendering context
- External-file helpers (resolve_external_file_path, load_external_file_text, is_file_path)
- enforce_version_requirements   project required_lhp_version check
"""

from .external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)
from .init_template_context import InitTemplateContext
from .init_template_loader import InitTemplateLoader
from .pipeline_config_loader import PipelineConfigLoader
from .project_config_loader import ProjectConfigLoader
from .version_enforcement import enforce_version_requirements

__all__ = [
    "InitTemplateContext",
    "InitTemplateLoader",
    "PipelineConfigLoader",
    "ProjectConfigLoader",
    "enforce_version_requirements",
    "is_file_path",
    "load_external_file_text",
    "resolve_external_file_path",
]
