"""Import-statement consolidation for generated pipeline modules."""

from lhp.core.codegen.imports.categorizer import extract_future_imports
from lhp.core.codegen.imports.detector import ImportDetector
from lhp.core.codegen.imports.manager import ImportManager
from lhp.core.codegen.imports.source_parser import (
    ImportTarget,
    local_import_targets,
    parse_user_module,
)

__all__ = [
    "ImportDetector",
    "ImportManager",
    "ImportTarget",
    "extract_future_imports",
    "local_import_targets",
    "parse_user_module",
]
