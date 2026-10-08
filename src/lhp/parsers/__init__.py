"""Parsers package for LakehousePlumber.

YAML parsing (single + multi-document), schema parsers (column schema and
schema-transform formats), and YAML/flowgroup parsing live here. The public
surface below is the only API callers should depend on; submodule paths
(e.g. ``lhp.parsers.yaml_loader``) remain importable for callers that need
direct module references.
"""

from .editor_source_map import SourceSpan, YamlPath, source_spans
from .parse_cache import PersistentParseCache
from .schema_parser import SchemaParser
from .schema_transform_parser import SchemaTransformParser
from .yaml_loader import (
    SAFE_LOADER,
    load_yaml_documents_all,
    load_yaml_file,
    safe_load_yaml_with_fallback,
)

__all__ = [
    "SAFE_LOADER",
    "PersistentParseCache",
    "SchemaParser",
    "SchemaTransformParser",
    "SourceSpan",
    "YamlPath",
    "load_yaml_documents_all",
    "load_yaml_file",
    "safe_load_yaml_with_fallback",
    "source_spans",
]
