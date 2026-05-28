"""Operational-metadata configuration models (column definitions, presets, selection)."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class MetadataColumnConfig(BaseModel):
    """Configuration for a single metadata column."""

    expression: str
    description: Optional[str] = None
    applies_to: List[str] = ["streaming_table", "materialized_view"]
    additional_imports: Optional[List[str]] = None
    enabled: bool = True


class MetadataPresetConfig(BaseModel):
    """Configuration for a metadata column preset."""

    columns: List[str]
    description: Optional[str] = None


class OperationalMetadataSelection(BaseModel):
    """Operational metadata selection configuration (used in flowgroups/actions/presets)."""

    enabled: bool = True
    preset: Optional[str] = None  # Reference to project-defined preset
    columns: Optional[List[str]] = None  # Explicit column selection
    include_columns: Optional[List[str]] = None  # Alternative syntax
    exclude_columns: Optional[List[str]] = None  # Alternative syntax


class ProjectOperationalMetadataConfig(BaseModel):
    """Project-level operational metadata configuration (definitions only)."""

    columns: Dict[str, MetadataColumnConfig]
    presets: Optional[Dict[str, MetadataPresetConfig]] = None
    defaults: Optional[Dict[str, Any]] = None
