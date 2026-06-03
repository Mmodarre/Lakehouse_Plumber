"""Operational-metadata configuration models (column definitions, presets, selection)."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class MetadataColumnConfig(BaseModel):
    expression: str
    description: Optional[str] = None
    applies_to: List[str] = ["streaming_table", "materialized_view"]
    additional_imports: Optional[List[str]] = None
    enabled: bool = True


class MetadataPresetConfig(BaseModel):
    columns: List[str]
    description: Optional[str] = None


class OperationalMetadataSelection(BaseModel):
    enabled: bool = True
    preset: Optional[str] = None
    columns: Optional[List[str]] = None
    include_columns: Optional[List[str]] = None  # Alternative syntax
    exclude_columns: Optional[List[str]] = None  # Alternative syntax


class ProjectOperationalMetadataConfig(BaseModel):
    columns: Dict[str, MetadataColumnConfig]
    presets: Optional[Dict[str, MetadataPresetConfig]] = None
    defaults: Optional[Dict[str, Any]] = None
