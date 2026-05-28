"""Flowgroup model and per-flowgroup runtime context envelope."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

from pydantic import BaseModel

from ._action import Action


class FlowGroup(BaseModel):
    pipeline: str
    flowgroup: str
    job_name: Optional[str] = None
    variables: Optional[Dict[str, str]] = None  # Local variable definitions
    presets: List[str] = []
    use_template: Optional[str] = None
    template_parameters: Optional[Dict[str, Any]] = None
    actions: List[Action] = []
    operational_metadata: Optional[Union[bool, List[str]]] = (
        None  # Simplified: bool or list of column names
    )


@dataclass(frozen=True, slots=True)
class FlowGroupContext:
    """Envelope carrying per-flowgroup provenance across the worker boundary."""

    flowgroup: FlowGroup
    source_yaml: Path | None
    synthetic: bool = False
    auxiliary_files: Mapping[str, str] = field(default_factory=dict)
