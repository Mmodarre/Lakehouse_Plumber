"""Template and preset definitions."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from ._action import Action


class Template(BaseModel):
    name: str
    version: str = "1.0"
    description: Optional[str] = None
    presets: List[str] = []
    parameters: List[Dict[str, Any]] = []
    actions: Union[List[Action], List[Dict[str, Any]]] = []
    _raw_actions: bool = (
        False  # True when actions are raw dicts (not validated Action objects)
    )

    def has_raw_actions(self) -> bool:
        return self._raw_actions


class Preset(BaseModel):
    name: str
    version: str = "1.0"
    extends: Optional[str] = None
    description: Optional[str] = None
    defaults: Optional[Dict[str, Any]] = None
