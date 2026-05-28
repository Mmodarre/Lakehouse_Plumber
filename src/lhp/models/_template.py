"""Template and preset definitions."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from ._action import Action


class Template(BaseModel):
    name: str
    version: str = "1.0"
    description: Optional[str] = None
    presets: List[str] = []  # List of preset names to apply to template actions
    parameters: List[Dict[str, Any]] = []
    actions: Union[List[Action], List[Dict[str, Any]]] = []
    _raw_actions: bool = False  # Internal flag to track if actions are raw dictionaries

    def has_raw_actions(self) -> bool:
        """Check if template contains raw action dictionaries (not validated Action objects)."""
        return self._raw_actions

    def get_actions_as_dicts(self) -> List[Dict[str, Any]]:
        """Get actions as dictionaries, converting from Action objects if needed."""
        if self._raw_actions:
            return self.actions
        else:
            return [action.model_dump(mode="json") for action in self.actions]


class Preset(BaseModel):
    name: str
    version: str = "1.0"
    extends: Optional[str] = None
    description: Optional[str] = None
    defaults: Optional[Dict[str, Any]] = None
