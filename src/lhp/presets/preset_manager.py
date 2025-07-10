"""Preset management with hierarchical inheritance."""

from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from ..models.config import Preset
from ..parsers.yaml_parser import YAMLParser


class PresetManager:
    """Manages preset loading and inheritance resolution."""

    def __init__(self, presets_dir: Path):
        self.presets_dir = presets_dir
        self.presets: Dict[str, Preset] = {}
        self.parser = YAMLParser()
        self._load_presets()

    def _load_presets(self):
        """Load all presets from the presets directory."""
        if not self.presets_dir.exists():
            return

        presets = self.parser.discover_presets(self.presets_dir)
        for preset in presets:
            self.presets[preset.name] = preset

    def resolve_preset_chain(self, preset_names: List[str]) -> Dict[str, Any]:
        """Resolve a chain of presets with inheritance."""
        resolved = {}
        for preset_name in preset_names:
            preset_config = self._resolve_preset_inheritance(preset_name)
            resolved = self._deep_merge(resolved, preset_config)
        return resolved

    def _resolve_preset_inheritance(self, preset_name: str) -> Dict[str, Any]:
        """Resolve preset inheritance (copy from BurrowBuilder pattern logic)."""
        if preset_name not in self.presets:
            return {}

        preset = self.presets[preset_name]
        result = preset.defaults or {}

        # If extends another preset, merge parent first
        if preset.extends:
            parent_config = self._resolve_preset_inheritance(preset.extends)
            result = self._deep_merge(parent_config, result)

        return result

    def _deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep merge - copy from BurrowBuilder pattern validator."""
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            elif (
                key == "operational_metadata"
                and isinstance(value, list)
                and isinstance(result.get(key), list)
            ):
                # Special handling for operational_metadata lists - combine them
                result[key] = list(set(result[key] + value))  # Merge and deduplicate
            else:
                result[key] = value
        return result

    def get_preset(self, preset_name: str) -> Optional[Preset]:
        """Get a preset by name."""
        return self.presets.get(preset_name)

    def list_presets(self) -> List[str]:
        """List all available preset names."""
        return list(self.presets.keys())

    def get_operational_metadata_selection(
        self, preset_names: List[str]
    ) -> Union[bool, List[str], None]:
        """Get operational metadata selection from resolved preset chain.

        Args:
            preset_names: List of preset names to resolve

        Returns:
            Operational metadata selection (bool, list, or None)
        """
        resolved_config = self.resolve_preset_chain(preset_names)
        return resolved_config.get("operational_metadata")

    def validate_operational_metadata_references(
        self, preset_names: List[str], available_columns: set
    ) -> List[str]:
        """Validate that operational metadata references in presets are valid.

        Args:
            preset_names: List of preset names to validate
            available_columns: Set of available column names from project config

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        for preset_name in preset_names:
            if preset_name not in self.presets:
                errors.append(f"Preset '{preset_name}' not found")
                continue

            preset_config = self._resolve_preset_inheritance(preset_name)
            operational_metadata = preset_config.get("operational_metadata")

            if isinstance(operational_metadata, list):
                for column_name in operational_metadata:
                    if column_name not in available_columns:
                        errors.append(
                            f"Preset '{preset_name}' references unknown column '{column_name}'"
                        )

        return errors
