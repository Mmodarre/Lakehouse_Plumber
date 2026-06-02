"""Preset management with hierarchical inheritance."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from lhp.models import Preset

from ..errors import ErrorFactory, codes
from ..parsers.yaml_parser import YAMLParser
from ..utils.performance_timer import perf_timer

logger = logging.getLogger(__name__)


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
            logger.debug(f"Presets directory does not exist: {self.presets_dir}")
            return

        presets = self.parser.discover_presets(self.presets_dir)
        for preset in presets:
            logger.debug(f"Loaded preset '{preset.name}' from {self.presets_dir}")
            self.presets[preset.name] = preset
        logger.info(f"Discovered {len(self.presets)} preset(s) from {self.presets_dir}")

    def resolve_preset_chain(self, preset_names: List[str]) -> Dict[str, Any]:
        """Resolve a chain of presets with inheritance."""
        with perf_timer("preset_resolve", category="preset_resolve"):
            logger.debug(f"Resolving preset chain: {preset_names}")
            resolved = {}
            for preset_name in preset_names:
                preset_config = self._resolve_preset_inheritance(preset_name)
                resolved = self._deep_merge(resolved, preset_config)
            logger.debug(f"Preset chain resolved with {len(resolved)} top-level keys")
            return resolved

    def _resolve_preset_inheritance(
        self,
        preset_name: str,
        visited: Optional[set] = None,
        path: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Resolve preset inheritance chain.

        Args:
            preset_name: Name of the preset to resolve
            visited: Set of already-visited preset names (cycle detection)
            path: Ordered list of visited preset names (deterministic cycle path)

        Returns:
            Merged preset configuration

        Raises:
            LHPConfigError: If the preset is not found (LHP-ACT-001).
            LHPError: If circular inheritance is detected (LHP-DEP-022).
        """
        if visited is None:
            visited = set()
        if path is None:
            path = []

        # Cycle detection
        if preset_name in visited:
            cycle_path = " -> ".join([*path, preset_name])
            raise ErrorFactory.dependency_error(
                codes.DEP_022,
                title="Circular preset inheritance detected",
                details=(
                    f"Preset '{preset_name}' creates a circular inheritance chain: {cycle_path}"
                ),
                suggestions=[
                    "Remove the circular 'extends' reference in one of the presets",
                    "Review the preset inheritance chain for unintended cycles",
                ],
                context={"Preset": preset_name, "Chain": cycle_path},
            )

        if preset_name not in self.presets:
            raise ErrorFactory.preset_not_found(
                preset_name=preset_name,
                available_presets=sorted(self.presets.keys()),
            )

        preset = self.presets[preset_name]
        result = preset.defaults or {}

        # If extends another preset, merge parent first
        if preset.extends:
            logger.debug(
                f"Preset '{preset_name}' extends '{preset.extends}', resolving parent"
            )
            parent_config = self._resolve_preset_inheritance(
                preset.extends, visited | {preset_name}, [*path, preset_name]
            )
            result = self._deep_merge(parent_config, result)

        return result

    def _deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                logger.debug(f"Deep merging nested dict for key '{key}'")
                result[key] = self._deep_merge(result[key], value)
            elif (
                key == "operational_metadata"
                and isinstance(value, list)
                and isinstance(result.get(key), list)
            ):
                # Special handling for operational_metadata lists - combine them
                result[key] = list(
                    dict.fromkeys(result[key] + value)
                )  # Order-preserving dedup
                logger.debug(
                    f"Merged operational_metadata lists: {len(result[key])} entries after dedup"
                )
            else:
                result[key] = value
        return result

    def list_presets(self) -> List[str]:
        """List all available preset names."""
        return list(self.presets.keys())
