"""Blueprint and instance discovery service for LakehousePlumber.

Mirrors `FlowgroupDiscoverer`'s pattern: pattern-driven file discovery via
`discover_files_with_patterns`, then validation. Default patterns are
`blueprints/**/*.yaml` for blueprint definitions and
`pipelines/**/*.yaml` for instance files (alongside hand-written
flowgroups). Both are configurable via `blueprint_include` /
`instance_include` in `lhp.yaml`.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ...models.config import Blueprint, BlueprintInstance, ProjectConfig
from ...parsers.blueprint_parser import BlueprintParser
from ...utils.error_formatter import (
    ErrorCategory,
    LHPValidationError,
)
from ...utils.file_pattern_matcher import discover_files_with_patterns
from ...utils.performance_timer import perf_timer

DEFAULT_BLUEPRINT_PATTERNS = ["blueprints/**/*.yaml", "blueprints/**/*.yml"]
DEFAULT_INSTANCE_PATTERNS = ["pipelines/**/*.yaml", "pipelines/**/*.yml"]


class BlueprintDiscoverer:
    """Discovers blueprint and instance YAML files according to project config patterns.

    Returns:
        - `discover_blueprints`: dict[blueprint_name, (Blueprint, blueprint_path)]
        - `discover_instances`: list[(BlueprintInstance, instance_path)]
    """

    def __init__(
        self,
        project_root: Path,
        project_config: Optional[ProjectConfig] = None,
        blueprint_parser: Optional[BlueprintParser] = None,
    ) -> None:
        self.project_root = project_root
        self.project_config = project_config
        self.blueprint_parser = blueprint_parser or BlueprintParser()
        self.logger = logging.getLogger(__name__)

    def _blueprint_patterns(self) -> List[str]:
        if self.project_config and self.project_config.blueprint_include:
            return self.project_config.blueprint_include
        return DEFAULT_BLUEPRINT_PATTERNS

    def _instance_patterns(self) -> List[str]:
        if self.project_config and self.project_config.instance_include:
            return self.project_config.instance_include
        return DEFAULT_INSTANCE_PATTERNS

    def discover_blueprints(self) -> Dict[str, Tuple[Blueprint, Path]]:
        """Discover blueprint files and return a name-keyed registry.

        Validates uniqueness of `blueprint.name` across all discovered files;
        a duplicate name raises code 046 with both file paths in the context.
        Returns an empty dict if there are no blueprint files.
        """
        with perf_timer("discover_blueprints [discoverer]"):
            files = discover_files_with_patterns(
                self.project_root, self._blueprint_patterns()
            )
            self.logger.debug(
                f"Found {len(files)} blueprint file(s) under {self.project_root}"
            )

            registry: Dict[str, Tuple[Blueprint, Path]] = {}
            for path in files:
                blueprint = self.blueprint_parser.parse_blueprint_file(path)
                if blueprint.name in registry:
                    existing_path = registry[blueprint.name][1]
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="046",
                        title=f"Duplicate blueprint name '{blueprint.name}'",
                        details=(
                            f"Two blueprint files declare the same name "
                            f"'{blueprint.name}':\n  - {existing_path}\n  - {path}\n"
                            "Each blueprint must have a unique name (referenced "
                            "by 'blueprint:' in instance files)."
                        ),
                        suggestions=[
                            "Rename one of the blueprints",
                            "Or remove the duplicate file",
                        ],
                        context={
                            "blueprint": blueprint.name,
                            "file_a": str(existing_path),
                            "file_b": str(path),
                        },
                    )
                registry[blueprint.name] = (blueprint, path)

            self.logger.info(
                f"Discovered {len(registry)} blueprint(s): "
                f"{sorted(registry.keys())}"
            )
            return registry

    def discover_instances(
        self, blueprints: Dict[str, Tuple[Blueprint, Path]]
    ) -> List[Tuple[BlueprintInstance, Path]]:
        """Discover instance files and parse each one against the blueprint registry.

        With the default `instance_include = ['pipelines/**/*.yaml']`, the
        instance pattern overlaps with the flowgroup `include:` pattern.
        We route by content shape: a peeked first document is parsed as an
        instance only if `BlueprintParser.looks_like_instance()` returns True;
        all other files (regular flowgroups, blueprint definitions, etc.) are
        skipped here and handled by their respective discoverers.

        Args:
            blueprints: Output of `discover_blueprints()`. Used by the parser
                to validate `use_blueprint:` references and parameter names.

        Returns:
            List of (BlueprintInstance, instance_path).
        """
        from ...utils.yaml_loader import load_yaml_documents_all

        with perf_timer("discover_instances [discoverer]"):
            files = discover_files_with_patterns(
                self.project_root, self._instance_patterns()
            )
            self.logger.debug(
                f"Found {len(files)} candidate instance file(s) under "
                f"{self.project_root}"
            )

            blueprint_models: Dict[str, Blueprint] = {
                name: bp for name, (bp, _) in blueprints.items()
            }
            instances: List[Tuple[BlueprintInstance, Path]] = []
            skipped_non_instance = 0
            for path in files:
                try:
                    documents = load_yaml_documents_all(
                        path, error_context=f"instance candidate {path}"
                    )
                except Exception as e:
                    self.logger.debug(
                        f"Skipping {path} during instance discovery (load "
                        f"error): {e}"
                    )
                    continue

                if not documents or not BlueprintParser.looks_like_instance(
                    documents[0]
                ):
                    skipped_non_instance += 1
                    self.logger.debug(
                        f"Skipping {path}: not an instance file "
                        "(no use_blueprint/blueprint key)"
                    )
                    continue

                instance = self.blueprint_parser.parse_instance_file(
                    path, blueprint_models
                )
                instances.append((instance, path))

            if skipped_non_instance:
                self.logger.debug(
                    f"Skipped {skipped_non_instance} non-instance file(s) "
                    "during instance discovery (overlapping with flowgroup "
                    "include pattern)"
                )
            self.logger.info(f"Discovered {len(instances)} instance(s)")
            return instances
