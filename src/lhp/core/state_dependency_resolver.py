"""State dependency resolver for LakehousePlumber dependency tracking."""

import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
from ..models.config import FlowGroup
from ..parsers.yaml_parser import YAMLParser
from ..presets.preset_manager import PresetManager
from ..core.template_engine import TemplateEngine
from .state_manager import DependencyInfo


class StateDependencyResolver:
    """Resolves dependencies for YAML files including presets, templates, and transitive dependencies.
    
    Implements safe caching: caches which files are referenced (expensive discovery)
    while always recalculating checksums (cheap validation) to preserve change detection.
    """

    def __init__(self, project_root: Path):
        """Initialize dependency resolver.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        self.yaml_parser = YAMLParser()
        self.preset_manager = PresetManager(project_root / "presets")
        self.template_engine = TemplateEngine(project_root / "templates")
        
        # Cache for dependency paths (not checksums!)
        # Key: (yaml_file, source_checksum, environment, pipeline, flowgroup)
        # Value: Dict[str, Tuple[type, last_modified]] - paths with metadata but no checksums
        self._dependency_paths_cache: Dict[Tuple[str, str, str, str, str], Dict[str, Tuple[str, str]]] = {}

    def resolve_file_dependencies(self, yaml_file: Path, environment: str, 
                                  pipeline: str = None, flowgroup_name: str = None) -> Dict[str, DependencyInfo]:
        """Resolve all dependencies for a YAML file with safe caching.
        
        Supports multi-flowgroup files. If pipeline and flowgroup_name are provided,
        resolves dependencies for that specific flowgroup only.
        
        Caches dependency discovery (which files are referenced) but always recalculates
        checksums to preserve change detection accuracy.
        
        Args:
            yaml_file: Path to the YAML file (relative to project_root)
            environment: Environment name for dependency resolution
            pipeline: Optional pipeline name to identify specific flowgroup in multi-flowgroup files
            flowgroup_name: Optional flowgroup name to identify specific flowgroup in multi-flowgroup files
            
        Returns:
            Dictionary mapping dependency paths to DependencyInfo objects with CURRENT checksums
        """
        dependencies = {}
        
        try:
            # Resolve yaml_file path relative to project_root
            resolved_yaml_file = self.project_root / yaml_file if not yaml_file.is_absolute() else yaml_file
            
            # Calculate source YAML checksum for cache key
            source_checksum = self._calculate_checksum(resolved_yaml_file)
            yaml_file_str = str(yaml_file)
            # Include pipeline and flowgroup in cache key for multi-flowgroup files
            cache_key = (yaml_file_str, source_checksum, environment, pipeline or '', flowgroup_name or '')
            
            # Check cache for dependency paths
            if cache_key in self._dependency_paths_cache:
                # Cache hit: reuse discovered paths but recalculate checksums
                cached_paths = self._dependency_paths_cache[cache_key]
                dependencies = self._recalculate_dependency_checksums(cached_paths)
                self.logger.debug(f"Cache hit: Reused {len(dependencies)} dependency paths for {yaml_file}")
            else:
                # Cache miss: full discovery and resolution
                # Parse all flowgroups from file (supports multi-document and array syntax)
                flowgroups = self.yaml_parser.parse_flowgroups_from_file(resolved_yaml_file)
                
                # Find the specific flowgroup if pipeline and flowgroup_name provided
                target_flowgroup = None
                if pipeline and flowgroup_name:
                    for fg in flowgroups:
                        if fg.pipeline == pipeline and fg.flowgroup == flowgroup_name:
                            target_flowgroup = fg
                            break
                    if not target_flowgroup:
                        self.logger.warning(f"Flowgroup {flowgroup_name} not found in {yaml_file}")
                        return dependencies
                else:
                    # If no specific flowgroup specified, use first one (backward compat)
                    target_flowgroup = flowgroups[0] if flowgroups else None
                
                if not target_flowgroup:
                    return dependencies
                
                # Resolve preset dependencies
                preset_deps = self._resolve_preset_dependencies(target_flowgroup)
                dependencies.update(preset_deps)
                
                # Resolve template dependencies
                template_deps = self._resolve_template_dependencies(target_flowgroup)
                dependencies.update(template_deps)
                
                # Resolve custom data source dependencies
                custom_datasource_deps = self._resolve_custom_datasource_dependencies(target_flowgroup)
                dependencies.update(custom_datasource_deps)
                
                # Resolve external file dependencies (Python, SQL, etc.)
                external_file_deps = self._resolve_external_file_dependencies(target_flowgroup)
                dependencies.update(external_file_deps)
                
                # Cache the dependency paths (without checksums)
                self._cache_dependency_paths(cache_key, dependencies)
                
                self.logger.debug(f"Cache miss: Discovered {len(dependencies)} dependencies for {yaml_file}")
            
        except Exception as e:
            self.logger.warning(f"Failed to resolve dependencies for {yaml_file}: {e}")
            
        return dependencies
    
    def _cache_dependency_paths(self, cache_key: Tuple[str, str, str, str, str], 
                                dependencies: Dict[str, DependencyInfo]) -> None:
        """Cache dependency paths and metadata (but not checksums).
        
        Args:
            cache_key: Cache key tuple (yaml_file, source_checksum, environment, pipeline, flowgroup)
            dependencies: Full dependency info with checksums
        """
        # Store only paths, types, and last_modified (not checksums)
        cached_paths = {
            path: (dep_info.type, dep_info.last_modified)
            for path, dep_info in dependencies.items()
        }
        self._dependency_paths_cache[cache_key] = cached_paths
    
    def _recalculate_dependency_checksums(self, cached_paths: Dict[str, Tuple[str, str]]) -> Dict[str, DependencyInfo]:
        """Recalculate current checksums for cached dependency paths.
        
        Args:
            cached_paths: Dict mapping paths to (type, last_modified) tuples
            
        Returns:
            Dict mapping paths to DependencyInfo with CURRENT checksums
        """
        dependencies = {}
        for path, (dep_type, _) in cached_paths.items():
            file_path = self.project_root / path
            if file_path.exists():
                # Always recalculate current checksum and modification time
                current_checksum = self._calculate_checksum(file_path)
                current_modified = self._get_file_modification_time(file_path)
                dependencies[path] = DependencyInfo(
                    path=path,
                    checksum=current_checksum,  # ALWAYS FRESH
                    type=dep_type,
                    last_modified=current_modified  # ALWAYS FRESH
                )
            else:
                # File no longer exists - include with empty checksum
                dependencies[path] = DependencyInfo(
                    path=path,
                    checksum="",
                    type=dep_type,
                    last_modified=""
                )
        return dependencies
    
    def clear_cache(self) -> None:
        """Clear the dependency paths cache."""
        self._dependency_paths_cache.clear()
        self.logger.debug("Dependency paths cache cleared")

    def _resolve_preset_dependencies(self, flowgroup: FlowGroup) -> Dict[str, DependencyInfo]:
        """Resolve preset dependencies including transitive dependencies.
        
        Args:
            flowgroup: FlowGroup to resolve presets for
            
        Returns:
            Dictionary mapping preset paths to DependencyInfo objects
        """
        dependencies = {}
        
        if not flowgroup.presets:
            return dependencies
            
        # Process each preset
        for preset_name in flowgroup.presets:
            preset_deps = self._resolve_preset_chain(preset_name, set())
            dependencies.update(preset_deps)
            
        return dependencies

    def _resolve_preset_chain(self, preset_name: str, visited: Set[str]) -> Dict[str, DependencyInfo]:
        """Resolve a preset chain including transitive dependencies.
        
        Args:
            preset_name: Name of the preset to resolve
            visited: Set of already visited presets (for circular dependency detection)
            
        Returns:
            Dictionary mapping preset paths to DependencyInfo objects
        """
        dependencies = {}
        
        # Check for circular dependencies
        if preset_name in visited:
            self.logger.warning(f"Circular dependency detected in preset chain: {preset_name}")
            return dependencies
            
        visited.add(preset_name)
        
        # Get preset file path
        preset_file = self.project_root / "presets" / f"{preset_name}.yaml"
        
        if not preset_file.exists():
            self.logger.warning(f"Preset file not found: {preset_file}")
            # Still create dependency info with empty checksum for missing files
            dependencies[str(preset_file.relative_to(self.project_root))] = DependencyInfo(
                path=str(preset_file.relative_to(self.project_root)),
                checksum="",
                type="preset",
                last_modified=""
            )
            return dependencies
            
        # Calculate checksum and create dependency info
        checksum = self._calculate_checksum(preset_file)
        last_modified = self._get_file_modification_time(preset_file)
        
        dependencies[str(preset_file.relative_to(self.project_root))] = DependencyInfo(
            path=str(preset_file.relative_to(self.project_root)),
            checksum=checksum,
            type="preset",
            last_modified=last_modified
        )
        
        # Resolve transitive dependencies (if preset extends another preset)
        try:
            preset = self.preset_manager.get_preset(preset_name)
            if preset and preset.extends:
                transitive_deps = self._resolve_preset_chain(preset.extends, visited.copy())
                dependencies.update(transitive_deps)
        except Exception as e:
            self.logger.warning(f"Failed to resolve transitive dependencies for preset {preset_name}: {e}")
            
        return dependencies

    def _resolve_template_dependencies(self, flowgroup: FlowGroup) -> Dict[str, DependencyInfo]:
        """Resolve template dependencies including transitive preset dependencies.
        
        Args:
            flowgroup: FlowGroup to resolve templates for
            
        Returns:
            Dictionary mapping template and preset paths to DependencyInfo objects
        """
        dependencies = {}
        
        if not flowgroup.use_template:
            return dependencies
            
        template_name = flowgroup.use_template
        
        # Get template file path
        template_file = self.project_root / "templates" / f"{template_name}.yaml"
        
        if not template_file.exists():
            self.logger.warning(f"Template file not found: {template_file}")
            # Still create dependency info with empty checksum for missing files
            dependencies[str(template_file.relative_to(self.project_root))] = DependencyInfo(
                path=str(template_file.relative_to(self.project_root)),
                checksum="",
                type="template",
                last_modified=""
            )
            return dependencies
            
        # Calculate checksum and create dependency info for template
        checksum = self._calculate_checksum(template_file)
        last_modified = self._get_file_modification_time(template_file)
        
        dependencies[str(template_file.relative_to(self.project_root))] = DependencyInfo(
            path=str(template_file.relative_to(self.project_root)),
            checksum=checksum,
            type="template",
            last_modified=last_modified
        )
        
        # Resolve transitive preset dependencies AND external files from template
        try:
            template = self.template_engine.get_template(template_name)
            if template:
                # Resolve preset dependencies
                if hasattr(template, 'presets') and template.presets:
                    for preset_name in template.presets:
                        preset_deps = self._resolve_preset_chain(preset_name, set())
                        dependencies.update(preset_deps)
                
                # Resolve external files from template actions
                if hasattr(template, 'actions') and template.actions:
                    template_external_files = self._extract_external_files_from_template_actions(template.actions)
                    
                    # Track each external file as a dependency
                    for file_path in template_external_files:
                        external_dep = self._create_external_file_dependency(file_path)
                        if external_dep:
                            dependencies[file_path] = external_dep
                            self.logger.debug(f"Tracked external file from template {template_name}: {file_path}")
                            
        except Exception as e:
            self.logger.warning(f"Failed to resolve transitive dependencies for template {template_name}: {e}")
            
        return dependencies

    def _extract_external_files_from_template_actions(self, template_actions: List[Dict[str, Any]]) -> Set[str]:
        """Extract external file references from template actions (raw dictionary format).
        
        Args:
            template_actions: List of action dictionaries from template
            
        Returns:
            Set of external file paths found in template actions
        """
        files = set()
        
        for action in template_actions:
            # Expectations files (data quality)
            if action.get('expectations_file'):
                files.add(action['expectations_file'])
            
            # Python transform files
            if action.get('type') == 'transform' and action.get('transform_type') == 'python':
                if action.get('module_path'):
                    files.add(action['module_path'])
            
            # Python load files
            if action.get('type') == 'load':
                source = action.get('source', {})
                if isinstance(source, dict):
                    if source.get('type') == 'python' and source.get('module_path'):
                        files.add(source['module_path'])
                    if source.get('sql_path'):
                        files.add(source['sql_path'])
            
            # SQL files (load and transform)
            if action.get('sql_path'):
                files.add(action['sql_path'])
            
            # Snapshot CDC source function files
            if action.get('type') == 'write':
                write_target = action.get('write_target', {})
                if isinstance(write_target, dict) and write_target.get('mode') == 'snapshot_cdc':
                    snapshot_config = write_target.get('snapshot_cdc_config', {})
                    source_function = snapshot_config.get('source_function', {})
                    if source_function.get('file'):
                        files.add(source_function['file'])
        
        return files

    def resolve_global_dependencies(self, environment: str) -> Dict[str, DependencyInfo]:
        """Resolve global dependencies for an environment.
        
        Args:
            environment: Environment name
            
        Returns:
            Dictionary mapping dependency paths to DependencyInfo objects
        """
        dependencies = {}
        
        # Resolve substitution file dependency
        substitution_file = self.project_root / "substitutions" / f"{environment}.yaml"
        if substitution_file.exists():
            checksum = self._calculate_checksum(substitution_file)
            last_modified = self._get_file_modification_time(substitution_file)
            
            dependencies[str(substitution_file.relative_to(self.project_root))] = DependencyInfo(
                path=str(substitution_file.relative_to(self.project_root)),
                checksum=checksum,
                type="substitution",
                last_modified=last_modified
            )
        
        # Resolve project config dependency
        project_config_file = self.project_root / "lhp.yaml"
        if project_config_file.exists():
            checksum = self._calculate_checksum(project_config_file)
            last_modified = self._get_file_modification_time(project_config_file)
            
            dependencies[str(project_config_file.relative_to(self.project_root))] = DependencyInfo(
                path=str(project_config_file.relative_to(self.project_root)),
                checksum=checksum,
                type="project_config",
                last_modified=last_modified
            )
            
        return dependencies

    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            SHA256 checksum as hex string
        """
        import hashlib
        
        try:
            sha256_hash = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return ""

    def _get_file_modification_time(self, file_path: Path) -> str:
        """Get file modification time as ISO string.
        
        Args:
            file_path: Path to the file
            
        Returns:
            ISO format timestamp string
        """
        try:
            from datetime import datetime
            mtime = file_path.stat().st_mtime
            return datetime.fromtimestamp(mtime).isoformat()
        except Exception as e:
            self.logger.warning(f"Failed to get modification time for {file_path}: {e}")
            return ""

    def _resolve_custom_datasource_dependencies(self, flowgroup: FlowGroup) -> Dict[str, DependencyInfo]:
        """Resolve custom data source module_path dependencies.
        
        Args:
            flowgroup: FlowGroup to resolve custom data source dependencies for
            
        Returns:
            Dictionary mapping module paths to DependencyInfo objects
        """
        dependencies = {}
        
        if not flowgroup.actions:
            return dependencies
            
        # Process each action looking for custom_datasource type
        for action in flowgroup.actions:
            if (hasattr(action, 'type') and action.type == 'load' and 
                hasattr(action, 'source') and isinstance(action.source, dict) and
                action.source.get('type') == 'custom_datasource' and
                action.source.get('module_path') and action.source.get('custom_datasource_class')):
                
                # This is a custom data source action with module_path
                module_path = action.source.get('module_path')
                module_file = self.project_root / module_path
                if module_file.exists():
                    checksum = self._calculate_checksum(module_file)
                    last_modified = self._get_file_modification_time(module_file)
                    
                    dependencies[module_path] = DependencyInfo(
                        path=module_path,
                        checksum=checksum,
                        type="custom_datasource_module",
                        last_modified=last_modified
                    )
                    
                    self.logger.debug(f"Found custom data source dependency: {module_path}")
                else:
                    self.logger.warning(f"Custom data source module not found: {module_path}")
                    
        return dependencies

    def _resolve_external_file_dependencies(self, flowgroup: FlowGroup) -> Dict[str, DependencyInfo]:
        """Resolve external file dependencies (Python, SQL, etc.) using simple hash-based tracking.
        
        Args:
            flowgroup: FlowGroup to resolve external file dependencies for
            
        Returns:
            Dictionary mapping file paths to DependencyInfo objects
        """
        dependencies = {}
        
        if not flowgroup.actions:
            return dependencies
        
        # Extract all external files from the flowgroup
        all_external_files = self._extract_all_external_files(flowgroup)
        
        # Process each external file with simple hash-based dependency tracking
        for file_path in all_external_files:
            dependency_info = self._create_external_file_dependency(file_path)
            if dependency_info:
                dependencies[file_path] = dependency_info
        
        return dependencies

    def _extract_all_external_files(self, flowgroup: FlowGroup) -> Set[str]:
        """Extract all external file references from flowgroup - content agnostic.
        
        Args:
            flowgroup: FlowGroup to extract external files from
            
        Returns:
            Set of external file paths
        """
        files = set()
        
        if not flowgroup.actions:
            return files
        
        for action in flowgroup.actions:
            # Python transform files
            if (hasattr(action, 'type') and action.type == 'transform' and
                hasattr(action, 'transform_type') and action.transform_type == 'python' and
                hasattr(action, 'module_path') and action.module_path):
                files.add(action.module_path)
            
            # Python load files
            elif (hasattr(action, 'type') and action.type == 'load' and
                  hasattr(action, 'source') and isinstance(action.source, dict) and
                  action.source.get('type') == 'python' and
                  action.source.get('module_path')):
                files.add(action.source['module_path'])
            
            # SQL files (load and transform with sql_path)
            if (hasattr(action, 'sql_path') and action.sql_path):
                files.add(action.sql_path)
            elif (hasattr(action, 'source') and isinstance(action.source, dict) and
                  action.source.get('sql_path')):
                files.add(action.source['sql_path'])
            
            # Expectation files (data quality)
            if (hasattr(action, 'expectations_file') and action.expectations_file):
                files.add(action.expectations_file)
            
            # Snapshot CDC source function files
            if (hasattr(action, 'type') and action.type == 'write' and
                hasattr(action, 'write_target') and isinstance(action.write_target, dict) and
                action.write_target.get('mode') == 'snapshot_cdc'):
                snapshot_config = action.write_target.get('snapshot_cdc_config', {})
                source_function = snapshot_config.get('source_function', {})
                if source_function.get('file'):
                    files.add(source_function['file'])
        
        return files

    def _create_external_file_dependency(self, file_path: str) -> Optional[DependencyInfo]:
        """Create dependency info for an external file using simple hash-based tracking.
        
        Args:
            file_path: Path to external file (relative to project root)
            
        Returns:
            DependencyInfo object if file exists, None otherwise
        """
        file_full_path = self.project_root / file_path
        if file_full_path.exists():
            checksum = self._calculate_checksum(file_full_path)
            last_modified = self._get_file_modification_time(file_full_path)
            
            self.logger.debug(f"Found external file dependency: {file_path}")
            
            return DependencyInfo(
                path=file_path,
                checksum=checksum,
                type="external_file",
                last_modified=last_modified
            )
        else:
            self.logger.debug(f"External file not found (will not track): {file_path}")
            return None

    def calculate_composite_checksum(self, dependencies: List[str]) -> str:
        """Calculate composite checksum for a list of dependency paths.
        
        Args:
            dependencies: List of dependency file paths relative to project root
            
        Returns:
            Composite SHA256 checksum as hex string
        """
        import hashlib
        
        try:
            sha256_hash = hashlib.sha256()
            
            # Sort dependencies for deterministic checksum
            sorted_deps = sorted(dependencies)
            
            for dep_path in sorted_deps:
                file_path = self.project_root / dep_path
                if file_path.exists():
                    # Add file path to hash
                    sha256_hash.update(dep_path.encode('utf-8'))
                    # Add file content to hash
                    with open(file_path, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            sha256_hash.update(chunk)
                else:
                    # Add path with placeholder for missing files
                    sha256_hash.update(f"{dep_path}:MISSING".encode('utf-8'))
                    
            return sha256_hash.hexdigest()
            
        except Exception as e:
            self.logger.warning(f"Failed to calculate composite checksum: {e}")
            return "" 