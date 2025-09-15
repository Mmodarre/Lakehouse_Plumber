"""Python transformation generator."""

import shutil
from pathlib import Path
from typing import Optional
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action


class PythonFunctionConflictError(ValueError):
    """Raised when different Python source files would create same destination file."""
    
    def __init__(self, destination: str, existing_source: str, new_source: str):
        self.destination = destination
        self.existing_source = existing_source
        self.new_source = new_source
        
        message = (
            f"Python function naming conflict detected:\n"
            f"  Existing: {existing_source} → {destination}\n"
            f"  New:      {new_source} → {destination}\n\n"
            f"Resolution required:\n"
            f"  1. Rename one of the Python functions\n"
            f"  2. Move functions to different directories\n"
            f"  3. Update YAML module_path to use different name"
        )
        super().__init__(message)


class PythonTransformGenerator(BaseActionGenerator):
    """Generate Python transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate Python transform code."""
        # Extract module configuration from action level
        module_path = getattr(action, 'module_path', None)
        function_name = getattr(action, 'function_name', None)
        parameters = getattr(action, 'parameters', {})

        if not module_path:
            raise ValueError("Python transform must have 'module_path'")
        if not function_name:
            raise ValueError("Python transform must have 'function_name'")

        # Resolve and copy Python file
        project_root = context.get("spec_dir") or Path.cwd()
        copied_module_name = self._copy_python_file(module_path, project_root, context)

        # Determine source view(s) from action.source directly
        source_views = self._extract_source_views_from_action_source(action.source)

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        # Handle operational metadata
        flowgroup = context.get("flowgroup")
        preset_config = context.get("preset_config", {})
        project_config = context.get("project_config")

        # Initialize operational metadata handler
        from ...utils.operational_metadata import OperationalMetadata
        operational_metadata = OperationalMetadata(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )

        # Update context for substitutions
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)

        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, "view"
        )

        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "source_views": source_views,
            "readMode": readMode,
            "module_path": module_path,
            "module_name": copied_module_name,
            "function_name": function_name,
            "parameters": parameters,
            "description": action.description
            or f"Python transform: {copied_module_name}.{function_name}",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
        }

        # Add import for the copied module
        self.add_import(f"from custom_python_functions.{copied_module_name} import {function_name}")

        return self.render_template("transform/python.py.j2", template_context)

    def _extract_source_views_from_action_source(self, source) -> list:
        """Extract source view names from action.source field."""
        if source is None:
            raise ValueError("Python transform source cannot be None - transforms require input data")
        elif isinstance(source, str):
            return [source]  # Single source view
        elif isinstance(source, list):
            return source  # Multiple source views
        else:
            raise ValueError("Python transform source must be a string or list of strings")

    def _copy_python_file(self, module_path: str, project_root: Path, context: dict) -> str:
        """Copy Python file to custom_python_functions directory and return module name."""
        # Resolve source file path relative to project root
        source_file = project_root / module_path
        
        if not source_file.exists():
            raise FileNotFoundError(f"Python module file not found: {source_file}")
        
        # Extract module name from path (strip .py extension)
        base_module_name = Path(module_path).stem
        
        # Check for naming conflicts and add prefix if needed
        module_name = self._resolve_module_name_conflicts(
            module_path, base_module_name, context
        )
        
        # Determine output directory for the current flowgroup
        flowgroup = context.get("flowgroup")
        if not flowgroup:
            raise ValueError("Flowgroup context required for Python file copying")
            
        # Create custom_python_functions directory structure
        output_dir = context.get("output_dir", Path.cwd())
        if output_dir is None:
            # For dry-run mode, use a temporary directory
            import tempfile
            output_dir = Path(tempfile.mkdtemp())
        custom_functions_dir = output_dir / "custom_python_functions"
        custom_functions_dir.mkdir(parents=True, exist_ok=True)
        
        # Create __init__.py file
        init_file = custom_functions_dir / "__init__.py"
        init_file.write_text("# Generated package for custom Python functions\n")
        
        # Copy the Python file with header (use resolved module name)
        dest_file = custom_functions_dir / f"{module_name}.py"
        
        # Add header to copied file
        original_content = source_file.read_text()
        
        # Apply substitutions to the original content if substitution_manager is available
        if context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            original_content = substitution_mgr._process_string(original_content)
            
            # Track secret references if they exist
            secret_refs = substitution_mgr.get_secret_references()
            if "secret_references" in context and context["secret_references"] is not None:
                context["secret_references"].update(secret_refs)
        
        header = f"""# LHP-SOURCE: {module_path}
# Generated by LakehousePlumber - DO NOT EDIT
# Changes will be overwritten on next generation

"""
        dest_file.write_text(header + original_content)
        
        # Track additional files with state manager if available
        state_manager = context.get("state_manager")
        source_yaml = context.get("source_yaml")
        if state_manager and source_yaml:
            import logging
            logger = logging.getLogger(__name__)
            
            # Track the __init__.py file
            state_manager.track_generated_file(
                generated_path=init_file,
                source_yaml=source_yaml,
                environment=context.get("environment", "unknown"),
                pipeline=flowgroup.pipeline,
                flowgroup=flowgroup.flowgroup,
                generation_context="",  # No context needed for Python transform files
            )
            logger.debug(f"Tracked additional file: {init_file} for Python transform")
            
            # Track the copied Python function file
            state_manager.track_generated_file(
                generated_path=dest_file,
                source_yaml=source_yaml,
                environment=context.get("environment", "unknown"),
                pipeline=flowgroup.pipeline,
                flowgroup=flowgroup.flowgroup,
                generation_context="",  # No context needed for Python transform files
            )
            logger.debug(f"Tracked additional file: {dest_file} for Python transform")
        else:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Skipping file tracking - state_manager: {bool(state_manager)}, source_yaml: {bool(source_yaml)}")
        
        return module_name

    def _resolve_module_name_conflicts(self, module_path: str, base_module_name: str, context: dict) -> str:
        """Check for naming conflicts and fail if different sources would create same destination."""
        output_dir = context.get("output_dir", Path.cwd())
        
        if output_dir is None:
            # For dry-run mode, no conflict resolution needed
            return base_module_name
            
        custom_functions_dir = output_dir / "custom_python_functions"
        potential_conflict_file = custom_functions_dir / f"{base_module_name}.py"
        
        # If no existing file, no conflict
        if not potential_conflict_file.exists():
            return base_module_name
            
        # Check source path from existing file header
        existing_source = self._extract_source_path_from_header(potential_conflict_file)
        current_source = str(Path(module_path))  # Normalize to relative path
        
        if existing_source == current_source:
            # Same source file - legitimate update, overwrite existing
            return base_module_name
        else:
            # Different source files - real conflict, FAIL immediately
            raise PythonFunctionConflictError(
                destination=f"custom_python_functions/{base_module_name}.py",
                existing_source=existing_source or "unknown",
                new_source=current_source
            )
    
    def _extract_source_path_from_header(self, file_path: Path) -> Optional[str]:
        """Extract source path from simple LHP header."""
        try:
            first_line = file_path.read_text().split('\n')[0]
            if first_line.startswith('# LHP-SOURCE: '):
                return first_line[14:].strip()  # Remove "# LHP-SOURCE: " prefix
        except Exception:
            pass
        return None

