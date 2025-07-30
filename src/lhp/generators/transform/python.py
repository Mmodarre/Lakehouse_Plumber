"""Python transformation generator."""

import shutil
from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action


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
        header = f"""# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                                    WARNING                                   ║
# ║                          DO NOT EDIT THIS FILE DIRECTLY                      ║
# ╠══════════════════════════════════════════════════════════════════════════════╣
# ║ This file was automatically copied from: {module_path:<31}     ║
# ║ during pipeline generation. Any changes made here will be OVERWRITTEN        ║
# ║ on the next generation cycle.                                                ║
# ║                                                                              ║
# ║ To make changes:                                                             ║
# ║ 1. Edit the original file: {module_path:<42}        ║
# ║ 2. Regenerate the pipeline                                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

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
            )
            logger.debug(f"Tracked additional file: {init_file} for Python transform")
            
            # Track the copied Python function file
            state_manager.track_generated_file(
                generated_path=dest_file,
                source_yaml=source_yaml,
                environment=context.get("environment", "unknown"),
                pipeline=flowgroup.pipeline,
                flowgroup=flowgroup.flowgroup,
            )
            logger.debug(f"Tracked additional file: {dest_file} for Python transform")
        else:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Skipping file tracking - state_manager: {bool(state_manager)}, source_yaml: {bool(source_yaml)}")
        
        return module_name

    def _resolve_module_name_conflicts(self, module_path: str, base_module_name: str, context: dict) -> str:
        """Resolve module naming conflicts for same pipeline by adding directory prefix."""
        flowgroup = context.get("flowgroup")
        output_dir = context.get("output_dir", Path.cwd())
        
        if output_dir is None:
            # For dry-run mode, no conflict resolution needed
            return base_module_name
            
        custom_functions_dir = output_dir / "custom_python_functions"
        potential_conflict_file = custom_functions_dir / f"{base_module_name}.py"
        
        # If no existing file, no conflict
        if not potential_conflict_file.exists():
            return base_module_name
            
        # Check if existing file has same content (same source file)
        source_file = context.get("spec_dir", Path.cwd()) / module_path
        if source_file.exists():
            try:
                existing_content = potential_conflict_file.read_text()
                new_content = source_file.read_text()
                
                # If content matches (ignoring warning header), it's the same file
                existing_without_header = self._remove_warning_header(existing_content)
                if existing_without_header.strip() == new_content.strip():
                    return base_module_name  # Same file, no conflict
            except Exception:
                pass  # Continue with conflict resolution if file reading fails
        
        # There's a conflict - add directory prefix from module_path
        # For "transformations/customer/cleaner.py" -> "transformations_customer_cleaner"
        path_parts = Path(module_path).parent.parts
        if path_parts:
            prefix = "_".join(path_parts)
            return f"{prefix}_{base_module_name}"
        else:
            # File is in root, add a simple prefix
            return f"root_{base_module_name}"
    
    def _remove_warning_header(self, content: str) -> str:
        """Remove the warning header from copied file content."""
        lines = content.split('\n')
        # Find the end of the warning header (look for the closing ╝ character)
        for i, line in enumerate(lines):
            if '╝' in line:
                # Return content after the header and the empty line that follows
                return '\n'.join(lines[i+2:]) if i+2 < len(lines) else ""
        return content  # No header found, return as-is

    def _extract_source_views(self, config) -> list:
        """Legacy method - kept for backward compatibility during transition."""
        # This method is deprecated but kept to avoid breaking during refactor
        return self._extract_source_views_from_action_source(config.get("sources", []))
