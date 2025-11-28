"""Python transformation generator."""

import logging
import shutil
from pathlib import Path
from typing import Optional
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from .python_file_copier import PythonFunctionConflictError


class PythonTransformGenerator(BaseActionGenerator):
    """Generate Python transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

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
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

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
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
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
        logger = logging.getLogger(__name__)
        
        # Resolve source file path relative to project root
        source_file = project_root / module_path
        
        if not source_file.exists():
            raise FileNotFoundError(f"Python module file not found: {source_file}")
        
        # Extract module name from path (strip .py extension)
        module_name = Path(module_path).stem
        
        # Determine output directory for the current flowgroup
        flowgroup = context.get("flowgroup")
        if not flowgroup:
            raise ValueError("Flowgroup context required for Python file copying")
        
        # Get output directory
        output_dir = context.get("output_dir")
        if output_dir is None:
            # For dry-run mode, no file operations needed
            return module_name
        
        # Prepare destination
        custom_functions_dir = output_dir / "custom_python_functions"
        dest_file = custom_functions_dir / f"{module_name}.py"
        
        # Read and process source content
        original_content = source_file.read_text()
        
        # Apply substitutions if available
        if context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            original_content = substitution_mgr._process_string(original_content)
            
            # Track secret references
            secret_refs = substitution_mgr.get_secret_references()
            if "secret_references" in context and context["secret_references"] is not None:
                context["secret_references"].update(secret_refs)
        
        # Build content with header
        header = f"""# LHP-SOURCE: {module_path}
# Generated by LakehousePlumber - DO NOT EDIT
# Changes will be overwritten on next generation

"""
        full_content = header + original_content
        
        # Use thread-safe copier if available (always available in normal mode)
        python_copier = context.get("python_file_copier")
        if python_copier:
            # Thread-safe mode (parallel or sequential with conflict detection)
            python_copier.ensure_init_file(custom_functions_dir)
            file_copied = python_copier.copy_python_file(module_path, dest_file, full_content)
            
            # Track files with state manager (only if file was actually copied)
            if file_copied:
                state_manager = context.get("state_manager")
                source_yaml = context.get("source_yaml")
                if state_manager and source_yaml:
                    init_file = custom_functions_dir / "__init__.py"
                    
                    # Track the __init__.py file
                    state_manager.track_generated_file(
                        generated_path=init_file,
                        source_yaml=source_yaml,
                        environment=context.get("environment", "unknown"),
                        pipeline=flowgroup.pipeline,
                        flowgroup=flowgroup.flowgroup,
                        generation_context="",
                    )
                    logger.debug(f"Tracked additional file: {init_file} for Python transform")
                    
                    # Track the copied Python function file
                    state_manager.track_generated_file(
                        generated_path=dest_file,
                        source_yaml=source_yaml,
                        environment=context.get("environment", "unknown"),
                        pipeline=flowgroup.pipeline,
                        flowgroup=flowgroup.flowgroup,
                        generation_context="",
                    )
                    logger.debug(f"Tracked additional file: {dest_file} for Python transform")
        else:
            # Fallback mode for edge cases (dry-run, etc.)
            custom_functions_dir.mkdir(parents=True, exist_ok=True)
            init_file = custom_functions_dir / "__init__.py"
            init_file.write_text("# Generated package for custom Python functions\n")
            dest_file.write_text(full_content)
            logger.debug(f"Copied Python file (sequential mode): {module_path} → {dest_file.name}")
            
            # Track files with state manager if available
            state_manager = context.get("state_manager")
            source_yaml = context.get("source_yaml")
            if state_manager and source_yaml:
                # Track the __init__.py file
                state_manager.track_generated_file(
                    generated_path=init_file,
                    source_yaml=source_yaml,
                    environment=context.get("environment", "unknown"),
                    pipeline=flowgroup.pipeline,
                    flowgroup=flowgroup.flowgroup,
                    generation_context="",
                )
                logger.debug(f"Tracked additional file: {init_file} for Python transform")
                
                # Track the copied Python function file
                state_manager.track_generated_file(
                    generated_path=dest_file,
                    source_yaml=source_yaml,
                    environment=context.get("environment", "unknown"),
                    pipeline=flowgroup.pipeline,
                    flowgroup=flowgroup.flowgroup,
                    generation_context="",
                )
                logger.debug(f"Tracked additional file: {dest_file} for Python transform")
            else:
                logger.debug(f"Skipping file tracking - state_manager: {bool(state_manager)}, source_yaml: {bool(source_yaml)}")
        
        return module_name

