"""
Bundle manager for LHP Databricks Asset Bundle integration.

This module provides the main BundleManager class that coordinates bundle
resource operations including resource file synchronization and management.
"""

import logging
import re
from pathlib import Path
from typing import List, Dict, Optional, Union, Any

from .exceptions import BundleResourceError, YAMLParsingError
from ..utils.template_renderer import TemplateRenderer


logger = logging.getLogger(__name__)


class BundleManager:
    """
    Manages Databricks Asset Bundle resource files using a conservative approach.
    
    This class handles synchronization of bundle resource files with generated
    Python files while preserving user customizations and avoiding unnecessary
    modifications.
    
    Conservative Approach:
    - Preserves existing LHP-generated files (no modifications)
    - Backs up and replaces user-created files with LHP versions
    - Creates new files only when missing
    - Deletes orphaned files when Python directories are removed
    - Errors on ambiguous configurations (multiple files per pipeline)
    
    The conservative approach ensures Git stability by minimizing unnecessary
    file changes while maintaining LHP's ability to manage bundle resources.
    """
    
    def __init__(self, project_root: Union[Path, str], pipeline_config_path: Optional[str] = None):
        """
        Initialize the bundle manager.
        
        Args:
            project_root: Path to the project root directory
            pipeline_config_path: Optional path to custom pipeline config file (relative to project_root)
            
        Raises:
            TypeError: If project_root is None
        """
        if project_root is None:
            raise TypeError("project_root cannot be None")
            
        # Convert string to Path if necessary
        if isinstance(project_root, str):
            project_root = Path(project_root)
            
        self.project_root = project_root
        self.resources_base_dir = project_root / "resources" / "lhp"
        self.resources_dir = self.resources_base_dir  # Default to base dir for compatibility
        self.logger = logging.getLogger(__name__)
        
        # Set up template rendering using composition
        template_dir = Path(__file__).parent.parent / "templates"
        self.template_renderer = TemplateRenderer(template_dir)
        
        # Load pipeline config once at initialization for efficiency
        from ..core.services.pipeline_config_loader import PipelineConfigLoader
        self.config_loader = PipelineConfigLoader(self.project_root, pipeline_config_path)

    def _get_env_resources_dir(self, env: str) -> Path:
        """Get root-level resources directory (no longer environment-specific).
        
        Args:
            env: Environment name (kept for compatibility but not used)
            
        Returns:
            Path to root-level resources directory
        """
        return self.resources_base_dir
    
    def sync_resources_with_generated_files(self, output_dir: Path, env: str, 
                                           force: bool = False, 
                                           has_pipeline_config: bool = False) -> int:
        """
        Conservatively sync bundle resource files with generated Python files.
        
        Conservative approach - preserves existing LHP files:
        - Creates resource files for new pipeline directories  
        - Preserves existing LHP-generated files (no modifications)
        - Backs up and replaces user-created files with LHP versions
        - Deletes resource files for pipeline directories that no longer exist
        - Errors on multiple resource files for same pipeline
        
        Decision Matrix:
        | Python Dir | Bundle File Type | Action |
        |------------|------------------|--------|
        | Exists     | LHP file         | DON'T TOUCH (Scenario 1a) |
        | Exists     | LHP file + force + pc | REGENERATE (Scenario 1a override) |
        | Exists     | User file        | BACKUP + REPLACE (Scenario 1b) |
        | Exists     | No file          | CREATE (Scenario 2) |
        | Missing    | Any file         | DELETE (Scenario 3) |
        | Any        | Multiple         | ERROR (Scenario 4) |
        
        Args:
            output_dir: Directory containing generated Python files
            env: Environment name for template processing
            force: Force regeneration even for LHP-generated files
            has_pipeline_config: Whether a custom pipeline config was provided
            
        Returns:
            Number of resource files updated or removed
            
        Raises:
            BundleResourceError: If synchronization fails or multiple files detected
        """
        self.logger.info("Syncing bundle resources for environment: %s", env)
        
        # Setup: Prepare environment and gather current state
        current_pipeline_dirs, current_pipeline_names, existing_resource_files = \
            self._setup_sync_environment(env, output_dir)
        
        # Step 1: Process current pipelines using Conservative Approach
        updated_count = self._process_current_pipelines(current_pipeline_dirs, env, force, has_pipeline_config)
        
        # Step 2: Clean up orphaned resources  
        removed_count = self._cleanup_orphaned_resources(current_pipeline_names)
        
        # Step 3: Update configuration files
        self._update_configuration_files(output_dir, env)
        
        # Step 4: Log results and return summary
        self._log_sync_summary(updated_count, removed_count)
        return updated_count + removed_count

    def _sync_pipeline_resource(self, pipeline_name: str, pipeline_dir: Path, env: str,
                               force: bool = False, has_pipeline_config: bool = False) -> bool:
        """
        Sync a single pipeline resource file using conservative approach.
        
        Decision Logic:
        - Scenario 1a: Python exists + LHP file exists → DON'T TOUCH (preserve existing)
        - Scenario 1a override: Python exists + LHP file + force + pipeline config → REGENERATE
        - Scenario 1b: Python exists + User file exists → BACKUP + REPLACE  
        - Scenario 2:  Python exists + No file exists → CREATE
        - Scenario 4:  Multiple files exist → ERROR (configuration error)
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_dir: Directory containing pipeline Python files  
            env: Environment name
            force: Force regeneration even for LHP-generated files
            has_pipeline_config: Whether a custom pipeline config was provided
            
        Returns:
            True if resource file was created or updated, False if no changes needed
            
        Raises:
            BundleResourceError: If multiple files exist for same pipeline
        """
        # Step 1: Find all resource files for this pipeline
        related_files = self._find_all_resource_files_for_pipeline(pipeline_name)
        
        # Step 2: Scenario 4 - ERROR on multiple files (configuration error)
        if len(related_files) > 1:
            file_list = [str(f) for f in related_files]
            error_msg = (
                f"Multiple files define the same pipeline '{pipeline_name}_pipeline': {file_list}. "
                f"Each pipeline must be defined in only one resource file."
            )
            self.logger.error(error_msg)
            raise BundleResourceError(error_msg)
        
        # Step 3: Handle single file scenarios
        if related_files:
            existing_file = related_files[0]
            
            # Scenario 1a: LHP file exists - regenerate only with force + pipeline-config
            if self._is_lhp_generated_file(existing_file):
                if force and has_pipeline_config:
                    self.logger.info(f"Scenario 1a override: Force regenerating {existing_file.name} with pipeline config")
                    self._create_new_resource_file(pipeline_name, pipeline_dir.parent, env)
                    return True
                else:
                    self.logger.debug(f"Scenario 1a: Skipping {existing_file.name} (LHP file preserved)")
                    return False
            
            # Scenario 1b: User file exists - BACKUP + REPLACE  
            else:
                self.logger.info(f"Scenario 1b: Backing up user file {existing_file.name} and replacing with LHP version")
                self._backup_single_file(existing_file, pipeline_name)
                self._create_new_resource_file(pipeline_name, pipeline_dir.parent, env)
                return True
        
        # Step 4: Scenario 2 - No file exists, CREATE new
        else:
            self.logger.info(f"Scenario 2: Creating new resource file for pipeline '{pipeline_name}'")
            self._create_new_resource_file(pipeline_name, pipeline_dir.parent, env)
            return True

    def ensure_resources_directory(self):
        """Create resources/lhp directory if it doesn't exist."""
        self._safe_directory_create(self.resources_dir, "LHP resources directory")

    def get_pipeline_directories(self, output_dir: Path) -> List[Path]:
        """
        Get list of pipeline directories in the output directory.
        
        Args:
            output_dir: Directory to scan for pipeline directories
            
        Returns:
            List of pipeline directory paths in sorted order
            
        Raises:
            BundleResourceError: If directory access fails
        """
        # Validate directory access using utility
        self._safe_directory_access(output_dir, "output directory")
        
        try:
            pipeline_dirs = []
            # Sort directories to ensure deterministic processing order across platforms
            for item in sorted(output_dir.iterdir()):
                if item.is_dir():
                    pipeline_dirs.append(item)
                    self.logger.debug("Found pipeline directory: %s", item.name)
            
            return pipeline_dirs
            
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Error scanning output directory {output_dir}: {e}", e) from e



    def get_resource_file_path(self, pipeline_name: str) -> Path:
        """
        Find or generate resource file path for a pipeline.
        
        This method looks for existing resource files in order of preference:
        1. {pipeline_name}.pipeline.yml (preferred format)
        2. {pipeline_name}.yml (simple format)
        
        If neither exists, returns path for the preferred format.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Path to the resource file for this pipeline
        """
        # Check for preferred format first
        preferred_path = self.resources_dir / f"{pipeline_name}.pipeline.yml"
        if preferred_path.exists():
            return preferred_path
        
        # Check for simple format
        simple_path = self.resources_dir / f"{pipeline_name}.yml"
        if simple_path.exists():
            return simple_path
        
        # If neither exists, return preferred format for new file creation
        return preferred_path



    def generate_resource_file_content(self, pipeline_name: str, output_dir: Path, env: str) -> str:
        """
        Generate content for a bundle resource file using Jinja2 template.
        
        Applies LHP token substitution to ALL fields in pipeline_config.yaml, enabling
        environment-specific configuration for node types, policies, emails, and all other
        pipeline settings. Catalog/schema are validated if present.
        
        Args:
            pipeline_name: Name of the pipeline
            output_dir: Output directory
            env: Environment name for token resolution (REQUIRED)
            
        Returns:
            YAML content for the resource file with fully substituted pipeline config
            
        Raises:
            ValueError: If pipeline config validation fails
        """
        # Get RAW pipeline configuration
        pipeline_config_raw = self.config_loader.get_pipeline_config(pipeline_name)
        
        # Apply substitution to ENTIRE config (all fields)
        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
        
        if substitution_file.exists():
            from ..utils.substitution import EnhancedSubstitutionManager
            sub_mgr = EnhancedSubstitutionManager(substitution_file, env)
            pipeline_config_resolved = sub_mgr.substitute_yaml(pipeline_config_raw)
        else:
            # No substitution file - use raw config
            self.logger.debug(f"No substitution file found at {substitution_file}, using raw config")
            pipeline_config_resolved = pipeline_config_raw
        
        # Extract catalog/schema for validation (if present)
        catalog = pipeline_config_resolved.get('catalog')
        schema = pipeline_config_resolved.get('schema')
        
        # Validate catalog/schema pairing
        if catalog or schema:
            if not (catalog and schema):
                raise ValueError(
                    f"Pipeline '{pipeline_name}' config must define BOTH catalog AND schema, "
                    f"or neither. Found: catalog={'defined' if catalog else 'missing'}, "
                    f"schema={'defined' if schema else 'missing'}"
                )
            
            # Validate non-empty after substitution
            if not catalog.strip() or not schema.strip():
                raise ValueError(
                    f"Pipeline '{pipeline_name}' config has empty catalog or schema after substitution"
                )
            
            self.logger.info(
                f"Pipeline '{pipeline_name}' using catalog/schema from config: {catalog}.{schema}"
            )
        
        # Build template context with fully resolved config
        context = {
            "pipeline_name": pipeline_name,
            "pipeline_config": pipeline_config_resolved,  # Fully substituted!
            "catalog": catalog,
            "schema": schema,
        }
        
        return self.template_renderer.render_template("bundle/pipeline_resource.yml.j2", context)



    def _extract_most_common_database(self, database_values: List[str]) -> str:
        """
        Extract most common database value, first one in case of tie.
        
        Args:
            database_values: List of database strings from write_targets
            
        Returns:
            Most common database string, or None if list is empty
        """
        if not database_values:
            return None
        
        from collections import Counter
        counter = Counter(database_values)
        
        # Get most common - Counter.most_common() returns in order of frequency,
        # then by first occurrence for ties (deterministic)
        most_common = counter.most_common(1)[0][0]
        return most_common

    def _extract_first_global_catalog_schema(self, output_dir: Union[Path, str]) -> Dict[str, str]:
        """
        Extract FIRST catalog.schema found from ANY Python file across ALL pipelines.
        
        This method:
        1. Scans all pipeline directories in the output directory
        2. Searches through all Python files for catalog.schema patterns
        3. Returns the FIRST catalog.schema found (not most common)
        
        Args:
            output_dir: Output directory containing generated Python files (Path or str)
            
        Returns:
            Dict with 'catalog' and 'schema' keys containing resolved values
        """
        # Convert string to Path for backward compatibility
        if isinstance(output_dir, str):
            output_dir = self.project_root / "generated"
        
        if not output_dir.exists():
            self.logger.debug(f"Output directory not found: {output_dir}")
            return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}
        
        # Find all pipeline directories (directories containing Python files)
        # Sort for deterministic cross-platform behavior
        pipeline_dirs = sorted([d for d in output_dir.iterdir() if d.is_dir()])
        self.logger.debug(f"Found {len(pipeline_dirs)} pipeline directories in {output_dir}")
        
        # Search through all Python files across all pipelines
        for pipeline_dir in pipeline_dirs:
            # Sort Python files for deterministic ordering within each directory
            python_files = sorted(list(pipeline_dir.glob("*.py")))
            self.logger.debug(f"Searching {len(python_files)} Python files in {pipeline_dir.name}")
            
            for py_file in python_files:
                try:
                    content = py_file.read_text(encoding='utf-8')
                    file_database_values = self._extract_database_patterns(content)
                    
                    # Return the FIRST catalog.schema found
                    if file_database_values:
                        first_db_value = file_database_values[0]
                        self.logger.info(f"Found first global catalog.schema: {first_db_value} in {py_file}")
                        return self._parse_resolved_database_string(first_db_value)
                        
                except Exception as e:
                    self.logger.debug(f"Could not read {py_file}: {e}")
                    continue
        
        # No catalog.schema found in any Python files
        self.logger.info("No catalog.schema patterns found in any Python files, using defaults")
        return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}

    def _get_all_substitution_environments(self) -> List[str]:
        """
        Discover all substitution environment files and return environment names.
        
        Scans the substitutions/ directory for all *.yaml files and extracts
        the environment names (filename without extension).
        
        Returns:
            List of environment names found in substitutions directory
            
        Example:
            If substitutions/ contains: dev.yaml, tst.yaml, prod.yaml
            Returns: ['dev', 'tst', 'prod']
        """
        substitutions_dir = self.project_root / "substitutions"
        
        if not substitutions_dir.exists():
            self.logger.debug(f"Substitutions directory not found: {substitutions_dir}")
            return []
        
        # Find all .yaml files in substitutions directory
        yaml_files = list(substitutions_dir.glob("*.yaml"))
        environments = [f.stem for f in yaml_files if f.is_file()]
        
        # Sort for consistent ordering
        environments.sort()
        
        self.logger.debug(f"Found {len(environments)} substitution environments: {environments}")
        return environments

    def _find_substitution_variables_for_values(self, catalog: str, schema: str, env: str) -> Dict[str, str]:
        """
        Find variable names in substitution files that contain specific catalog/schema values.
        
        Parses the specified environment's substitution file to find which variable names
        contain the given catalog and schema values.
        
        Args:
            catalog: Catalog value to search for (e.g., "acmi_edw_dev")
            schema: Schema value to search for (e.g., "bronze_layer") 
            env: Environment name to search in (e.g., "dev")
            
        Returns:
            Dict mapping 'catalog_var' and 'schema_var' to variable names found
            
        Example:
            If substitutions/dev.yaml contains:
                catalog: "acmi_edw_dev"
                bronze_schema: "bronze_layer"
            And we search for catalog="acmi_edw_dev", schema="bronze_layer"
            Returns: {"catalog_var": "catalog", "schema_var": "bronze_schema"}
        """
        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
        
        if not substitution_file.exists():
            self.logger.warning(f"Substitution file not found: {substitution_file}")
            return {"catalog_var": None, "schema_var": None}
        
        try:
            # Import yaml here to use PyYAML (not ruamel.yaml)
            import yaml
            
            from ..utils.yaml_loader import load_yaml_file
            config = load_yaml_file(substitution_file, error_context="bundle substitution file")
            
            if not config:
                self.logger.warning(f"Empty substitution file: {substitution_file}")
                return {"catalog_var": None, "schema_var": None}
            
            # Get environment-specific config
            env_config = config.get(env, {})
            if not env_config:
                self.logger.warning(f"No configuration found for environment '{env}' in {substitution_file}")
                return {"catalog_var": None, "schema_var": None}
            
            # Search for variables with matching values
            catalog_var = None
            schema_var = None
            
            for var_name, var_value in env_config.items():
                if isinstance(var_value, str):
                    if var_value == catalog:
                        catalog_var = var_name
                        self.logger.debug(f"Found catalog variable: {var_name} = {var_value}")
                    if var_value == schema:
                        schema_var = var_name
                        self.logger.debug(f"Found schema variable: {var_name} = {var_value}")
            
            result = {"catalog_var": catalog_var, "schema_var": schema_var}
            self.logger.info(f"Variable resolution for {env}: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing substitution file {substitution_file}: {e}")
            return {"catalog_var": None, "schema_var": None}

    def _validate_databricks_targets_exist(self, environments: List[str]) -> None:
        """
        Validate that all required targets exist in databricks.yml.
        
        Checks that databricks.yml exists and contains target definitions for
        all specified environments. Raises error if any targets are missing.
        
        Args:
            environments: List of environment names that should have targets
                         (e.g., ['dev', 'tst', 'prod'])
            
        Raises:
            FileNotFoundError: If databricks.yml doesn't exist
            MissingDatabricksTargetError: If any targets are missing
            BundleResourceError: If databricks.yml is malformed
        """
        databricks_file = self.project_root / "databricks.yml"
        
        if not databricks_file.exists():
            raise FileNotFoundError(
                f"databricks.yml not found at {databricks_file}. "
                f"Please ensure your project has a databricks.yml file."
            )
        
        try:
            # Import yaml here to use PyYAML (not ruamel.yaml)
            import yaml
            
            from ..utils.yaml_loader import load_yaml_file
            try:
                data = load_yaml_file(databricks_file, allow_empty=False, error_context="databricks.yml")
            except ValueError as e:
                raise BundleResourceError(f"Failed to load databricks.yml: {e}")
            
            if not data:
                raise BundleResourceError(
                    f"databricks.yml is empty or malformed: {databricks_file}"
                )
            
            if 'targets' not in data:
                from .exceptions import MissingDatabricksTargetError
                raise MissingDatabricksTargetError(
                    f"databricks.yml missing 'targets' section. "
                    f"Please add a targets section with your environment configurations."
                )
            
            targets = data['targets']
            if not isinstance(targets, dict):
                raise BundleResourceError(
                    f"databricks.yml 'targets' section must be a dictionary, "
                    f"but found {type(targets).__name__}"
                )
            
            # Check for missing targets
            missing_targets = [env for env in environments if env not in targets]
            
            if missing_targets:
                from .exceptions import MissingDatabricksTargetError
                available_targets = list(targets.keys())
                raise MissingDatabricksTargetError(
                    f"Missing targets in databricks.yml: {missing_targets}. "
                    f"Available targets: {available_targets}. "
                    f"Please add the missing target configurations to your databricks.yml file."
                )
            
            self.logger.debug(f"All required targets exist in databricks.yml: {environments}")
            
        except (yaml.YAMLError, OSError) as e:
            raise BundleResourceError(
                f"Failed to read or parse databricks.yml: {e}"
            ) from e

    def _update_databricks_variables(self, output_dir: Path, current_env: str) -> None:
        """
        Update databricks.yml with pipeline variables for all environments.
        
        This method implements the complete new flow:
        1. Extract first global catalog.schema from any Python file
        2. Get all substitution environments 
        3. Find variable names for catalog/schema values in current environment
        4. Load actual values from each environment's substitution file
        5. Update databricks.yml variables for all targets
        
        Args:
            output_dir: Directory containing generated Python files
            current_env: Current environment being generated (e.g., 'dev')
            
        Raises:
            BundleResourceError: If the update process fails
            MissingDatabricksTargetError: If required targets are missing
        """
        self.logger.info("Updating databricks.yml with pipeline variables...")
        
        # Step 1: Extract first global catalog.schema
        global_db_info = self._extract_first_global_catalog_schema(output_dir)
        catalog_value = global_db_info.get("catalog")
        schema_value = global_db_info.get("schema")
        
        if not catalog_value or not schema_value:
            self.logger.warning(
                f"Could not find catalog.schema in Python files. "
                f"Found: catalog={catalog_value}, schema={schema_value}. "
                f"Skipping databricks.yml variable update."
            )
            return
        
        self.logger.info(f"Found global catalog.schema: {catalog_value}.{schema_value}")
        
        # Step 2: Get all substitution environments
        all_environments = self._get_all_substitution_environments()
        if not all_environments:
            self.logger.warning("No substitution files found. Skipping databricks.yml variable update.")
            return
        
        self.logger.debug(f"Found substitution environments: {all_environments}")
        
        # Step 2.5: Identify pipelines with catalog/schema in config
        pipelines_with_config = set()
        pipelines_with_config_errors = {}
        all_pipeline_names = set()
        
        for pipeline_dir in sorted([d for d in output_dir.iterdir() if d.is_dir()]):
            pipeline_name = pipeline_dir.name
            all_pipeline_names.add(pipeline_name)
            
            try:
                # Check if pipeline config defines catalog/schema
                pipeline_config = self.config_loader.get_pipeline_config(pipeline_name)
                if pipeline_config.get('catalog') or pipeline_config.get('schema'):
                    pipelines_with_config.add(pipeline_name)
                    self.logger.debug(
                        f"Pipeline '{pipeline_name}' has catalog/schema in config - "
                        f"will skip databricks.yml variable update"
                    )
            except Exception as e:
                pipelines_with_config_errors[pipeline_name] = str(e)
                self.logger.warning(
                    f"Pipeline '{pipeline_name}' config check failed: {e}"
                )
        
        # Skip entirely if ALL pipelines have config
        if pipelines_with_config and pipelines_with_config == all_pipeline_names:
            self.logger.info(
                f"All {len(all_pipeline_names)} pipelines have catalog/schema in config. "
                f"Skipping databricks.yml variable update entirely."
            )
            return
        
        if pipelines_with_config:
            self.logger.info(
                f"{len(pipelines_with_config)} pipelines have config-defined catalog/schema: "
                f"{sorted(pipelines_with_config)}"
            )
        
        # Step 3: Find variable names in current environment's substitution file
        variable_info = self._find_substitution_variables_for_values(
            catalog_value, schema_value, current_env
        )
        catalog_var_name = variable_info.get("catalog_var")
        schema_var_name = variable_info.get("schema_var")
        
        if not catalog_var_name or not schema_var_name:
            self.logger.warning(
                f"Could not find variable names in {current_env} substitution file for "
                f"catalog='{catalog_value}', schema='{schema_value}'. "
                f"Found: catalog_var={catalog_var_name}, schema_var={schema_var_name}. "
                f"Skipping databricks.yml variable update."
            )
            return
        
        self.logger.info(f"Using variable names: {catalog_var_name}={catalog_value}, {schema_var_name}={schema_value}")
        
        # Step 4: Validate all targets exist in databricks.yml
        self._validate_databricks_targets_exist(all_environments)
        
        # Step 5: Load actual values from each environment's substitution files
        environment_variables = {}
        for env in all_environments:
            env_values = self._load_substitution_values_for_environment(env, catalog_var_name, schema_var_name)
            environment_variables[env] = {
                "default_pipeline_catalog": env_values["catalog"],
                "default_pipeline_schema": env_values["schema"]
            }
        
        # Step 6: Update databricks.yml using DatabricksYAMLManager
        from .databricks_yaml_manager import DatabricksYAMLManager
        
        databricks_manager = DatabricksYAMLManager(self.project_root)
        databricks_manager.bulk_update_all_targets(all_environments, environment_variables)
        
        self.logger.info(f"Updated databricks.yml variables for {len(all_environments)} targets: {all_environments}")

    def _load_substitution_values_for_environment(self, env: str, catalog_var: str, schema_var: str) -> Dict[str, str]:
        """
        Load actual catalog and schema values from a specific environment's substitution file.
        
        Args:
            env: Environment name (e.g., 'dev')
            catalog_var: Variable name for catalog (e.g., 'catalog')
            schema_var: Variable name for schema (e.g., 'bronze_schema')
            
        Returns:
            Dict with 'catalog' and 'schema' keys containing actual values
        """
        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
        
        if not substitution_file.exists():
            self.logger.warning(f"Substitution file not found: {substitution_file}")
            return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}
        
        try:
            from ..utils.yaml_loader import load_yaml_file
            config = load_yaml_file(substitution_file, error_context="bundle substitution file")
            
            env_config = config.get(env, {}) if config else {}
            
            catalog_value = env_config.get(catalog_var, "main")
            schema_value = env_config.get(schema_var, f"lhp_${{bundle.target}}")
            
            self.logger.debug(f"Loaded values for {env}: {catalog_var}={catalog_value}, {schema_var}={schema_value}")
            
            return {
                "catalog": catalog_value,
                "schema": schema_value
            }
            
        except Exception as e:
            self.logger.error(f"Error loading substitution values from {substitution_file}: {e}")
            return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}

    def _extract_database_from_python_files(self, pipeline_name: str, output_dir: Union[Path, str]) -> Dict[str, str]:
        """
        Extract database info from generated Python files using regex patterns.
        
        This method:
        1. Finds all Python files in the pipeline directory
        2. Extracts catalog.schema patterns using regex
        3. Selects most common database value (first on tie)
        4. Parses catalog.schema components (no substitution needed)
        
        Args:
            pipeline_name: Name of the pipeline to search for
            output_dir: Output directory containing generated Python files (Path or str)
            
        Returns:
            Dict with 'catalog' and 'schema' keys containing resolved values
        """
        # Convert string to Path for backward compatibility
        if isinstance(output_dir, str):
            output_dir = self.project_root / "generated"
        
        pipeline_dir = output_dir / pipeline_name
        
        if not pipeline_dir.exists():
            self.logger.debug(f"Pipeline directory not found: {pipeline_dir}")
            return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}
        
        # Find all Python files in pipeline directory
        python_files = list(pipeline_dir.glob("*.py"))
        self.logger.debug(f"Found {len(python_files)} Python files in {pipeline_dir}")
        
        database_values = []
        processed_files = 0
        
        for py_file in python_files:
            try:
                content = py_file.read_text(encoding='utf-8')
                file_database_values = self._extract_database_patterns(content)
                database_values.extend(file_database_values)
                processed_files += 1
                self.logger.debug(f"Extracted {len(file_database_values)} database values from {py_file.name}")
            except Exception as e:
                self.logger.debug(f"Could not read {py_file}: {e}")
                continue
        
        self.logger.debug(f"Processed {processed_files} Python files with {len(database_values)} total database values")
        
        # Find most common database value
        most_common_db = self._extract_most_common_database(database_values)
        
        if not most_common_db:
            self.logger.info(f"No database values found in Python files for pipeline '{pipeline_name}', using defaults")
            return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}
        
        self.logger.debug(f"Most common database value: {most_common_db}")
        
        # Parse resolved database string (no substitution needed)
        return self._parse_resolved_database_string(most_common_db)

    def _extract_database_patterns(self, content: str) -> List[str]:
        """
        Extract catalog.schema patterns from Python file content using regex.
        
        Looks for these patterns:
        - dp.create_streaming_table(name="catalog.schema.table")
        - @dp.materialized_view(name="catalog.schema.table")
        
        Args:
            content: Python file content as string
            
        Returns:
            List of catalog.schema strings found in the content
        """
        # Regex patterns for table creation (only patterns that create new tables)
        patterns = [
            r'dp\.create_streaming_table\(\s*\n?\s*name="([^"]+)"',  # streaming tables
            r'@dp\.materialized_view\(\s*\n?\s*name="([^"]+)"',                  # materialized views
        ]
        
        database_values = []
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.MULTILINE)
            for match in matches:
                # Only extract catalog.schema from catalog.schema.table patterns
                if "." in match and len(match.split(".")) >= 2:
                    parts = match.split(".")
                    database_value = f"{parts[0]}.{parts[1]}"  # catalog.schema
                    database_values.append(database_value)
                    self.logger.debug(f"Found database pattern: {database_value} from table: {match}")
        
        return database_values

    def _parse_resolved_database_string(self, database_string: str) -> Dict[str, str]:
        """
        Parse already-resolved database string to extract catalog and schema.
        
        Since Python files contain fully resolved values (templates and substitutions
        already applied), no token resolution is needed.
        
        Args:
            database_string: Resolved database string like "acmi_edw_dev.edw_bronze"
            
        Returns:
            Dict with 'catalog' and 'schema' keys
            
        Example:
            Input: "acmi_edw_dev.edw_bronze"
            Output: {"catalog": "acmi_edw_dev", "schema": "edw_bronze"}
        """
        if not database_string or "." not in database_string:
            self.logger.debug(f"Invalid database string '{database_string}', using defaults")
            return {"catalog": "main", "schema": f"lhp_${{bundle.target}}"}
        
        # Split on first dot only (catalog.schema)
        parts = database_string.split(".", 1)
        result = {
            "catalog": parts[0],
            "schema": parts[1]
        }
        
        self.logger.debug(f"Parsed resolved database string '{database_string}' to: {result}")
        return result

    def _create_unique_backup_path(self, resource_file: Path) -> Path:
        """
        Create a unique backup file path with .bkup extension.
        
        This utility method supports the Conservative Approach by providing consistent
        backup path generation with automatic collision handling for all backup scenarios.
        
        Args:
            resource_file: Path to the resource file to backup
            
        Returns:
            Path to a unique backup file (handles existing backups with counters)
            
        Example:
            - First backup: file.yml → file.yml.bkup
            - Second backup: file.yml → file.yml.bkup.1
            - Third backup: file.yml → file.yml.bkup.2
        """
        backup_file = resource_file.with_suffix(resource_file.suffix + '.bkup')
        
        if not backup_file.exists():
            return backup_file
            
        counter = 1
        original_backup = backup_file
        while backup_file.exists():
            backup_file = original_backup.with_suffix(f'.bkup.{counter}')
            counter += 1
        
        return backup_file

    # === UTILITY METHODS ===
    
    def _safe_directory_create(self, directory: Path, error_context: str = "directory") -> None:
        """
        Safely create directory with consistent error handling.
        
        Args:
            directory: Path to directory to create
            error_context: Context for error messages
            
        Raises:
            BundleResourceError: If directory creation fails
        """
        try:
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.debug("Ensured %s exists: %s", error_context, directory)
        except OSError as e:
            raise BundleResourceError(f"Failed to create {error_context}: {e}", e) from e
    
    def _safe_directory_access(self, directory: Path, error_context: str = "directory") -> None:
        """
        Safely validate directory access with consistent error handling.
        
        Args:
            directory: Path to directory to validate
            error_context: Context for error messages
            
        Raises:
            BundleResourceError: If directory access fails
        """
        try:
            if not directory.exists():
                raise BundleResourceError(f"{error_context.capitalize()} does not exist: {directory}")
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Cannot access {error_context} {directory}: {e}", e) from e

    def _handle_pipeline_error(self, pipeline_name: str, error: Exception, operation: str) -> BundleResourceError:
        """
        Create consistent error messages for pipeline operations.
        
        Args:
            pipeline_name: Name of the pipeline being processed
            error: Original exception that occurred
            operation: Description of the operation that failed
            
        Returns:
            BundleResourceError with consistent formatting and context
        """
        if isinstance(error, YAMLParsingError):
            error_msg = f"YAML processing failed for pipeline '{pipeline_name}': {error}"
        elif isinstance(error, OSError):
            error_msg = f"File system error for pipeline '{pipeline_name}': {error}"
        else:
            error_msg = f"{operation} failed for pipeline '{pipeline_name}': {error}"
        
        self.logger.error(error_msg)
        return BundleResourceError(error_msg, error)

    # === MAIN WORKFLOW METHODS ===

    def _setup_sync_environment(self, env: str, output_dir: Path) -> tuple[List[Path], set[str], List[Dict[str, Any]]]:
        """
        Setup sync environment and gather current state.
        
        Args:
            env: Environment name for template processing
            output_dir: Directory containing generated Python files
            
        Returns:
            Tuple of (current_pipeline_dirs, current_pipeline_names, existing_resource_files)
        """
        # Set environment-specific resources directory
        self.resources_dir = self._get_env_resources_dir(env)
        
        # Ensure resources directory exists
        self.ensure_resources_directory()
        
        # Get current state
        current_pipeline_dirs = self.get_pipeline_directories(output_dir)
        current_pipeline_names = {pipeline_dir.name for pipeline_dir in current_pipeline_dirs}
        existing_resource_files = self._get_existing_resource_files()
        
        return current_pipeline_dirs, current_pipeline_names, existing_resource_files

    def _process_current_pipelines(self, current_pipeline_dirs: List[Path], env: str,
                                  force: bool = False, has_pipeline_config: bool = False) -> int:
        """
        Process current pipeline directories using Conservative Approach.
        
        Args:
            current_pipeline_dirs: List of pipeline directories to process
            env: Environment name for template processing
            force: Force regeneration even for LHP-generated files
            has_pipeline_config: Whether a custom pipeline config was provided
            
        Returns:
            Number of pipelines that were updated
            
        Raises:
            BundleResourceError: If pipeline processing fails
        """
        updated_count = 0
        
        # Step 1: Create/update resource files for current pipeline directories
        for pipeline_dir in current_pipeline_dirs:
            pipeline_name = pipeline_dir.name
            
            try:
                if self._sync_pipeline_resource(pipeline_name, pipeline_dir, env, force, has_pipeline_config):
                    updated_count += 1
                    self.logger.debug("Successfully synced pipeline: %s", pipeline_name)
                    
            except (YAMLParsingError, OSError, Exception) as e:
                raise self._handle_pipeline_error(pipeline_name, e, "Pipeline sync")
        
        return updated_count

    def _cleanup_orphaned_resources(self, current_pipeline_names: set[str]) -> int:
        """
        Clean up resource files for pipelines that no longer exist.
        
        Args:
            current_pipeline_names: Set of currently existing pipeline names
            
        Returns:
            Number of resource files that were removed
        """
        removed_count = 0
        
        # Step 2: Backup resource files for pipeline directories that no longer exist
        # Check ALL files in resources/lhp, not just LHP-generated ones
        all_resource_files = self._get_all_resource_files_in_lhp_directory()
        for resource_file_info in all_resource_files:
            pipeline_name = resource_file_info["pipeline_name"]
            resource_file = resource_file_info["path"]
            
            if pipeline_name not in current_pipeline_names:
                try:
                    self._delete_resource_file(resource_file, pipeline_name)
                    removed_count += 1
                    self.logger.debug("Successfully deleted resource file for removed pipeline: %s", pipeline_name)
                    
                except Exception as e:
                    self.logger.warning("Failed to delete resource file %s: %s", resource_file, e)
        
        return removed_count

    def _update_configuration_files(self, output_dir: Path, env: str) -> None:
        """
        Update databricks.yml with pipeline variables.
        
        Args:
            output_dir: Directory containing generated Python files
            env: Environment name for template processing
            
        Raises:
            BundleResourceError: If databricks.yml update fails
        """
        # Step 3: NEW - Update databricks.yml with pipeline variables
        try:
            self._update_databricks_variables(output_dir, env)
        except Exception as e:
            self.logger.error("Failed to update databricks.yml variables: %s", e)
            raise BundleResourceError(f"Databricks YAML variable update failed: {e}", e)

    def _log_sync_summary(self, updated_count: int, removed_count: int) -> None:
        """
        Log Conservative Approach sync results summary.
        
        Uses f-string formatting for user-facing messages that tests expect.
        
        Args:
            updated_count: Number of pipelines updated
            removed_count: Number of resource files removed
        """
        # Log summary with conservative approach context (user-facing messages)
        if updated_count > 0 or removed_count > 0:
            if updated_count > 0 and removed_count > 0:
                self.logger.info(f"Bundle sync completed: updated {updated_count}, deleted {removed_count} resource file(s)")
            elif updated_count > 0:
                self.logger.info(f"Updated {updated_count} bundle resource file(s) (created new or replaced user files)")
            else:
                self.logger.info(f"Deleted {removed_count} orphaned bundle resource file(s)")
        else:
            self.logger.info("All bundle resources preserved (conservative approach - existing LHP files untouched)")

    def _create_new_resource_file(self, pipeline_name: str, output_dir: Path, env: str):
        """
        Create new resource file for a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            output_dir: Output directory containing generated Python files
            env: Environment name for template context
        """
        # Ensure resources directory exists
        self.ensure_resources_directory()
        
        resource_file = self.get_resource_file_path(pipeline_name)
        
        # Generate resource file content from Python files
        content = self.generate_resource_file_content(pipeline_name, output_dir, env)
        
        try:
            resource_file.write_text(content, encoding='utf-8')
            self.logger.info(f"Created new resource file: {resource_file}")
            
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to create resource file {resource_file}: {e}", e)

    

    def _get_existing_resource_files(self) -> List[Dict[str, Any]]:
        """
        Get list of existing resource files in the resources directory.
        
        Returns:
            List of dictionaries with 'path' and 'pipeline_name' keys
        """
        resource_files = []
        
        if not self.resources_dir.exists():
            return resource_files
            
        try:
            # Look for pipeline resource files (.pipeline.yml and .yml)
            for resource_file in self.resources_dir.glob("*.yml"):
                pipeline_name = self._extract_pipeline_name_from_resource_file(resource_file)
                if pipeline_name:
                    resource_files.append({
                        "path": resource_file,
                        "pipeline_name": pipeline_name
                    })
                    self.logger.debug(f"Found existing resource file: {resource_file.name} for pipeline: {pipeline_name}")
            
            return resource_files
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Error scanning resources directory {self.resources_dir}: {e}")
            return []

    def _extract_pipeline_name_from_resource_file(self, resource_file: Path) -> Optional[str]:
        """
        Extract pipeline name from resource file by parsing YAML content.
        
        Changed from filename-based to YAML content-based extraction for accuracy.
        
        Args:
            resource_file: Path to the resource file
            
        Returns:
            Pipeline name (directory name without _pipeline suffix) if LHP-generated, None otherwise
        """
        # First check if this is an LHP-generated file
        if not self._is_lhp_generated_file(resource_file):
            self.logger.debug(f"Skipping non-LHP file: {resource_file.name}")
            return None
            
        try:
            # Extract pipeline keys from YAML content
            pipeline_keys = self._extract_pipeline_keys_from_file(resource_file)
            
            if pipeline_keys:
                # Get first pipeline key (only one per file in Databricks bundles)
                pipeline_key = pipeline_keys[0]
                
                # Remove '_pipeline' suffix to get directory name
                # 'bronze_ncr_pipeline' -> 'bronze_ncr'
                if pipeline_key.endswith('_pipeline'):
                    return pipeline_key[:-9]  # Remove '_pipeline' (9 chars)
                
                # If no suffix, return as-is (edge case)
                return pipeline_key
                
        except BundleResourceError:
            # Already logged in _extract_pipeline_keys_from_file
            return None
            
        return None

    

    def _is_lhp_generated_file(self, resource_file: Path) -> bool:
        """
        Check if a resource file was generated by LHP by examining its content.
        
        Args:
            resource_file: Path to the resource file to check
            
        Returns:
            True if the file was generated by LHP, False otherwise
        """
        try:
            if not resource_file.exists() or not resource_file.is_file():
                return False
                
            # Read first few lines to check for LHP header
            with open(resource_file, 'r', encoding='utf-8') as f:
                first_lines = []
                for _ in range(5):  # Check first 5 lines
                    line = f.readline()
                    if not line:
                        break
                    first_lines.append(line.strip())
                
                # Look for LHP signature in the first few lines
                content = '\n'.join(first_lines)
                return "Generated by LakehousePlumber" in content
                
        except (OSError, PermissionError, UnicodeDecodeError) as e:
            self.logger.debug(f"Could not read file {resource_file} for LHP detection: {e}")
            return False

    def _get_all_resource_files_in_lhp_directory(self) -> List[Dict[str, Any]]:
        """
        Get ALL resource files in the resources/lhp directory, regardless of headers.
        
        Returns:
            List of dictionaries with 'path' and 'pipeline_name' keys
        """
        resource_files = []
        
        if not self.resources_dir.exists():
            return resource_files
            
        try:
            # Look for ALL pipeline resource files (.pipeline.yml and .yml)
            for resource_file in self.resources_dir.glob("*.yml"):
                # Extract pipeline name from filename (not header check)
                pipeline_name = self._extract_pipeline_name_from_filename(resource_file)
                if pipeline_name:
                    resource_files.append({
                        "path": resource_file,
                        "pipeline_name": pipeline_name
                    })
                    self.logger.debug(f"Found resource file: {resource_file.name} for pipeline: {pipeline_name}")
            
            return resource_files
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Error scanning resources directory {self.resources_dir}: {e}")
            return []

    def _extract_pipeline_name_from_filename(self, resource_file: Path) -> Optional[str]:
        """
        Extract pipeline name from resource file name (regardless of header).
        
        Args:
            resource_file: Path to the resource file
            
        Returns:
            Pipeline name extracted from filename, or None if not a pipeline file
        """
        file_name = resource_file.name
        
        # Handle .pipeline.yml format
        if file_name.endswith(".pipeline.yml"):
            return file_name[:-13]  # Remove ".pipeline.yml"
        
        # Handle .yml format  
        elif file_name.endswith(".yml"):
            return file_name[:-4]   # Remove ".yml"
        
        return None

    

    def _delete_resource_file(self, resource_file: Path, pipeline_name: str):
        """
        Delete a resource file for a pipeline that no longer exists.
        
        Args:
            resource_file: Path to the resource file to delete
            pipeline_name: Name of the pipeline (for logging)
            
        Raises:
            BundleResourceError: If file deletion fails
        """
        try:
            if resource_file.exists():
                # Delete the file
                resource_file.unlink()
                self.logger.info(f"Deleted resource file: {resource_file.name} (pipeline '{pipeline_name}' no longer exists)")
            else:
                self.logger.debug(f"Resource file already removed: {resource_file}")
                
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to delete resource file {resource_file}: {e}", e)

    def _find_all_resource_files_for_pipeline(self, pipeline_name: str) -> List[Path]:
        """
        Find resource files that define the given pipeline by parsing YAML content.
        
        This checks actual pipeline definitions in YAML (resources.pipelines keys),
        not filename patterns. Prevents false positives from similar filename prefixes.
        
        Args:
            pipeline_name: Name of the pipeline directory (e.g., 'bronze_ncr')
            
        Returns:
            List of Path objects for files that define this pipeline
            
        Raises:
            BundleResourceError: If YAML files are malformed
        """
        matching_files = []
        expected_pipeline_key = f"{pipeline_name}_pipeline"
        
        if not self.resources_dir.exists():
            return matching_files
        
        try:
            # Get all YAML files (both .yml and .yaml extensions)
            yaml_files = []
            for ext in ['*.yml', '*.yaml']:
                yaml_files.extend(self.resources_dir.glob(ext))
            
            for file_path in yaml_files:
                # Skip backup files
                if '.bkup' in file_path.name:
                    self.logger.debug(f"Skipping backup file: {file_path.name}")
                    continue
                    
                # Parse YAML and check for pipeline definition
                pipeline_keys = self._extract_pipeline_keys_from_file(file_path)
                if expected_pipeline_key in pipeline_keys:
                    matching_files.append(file_path)
                    self.logger.debug(f"Found pipeline '{expected_pipeline_key}' in {file_path.name}")
                    
            return matching_files
            
        except BundleResourceError:
            # Re-raise BundleResourceError as-is (already formatted)
            raise
        except (OSError, PermissionError) as e:
            error_msg = f"Error accessing resource files for pipeline '{pipeline_name}': {e}"
            self.logger.error(error_msg)
            raise BundleResourceError(error_msg, e)

    def _extract_pipeline_keys_from_file(self, resource_file: Path) -> List[str]:
        """
        Extract pipeline resource keys from a YAML file.
        
        Parses the YAML structure to find keys under resources.pipelines section.
        Used for content-based duplicate detection.
        
        Args:
            resource_file: Path to the resource file to parse
            
        Returns:
            List of pipeline keys found (e.g., ['bronze_ncr_pipeline'])
            
        Raises:
            BundleResourceError: If YAML is malformed or unreadable
        """
        try:
            import yaml
            
            with open(resource_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            # Navigate YAML structure: resources -> pipelines -> keys
            if data and isinstance(data, dict):
                resources = data.get('resources', {})
                if isinstance(resources, dict):
                    pipelines = resources.get('pipelines', {})
                    if isinstance(pipelines, dict):
                        return list(pipelines.keys())
            
            return []
            
        except yaml.YAMLError as e:
            error_msg = f"Malformed YAML in {resource_file.name}: {e}"
            self.logger.error(error_msg)
            raise BundleResourceError(error_msg, e)
        except Exception as e:
            error_msg = f"Failed to read {resource_file.name}: {e}"
            self.logger.error(error_msg)
            raise BundleResourceError(error_msg, e)

    def _backup_single_file(self, resource_file: Path, pipeline_name: str):
        """
        Backup a single resource file - Conservative Approach: Scenario 1b.
        
        ACTIVE METHOD - Used in user file backup+replace workflow.
        This method implements the Conservative Approach for preserving user customizations
        by backing up user-created files before replacing them with LHP versions.
        
        Args:
            resource_file: Path to the resource file to backup
            pipeline_name: Name of the pipeline (for logging) - kept for API compatibility
        """
        try:
            if resource_file.exists():
                backup_path = self._create_unique_backup_path(resource_file)
                resource_file.rename(backup_path)
                self.logger.info(f"Backed up file: {resource_file.name} -> {backup_path.name}")
            
        except (OSError, PermissionError) as e:
            # Conservative Approach: Only warn on backup failure, don't stop processing
            self.logger.warning("Failed to backup file %s: %s", resource_file, e) 

 