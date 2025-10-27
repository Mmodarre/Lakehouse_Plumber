"""Pipeline configuration loader with multi-document YAML support."""

import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from copy import deepcopy

logger = logging.getLogger(__name__)


class PipelineConfigLoader:
    """
    Load and merge pipeline configurations from multi-document YAML.
    
    Supports project-level defaults and per-pipeline overrides.
    Config loaded once at initialization for efficiency.
    """
    
    DEFAULT_PIPELINE_CONFIG = {
        "serverless": True,
        "edition": "ADVANCED",
        "channel": "CURRENT",
        "continuous": False
    }
    
    ALLOWED_EDITIONS = {"CORE", "PRO", "ADVANCED"}
    ALLOWED_CHANNELS = {"CURRENT", "PREVIEW"}
    
    def __init__(self, project_root: Path, config_file_path: Optional[str] = None):
        """
        Initialize and load config.
        
        Args:
            project_root: Project root directory
            config_file_path: Config file path relative to project_root or absolute
            
        Raises:
            FileNotFoundError: If explicit config_file_path doesn't exist
            yaml.YAMLError: If YAML syntax is invalid
            ValueError: If config validation fails
        """
        self.project_root = Path(project_root)
        self.logger = logger
        
        # Load and parse config
        self.project_defaults, self.pipeline_configs = self._load_config(config_file_path)
    
    def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Get merged config for a specific pipeline.
        
        Merge order: DEFAULT -> project_defaults -> pipeline_specific
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Merged configuration dictionary
        """
        # Start with defaults (deep copy to avoid mutation)
        config = deepcopy(self.DEFAULT_PIPELINE_CONFIG)
        
        # Deep merge with project_defaults
        if self.project_defaults:
            config = self._deep_merge(config, self.project_defaults)
        
        # Deep merge with pipeline-specific (if exists)
        if pipeline_name in self.pipeline_configs:
            config = self._deep_merge(config, self.pipeline_configs[pipeline_name])
        
        return config
    
    def _load_config(self, config_file_path: Optional[str]) -> Tuple[Dict, Dict]:
        """
        Load and parse multi-document YAML config.
        
        Returns:
            Tuple of (project_defaults, pipeline_configs)
            - project_defaults: Dict with project-level settings
            - pipeline_configs: Dict mapping pipeline names to their configs
        """
        # No config file specified - return empty defaults
        if config_file_path is None:
            self.logger.debug("No pipeline config file specified, using defaults only")
            return {}, {}
        
        # Resolve config file path
        config_path = Path(config_file_path)
        if not config_path.is_absolute():
            config_path = self.project_root / config_path
        
        # Check file exists
        if not config_path.exists():
            raise FileNotFoundError(
                f"Pipeline config file not found: {config_file_path}"
            )
        
        self.logger.info(f"Loading pipeline config from: {config_path}")
        
        # Load all YAML documents
        try:
            with open(config_path, 'r') as f:
                documents = list(yaml.safe_load_all(f))
        except yaml.YAMLError as e:
            self.logger.error(f"Invalid YAML in config file: {e}")
            raise
        
        # Parse documents
        project_defaults = {}
        pipeline_configs = {}
        
        for doc in documents:
            # Skip None/empty documents
            if doc is None:
                continue
            
            if not isinstance(doc, dict):
                self.logger.warning(f"Ignoring non-dict document: {doc}")
                continue
            
            # Check if it's project defaults
            if "project_defaults" in doc:
                project_defaults = doc["project_defaults"]
                self.logger.debug(f"Loaded project defaults: {list(project_defaults.keys())}")
                # Validate project defaults
                self._validate_config(project_defaults)
            
            # Check if it's a pipeline-specific config
            elif "pipeline" in doc:
                pipeline_name = doc["pipeline"]
                # Extract all keys except 'pipeline'
                pipeline_config = {k: v for k, v in doc.items() if k != "pipeline"}
                pipeline_configs[pipeline_name] = pipeline_config
                self.logger.debug(f"Loaded config for pipeline '{pipeline_name}': {list(pipeline_config.keys())}")
                # Validate pipeline config
                self._validate_config(pipeline_config)
            
            else:
                self.logger.warning(f"Document has neither 'project_defaults' nor 'pipeline' key, ignoring")
        
        return project_defaults, pipeline_configs
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Deep merge override into base.
        
        - Nested dicts are merged recursively
        - Lists are REPLACED (not appended)
        - Other values are replaced
        
        Args:
            base: Base dictionary
            override: Override dictionary
            
        Returns:
            Merged dictionary (new dict, doesn't mutate inputs)
        """
        result = deepcopy(base)
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dicts
                result[key] = self._deep_merge(result[key], value)
            else:
                # Replace value (including lists - they don't merge)
                result[key] = deepcopy(value)
        
        return result
    
    def _validate_config(self, config: Dict) -> None:
        """
        Validate configuration values.
        
        Validates:
        - edition: Must be in ALLOWED_EDITIONS
        - channel: Must be in ALLOWED_CHANNELS
        
        Does NOT validate:
        - Complex structures (clusters, notifications, etc.)
        - Unknown keys (allows forward compatibility)
        
        Raises:
            ValueError: If validation fails with helpful message
        """
        # Validate edition
        if 'edition' in config:
            if config['edition'] not in self.ALLOWED_EDITIONS:
                raise ValueError(
                    f"Invalid edition '{config['edition']}'. "
                    f"Allowed values: {', '.join(sorted(self.ALLOWED_EDITIONS))}"
                )
        
        # Validate channel
        if 'channel' in config:
            if config['channel'] not in self.ALLOWED_CHANNELS:
                raise ValueError(
                    f"Invalid channel '{config['channel']}'. "
                    f"Allowed values: {', '.join(sorted(self.ALLOWED_CHANNELS))}"
                )
        
        # Note: We intentionally do NOT validate:
        # - cluster structures
        # - notification formats
        # - tag values
        # - other complex nested structures
        # This provides flexibility and forward compatibility

