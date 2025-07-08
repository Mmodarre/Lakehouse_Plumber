# Plugin Architecture Research Report for Lakehouse Plumber

## Executive Summary

Lakehouse Plumber is a Python CLI tool that generates Delta Live Tables (DLT) pipeline code from YAML configurations. The application already has several extensibility mechanisms in place and is well-positioned to support a comprehensive plugin architecture. This report analyzes the current architecture and provides detailed recommendations for implementing plugin capabilities.

## Current Architecture Analysis

### Application Overview
- **Type**: Python CLI tool using Click framework
- **Purpose**: Generates DLT pipeline code from YAML configurations
- **Architecture**: Action-based with Load, Transform, and Write operations
- **Key Components**: 
  - Action Registry (`ActionRegistry` class)
  - Base Generator pattern (`BaseActionGenerator`)
  - Template engine (Jinja2-based)
  - YAML configuration parsing
  - State management for smart generation

### Existing Extension Points

The application already implements several plugin-like mechanisms:

1. **Action Registry System** (`src/lhp/core/action_registry.py`)
   - Maps action types to generator classes
   - Supports Load, Transform, and Write generators
   - Uses enum-based type validation
   - Already follows a factory pattern

2. **Generator Pattern** (`src/lhp/core/base_generator.py`)
   - Abstract base class for all action generators
   - Standard interface with `generate()` method
   - Template rendering capabilities
   - Import management

3. **Template System** (`src/lhp/templates/`)
   - Jinja2-based code generation
   - Reusable pipeline templates
   - Environment-specific substitutions

4. **Configuration System**
   - YAML-based configuration
   - Preset system for reusable configurations
   - Environment-specific substitutions

## Plugin Architecture Options

### Option 1: setuptools Entry Points (Recommended)

**Implementation Approach:**
- Use setuptools entry points for plugin discovery
- Define specific entry point groups for different plugin types
- Leverage Python's standard plugin ecosystem

**Entry Point Groups:**
```python
# Entry point groups to define
'lhp.generators.load'     # Custom load generators
'lhp.generators.transform' # Custom transform generators  
'lhp.generators.write'    # Custom write generators
'lhp.cli.commands'        # Custom CLI commands
'lhp.validators'          # Custom validators
'lhp.templates.filters'   # Custom Jinja2 filters
'lhp.formatters'          # Custom output formatters
```

**Benefits:**
- Standard Python plugin mechanism
- Automatic discovery without configuration
- Package manager integration (pip install)
- Wide ecosystem support
- Clean separation of concerns

**Implementation Steps:**

1. **Create Plugin Discovery System:**
```python
# src/lhp/core/plugin_manager.py
from importlib.metadata import entry_points
from typing import Dict, Type, List
import logging

class PluginManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._loaded_plugins = {}
    
    def discover_generators(self) -> Dict[str, Type[BaseActionGenerator]]:
        """Discover and load generator plugins."""
        generators = {}
        
        for group in ['lhp.generators.load', 'lhp.generators.transform', 'lhp.generators.write']:
            eps = entry_points(group=group)
            for ep in eps:
                try:
                    generator_class = ep.load()
                    generators[f"{group}.{ep.name}"] = generator_class
                    self.logger.info(f"Loaded generator plugin: {ep.name} from {group}")
                except Exception as e:
                    self.logger.warning(f"Failed to load plugin {ep.name}: {e}")
        
        return generators
    
    def discover_cli_commands(self) -> List:
        """Discover CLI command plugins."""
        commands = []
        eps = entry_points(group='lhp.cli.commands')
        for ep in eps:
            try:
                command = ep.load()
                commands.append(command)
                self.logger.info(f"Loaded CLI plugin: {ep.name}")
            except Exception as e:
                self.logger.warning(f"Failed to load CLI plugin {ep.name}: {e}")
        
        return commands
```

2. **Extend Action Registry:**
```python
# Modify src/lhp/core/action_registry.py
class ActionRegistry:
    def __init__(self):
        # ... existing code ...
        self.plugin_manager = PluginManager()
        self._initialize_generators()
        self._load_plugin_generators()
    
    def _load_plugin_generators(self):
        """Load generators from plugins."""
        plugin_generators = self.plugin_manager.discover_generators()
        
        for plugin_key, generator_class in plugin_generators.items():
            group, name = plugin_key.split('.', 2)[-2:]  # Extract group and name
            
            if 'load' in group:
                # Register as custom load type
                self._register_custom_load_generator(name, generator_class)
            elif 'transform' in group:
                # Register as custom transform type
                self._register_custom_transform_generator(name, generator_class)
            elif 'write' in group:
                # Register as custom write type
                self._register_custom_write_generator(name, generator_class)
```

3. **Plugin CLI Integration:**
```python
# Modify src/lhp/cli/main.py
from ..core.plugin_manager import PluginManager

@cli.group()
def cli():
    # ... existing code ...
    # Load CLI plugins
    plugin_manager = PluginManager()
    plugin_commands = plugin_manager.discover_cli_commands()
    
    for command in plugin_commands:
        cli.add_command(command)
```

**Example Plugin Package Structure:**
```
lhp-plugin-example/
├── setup.py
├── src/
│   └── lhp_plugin_example/
│       ├── __init__.py
│       ├── generators/
│       │   ├── __init__.py
│       │   └── custom_load.py
│       └── commands/
│           ├── __init__.py
│           └── custom_command.py
```

**Example Plugin setup.py:**
```python
from setuptools import setup, find_packages

setup(
    name="lhp-plugin-example",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        'lhp.generators.load': [
            'custom_api = lhp_plugin_example.generators.custom_load:CustomAPILoadGenerator',
        ],
        'lhp.cli.commands': [
            'custom-deploy = lhp_plugin_example.commands.deploy:deploy_command',
        ],
    },
    install_requires=[
        'lakehouse-plumber>=0.1.0',
    ],
)
```

### Option 2: Directory-Based Plugin System

**Implementation Approach:**
- Define plugin directories (similar to Django apps)
- Auto-discover Python modules in plugin directories
- Support both project-local and global plugins

**Benefits:**
- No setuptools dependency
- Easy for local/development plugins
- Simple file-based organization

**Implementation:**
```python
# src/lhp/core/plugin_loader.py
import importlib.util
import sys
from pathlib import Path
from typing import List, Dict, Any

class DirectoryPluginLoader:
    def __init__(self, plugin_dirs: List[Path]):
        self.plugin_dirs = plugin_dirs
        self.loaded_plugins = {}
    
    def discover_plugins(self) -> Dict[str, Any]:
        """Discover plugins in configured directories."""
        plugins = {}
        
        for plugin_dir in self.plugin_dirs:
            if not plugin_dir.exists():
                continue
                
            for plugin_path in plugin_dir.glob("*.py"):
                if plugin_path.name.startswith("_"):
                    continue
                    
                plugin_name = plugin_path.stem
                try:
                    spec = importlib.util.spec_from_file_location(
                        f"lhp_plugin_{plugin_name}", plugin_path
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    plugins[plugin_name] = module
                except Exception as e:
                    logging.warning(f"Failed to load plugin {plugin_name}: {e}")
        
        return plugins
```

### Option 3: Configuration-Based Plugin System

**Implementation Approach:**
- Define plugins in configuration files (YAML/JSON)
- Support both local imports and remote plugin loading
- Integration with existing substitution system

**Example Configuration:**
```yaml
# lhp.yaml
plugins:
  enabled:
    - name: "snowflake_connector"
      type: "load_generator"
      module: "lhp_plugins.snowflake"
      class: "SnowflakeLoadGenerator"
    - name: "custom_validator"
      type: "validator"
      module: "local_plugins.validators"
      class: "BusinessRuleValidator"
  directories:
    - "./plugins"
    - "~/.lhp/plugins"
```

### Option 4: Hybrid Approach (Recommended for Maximum Flexibility)

**Implementation Strategy:**
Combine multiple approaches to support different use cases:

1. **setuptools Entry Points** - For distributable, production plugins
2. **Directory-Based** - For development and project-specific plugins
3. **Configuration-Based** - For runtime plugin management and complex setups

## Specific Plugin Types to Support

### 1. Generator Plugins

**Purpose:** Add support for new data sources, transforms, or destinations

**Interface:**
```python
# Plugin interface for custom generators
class CustomLoadGenerator(BaseActionGenerator):
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        # Custom implementation
        return generated_code
    
    @classmethod
    def get_supported_options(cls) -> Dict[str, Any]:
        """Return schema for supported configuration options."""
        return {
            "api_endpoint": {"type": "string", "required": True},
            "auth_method": {"type": "string", "default": "bearer"}
        }
```

**Examples:**
- Snowflake connector
- Kafka streaming source
- MongoDB loader
- Custom REST API source
- Machine learning model deployment targets

### 2. CLI Command Plugins

**Purpose:** Add new CLI commands for extended functionality

**Interface:**
```python
import click
from lhp.cli.base import BaseCLIPlugin

@click.command()
@click.option('--environment', '-e', required=True)
@click.option('--dry-run', is_flag=True)
def deploy(environment, dry_run):
    """Deploy pipelines to Databricks workspace."""
    # Custom deployment logic
    pass

# Register command
cli_plugin = BaseCLIPlugin(command=deploy, name="deploy")
```

**Examples:**
- Deployment commands
- Pipeline testing commands
- Schema validation commands
- Performance profiling commands

### 3. Validator Plugins

**Purpose:** Add custom validation rules for configurations

**Interface:**
```python
class CustomValidator:
    def validate(self, config: Dict[str, Any]) -> List[str]:
        """Return list of validation errors."""
        errors = []
        # Custom validation logic
        return errors
```

### 4. Template Filter Plugins

**Purpose:** Add custom Jinja2 filters for template processing

**Interface:**
```python
def custom_filter(value, *args, **kwargs):
    """Custom Jinja2 filter implementation."""
    return processed_value

# Registration
template_filters = {
    'encrypt': encrypt_filter,
    'format_sql': sql_format_filter,
    'generate_uuid': uuid_filter
}
```

## Implementation Roadmap

### Phase 1: Foundation (Recommended First Steps)

1. **Create Plugin Manager Infrastructure**
   - Implement `PluginManager` class
   - Add entry point discovery
   - Create plugin registry

2. **Extend Action Registry**
   - Add plugin generator support
   - Implement dynamic type registration
   - Add plugin metadata tracking

3. **Update CLI System**
   - Add plugin command discovery
   - Implement plugin listing commands
   - Add plugin status/info commands

### Phase 2: Core Plugin Types

1. **Generator Plugins**
   - Define generator plugin interface
   - Implement plugin loading in registry
   - Add validation for plugin generators

2. **CLI Command Plugins**
   - Create CLI plugin base class
   - Implement command registration
   - Add plugin command help integration

### Phase 3: Advanced Features

1. **Validator Plugins**
   - Create validator plugin interface
   - Integrate with existing validation system
   - Add plugin-specific validation rules

2. **Template System Extensions**
   - Add filter plugin support
   - Implement function plugin system
   - Create template macro plugins

### Phase 4: Developer Experience

1. **Plugin Development Tools**
   - Create plugin scaffold generator
   - Add plugin testing framework
   - Implement plugin debugging tools

2. **Documentation and Examples**
   - Create plugin development guide
   - Build example plugin packages
   - Add plugin API documentation

## Security Considerations

### Code Execution Security
- **Sandboxing:** Consider using `RestrictedPython` for user-provided code
- **Validation:** Implement strict input validation for plugin configurations
- **Permissions:** Define clear permission models for plugin capabilities

### Plugin Trust Model
```python
class PluginTrustManager:
    def __init__(self):
        self.trusted_sources = set()
        self.blocked_plugins = set()
    
    def is_plugin_trusted(self, plugin_name: str, source: str) -> bool:
        """Check if plugin is from trusted source."""
        if plugin_name in self.blocked_plugins:
            return False
        return source in self.trusted_sources
```

### Configuration Security
- **Secret Handling:** Ensure plugins can't access sensitive configuration data
- **Isolation:** Isolate plugin execution environments
- **Auditing:** Log all plugin loading and execution activities

## Performance Considerations

### Lazy Loading
```python
class LazyPluginLoader:
    def __init__(self):
        self._plugin_cache = {}
        self._discovered_plugins = None
    
    def get_plugin(self, name: str):
        """Load plugin only when needed."""
        if name not in self._plugin_cache:
            self._plugin_cache[name] = self._load_plugin(name)
        return self._plugin_cache[name]
```

### Plugin Caching
- Cache plugin discovery results
- Implement plugin metadata caching
- Add plugin compilation caching for template filters

## Testing Strategy

### Plugin Testing Framework
```python
# Test utilities for plugin developers
class PluginTestCase:
    def setUp(self):
        self.plugin_manager = PluginManager()
        self.test_config = self.load_test_config()
    
    def assert_plugin_generates(self, plugin_name: str, expected_output: str):
        """Assert that plugin generates expected output."""
        plugin = self.plugin_manager.get_plugin(plugin_name)
        result = plugin.generate(self.test_config)
        self.assertEqual(result, expected_output)
```

### Integration Testing
- Test plugin loading mechanisms
- Validate plugin configuration schemas
- Test plugin interaction with core system

## Migration Strategy

### Backward Compatibility
- Maintain existing generator system alongside plugin system
- Gradual migration of built-in generators to plugin architecture
- Deprecation warnings for old-style extensions

### User Migration
1. **Phase 1:** Release plugin system as opt-in feature
2. **Phase 2:** Migrate built-in generators to plugins
3. **Phase 3:** Make plugin system the primary extension mechanism

## Conclusion and Recommendations

**Recommended Approach:** Implement **Option 1 (setuptools Entry Points)** as the primary plugin mechanism, with **Option 2 (Directory-Based)** as a supplementary system for development and local plugins.

**Key Benefits:**
1. **Standards-Based:** Uses Python ecosystem standards
2. **Scalable:** Supports both simple and complex plugin scenarios
3. **Developer-Friendly:** Familiar patterns for Python developers
4. **Flexible:** Multiple plugin types support different use cases

**Next Steps:**
1. Start with Phase 1 implementation (Plugin Manager infrastructure)
2. Create proof-of-concept generator plugin
3. Test plugin system with simple use case
4. Gradually expand plugin types and capabilities

The Lakehouse Plumber application is already well-architected for plugin support. The existing Action Registry and Generator patterns provide an excellent foundation for a comprehensive plugin system that will enable users to extend the application for their specific data engineering needs.