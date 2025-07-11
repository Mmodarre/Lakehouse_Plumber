# Plugin System Research for Lakehouse Plumber

## Executive Summary

This document provides a comprehensive analysis and implementation plan for adding a robust plugin/extension system to lakehouse-plumber. The plugin system will enable users to extend the tool's capabilities by adding new load types, transform types, write types, and event hooks without modifying the core codebase.

## Current Architecture Analysis

### Core Components

1. **Action Registry Pattern**
   - Central registry (`ActionRegistry`) manages action generators
   - Type-safe enum-based action types (`LoadSourceType`, `TransformType`, `WriteTargetType`)
   - Factory pattern for generator instantiation

2. **Generator Architecture**
   - Base class (`BaseActionGenerator`) provides interface
   - Concrete generators implement `generate()` method
   - Template-based code generation using Jinja2

3. **Configuration System**
   - Pydantic models for type safety
   - YAML-based configuration files
   - Environment-based substitutions

4. **Validation Pipeline**
   - Multi-stage validation (config, actions, dependencies)
   - Comprehensive error messaging

## Plugin System Design Options

### Option 1: Entry Point Based Plugin System (Recommended)

**Description**: Use Python's `setuptools` entry points for plugin discovery and registration.

**Advantages**:
- Industry standard approach (used by Flask, Pytest, etc.)
- Automatic discovery of installed plugins
- Clean separation between core and plugins
- Easy distribution via PyPI

**Implementation**:
```python
# In plugin's setup.py
setup(
    name='lhp-s3-plugin',
    entry_points={
        'lhp.load_generators': [
            's3_batch = lhp_s3_plugin.generators:S3BatchLoadGenerator',
        ],
        'lhp.event_hooks': [
            'on_pipeline_start = lhp_s3_plugin.hooks:on_pipeline_start',
        ],
    }
)
```

### Option 2: File-Based Plugin System

**Description**: Load plugins from a designated directory (e.g., `~/.lhp/plugins/`).

**Advantages**:
- Simple for development
- No installation required
- Easy to prototype

**Disadvantages**:
- Security concerns
- Dependency management challenges
- Distribution difficulties

### Option 3: Registry-Based Plugin System

**Description**: Explicit registration API that plugins call during initialization.

**Advantages**:
- Explicit control
- Type-safe registration
- Easy to debug

**Disadvantages**:
- Requires import-time side effects
- Less discoverable

## Recommended Architecture

### 1. Plugin Interface Contracts

```python
# src/lhp/plugin/interfaces.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

class PluginMetadata(BaseModel):
    """Metadata for a plugin."""
    name: str
    version: str
    description: str
    author: Optional[str] = None
    requires_lhp_version: Optional[str] = None
    dependencies: List[str] = []

class IPlugin(ABC):
    """Base interface for all plugins."""
    
    @abstractmethod
    def get_metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        pass
    
    @abstractmethod
    def validate_environment(self) -> bool:
        """Validate that the plugin can run in current environment."""
        pass

class ILoadGeneratorPlugin(IPlugin):
    """Interface for load generator plugins."""
    
    @abstractmethod
    def get_source_type(self) -> str:
        """Return the source type this generator handles."""
        pass
    
    @abstractmethod
    def get_generator_class(self) -> type:
        """Return the generator class."""
        pass

class ITransformGeneratorPlugin(IPlugin):
    """Interface for transform generator plugins."""
    
    @abstractmethod
    def get_transform_type(self) -> str:
        """Return the transform type this generator handles."""
        pass
    
    @abstractmethod
    def get_generator_class(self) -> type:
        """Return the generator class."""
        pass

class IEventHook(ABC):
    """Interface for event hooks."""
    
    @abstractmethod
    def get_events(self) -> List[str]:
        """Return list of events this hook handles."""
        pass
    
    @abstractmethod
    async def handle_event(self, event_name: str, context: Dict[str, Any]) -> None:
        """Handle the event."""
        pass
```

### 2. Plugin Manager

```python
# src/lhp/plugin/manager.py
import importlib.metadata
from typing import Dict, List, Any, Optional
import logging

class PluginManager:
    """Manages plugin discovery, loading, and lifecycle."""
    
    def __init__(self):
        self._plugins: Dict[str, IPlugin] = {}
        self._load_generators: Dict[str, type] = {}
        self._transform_generators: Dict[str, type] = {}
        self._write_generators: Dict[str, type] = {}
        self._event_hooks: Dict[str, List[IEventHook]] = {}
        self._logger = logging.getLogger(__name__)
    
    def discover_plugins(self) -> None:
        """Discover and load all installed plugins."""
        # Discover via entry points
        self._discover_entry_point_plugins()
        
        # Discover via plugin directory
        self._discover_directory_plugins()
    
    def _discover_entry_point_plugins(self) -> None:
        """Discover plugins via setuptools entry points."""
        entry_points = {
            'lhp.load_generators': self._register_load_generator,
            'lhp.transform_generators': self._register_transform_generator,
            'lhp.write_generators': self._register_write_generator,
            'lhp.event_hooks': self._register_event_hook,
        }
        
        for group, handler in entry_points.items():
            eps = importlib.metadata.entry_points(group=group)
            for ep in eps:
                try:
                    plugin = ep.load()
                    handler(plugin)
                except Exception as e:
                    self._logger.error(f"Failed to load plugin {ep.name}: {e}")
    
    def _register_load_generator(self, plugin_class: type) -> None:
        """Register a load generator plugin."""
        plugin = plugin_class()
        if not isinstance(plugin, ILoadGeneratorPlugin):
            raise TypeError(f"{plugin_class} must implement ILoadGeneratorPlugin")
        
        source_type = plugin.get_source_type()
        if source_type in self._load_generators:
            self._logger.warning(f"Overriding existing load generator for {source_type}")
        
        self._load_generators[source_type] = plugin.get_generator_class()
        self._plugins[f"load.{source_type}"] = plugin
    
    def get_load_generators(self) -> Dict[str, type]:
        """Get all registered load generators."""
        return self._load_generators.copy()
    
    def emit_event(self, event_name: str, context: Dict[str, Any]) -> None:
        """Emit an event to all registered hooks."""
        hooks = self._event_hooks.get(event_name, [])
        for hook in hooks:
            try:
                hook.handle_event(event_name, context)
            except Exception as e:
                self._logger.error(f"Event hook failed for {event_name}: {e}")
```

### 3. Modified Action Registry

```python
# src/lhp/core/action_registry.py (modified)
class ActionRegistry:
    """Registry for action generators with plugin support."""
    
    def __init__(self, plugin_manager: Optional[PluginManager] = None):
        self._plugin_manager = plugin_manager or PluginManager()
        self._load_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._transform_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._write_generators: Dict[str, Type[BaseActionGenerator]] = {}
        
        # Initialize with built-in generators
        self._initialize_generators()
        
        # Discover and register plugins
        self._plugin_manager.discover_plugins()
        self._register_plugin_generators()
    
    def _register_plugin_generators(self):
        """Register generators from plugins."""
        # Merge plugin generators with built-in ones
        plugin_load_generators = self._plugin_manager.get_load_generators()
        for source_type, generator_class in plugin_load_generators.items():
            self._load_generators[source_type] = generator_class
        
        # Similar for transform and write generators
```

### 4. Event Hook System

```python
# src/lhp/plugin/events.py
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass

class EventType(str, Enum):
    """Standard event types."""
    PIPELINE_START = "pipeline.start"
    PIPELINE_END = "pipeline.end"
    PIPELINE_ERROR = "pipeline.error"
    
    FLOWGROUP_START = "flowgroup.start"
    FLOWGROUP_END = "flowgroup.end"
    FLOWGROUP_ERROR = "flowgroup.error"
    
    ACTION_START = "action.start"
    ACTION_END = "action.end"
    ACTION_ERROR = "action.error"
    
    VALIDATION_START = "validation.start"
    VALIDATION_END = "validation.end"
    VALIDATION_ERROR = "validation.error"
    
    GENERATION_START = "generation.start"
    GENERATION_END = "generation.end"
    
    TABLE_CREATED = "table.created"
    TABLE_UPDATED = "table.updated"

@dataclass
class Event:
    """Event data structure."""
    type: EventType
    timestamp: float
    pipeline: Optional[str] = None
    flowgroup: Optional[str] = None
    action: Optional[str] = None
    metadata: Dict[str, Any] = None
    error: Optional[Exception] = None

class EventEmitter:
    """Emit events during pipeline execution."""
    
    def __init__(self, plugin_manager: PluginManager):
        self._plugin_manager = plugin_manager
    
    def emit(self, event: Event) -> None:
        """Emit an event."""
        context = {
            'event': event,
            'type': event.type.value,
            'timestamp': event.timestamp,
            'pipeline': event.pipeline,
            'flowgroup': event.flowgroup,
            'action': event.action,
            'metadata': event.metadata or {},
            'error': event.error
        }
        
        self._plugin_manager.emit_event(event.type.value, context)
```

### 5. Example Plugin Implementation

```python
# example_plugin/lhp_s3_plugin/generators.py
from lhp.core.base_generator import BaseActionGenerator
from lhp.models.config import Action
from typing import Dict, Any

class S3BatchLoadGenerator(BaseActionGenerator):
    """Load data from S3 in batch mode."""
    
    def __init__(self):
        super().__init__()
        self.add_import("import boto3")
        self.add_import("from pyspark.sql import SparkSession")
    
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate S3 batch load code."""
        source_config = action.source if isinstance(action.source, dict) else {}
        
        bucket = source_config.get('bucket')
        prefix = source_config.get('prefix', '')
        format = source_config.get('format', 'parquet')
        
        template_context = {
            'target': action.target,
            'bucket': bucket,
            'prefix': prefix,
            'format': format,
            'reader_options': source_config.get('reader_options', {}),
            'description': action.description or f"S3 batch load from s3://{bucket}/{prefix}"
        }
        
        return self.render_template('load/s3_batch.py.j2', template_context)

# example_plugin/lhp_s3_plugin/plugin.py
from lhp.plugin.interfaces import ILoadGeneratorPlugin, PluginMetadata
from .generators import S3BatchLoadGenerator

class S3Plugin(ILoadGeneratorPlugin):
    """S3 batch load plugin for lakehouse-plumber."""
    
    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="lhp-s3-plugin",
            version="1.0.0",
            description="S3 batch loading support for lakehouse-plumber",
            author="Example Author",
            requires_lhp_version=">=0.2.6",
            dependencies=["boto3>=1.26.0"]
        )
    
    def validate_environment(self) -> bool:
        """Check if boto3 is available."""
        try:
            import boto3
            return True
        except ImportError:
            return False
    
    def get_source_type(self) -> str:
        return "s3_batch"
    
    def get_generator_class(self) -> type:
        return S3BatchLoadGenerator

# example_plugin/setup.py
from setuptools import setup, find_packages

setup(
    name='lhp-s3-plugin',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'lakehouse-plumber>=0.2.6',
        'boto3>=1.26.0',
    ],
    entry_points={
        'lhp.load_generators': [
            's3_batch = lhp_s3_plugin.plugin:S3Plugin',
        ],
    },
    package_data={
        'lhp_s3_plugin': ['templates/*.j2'],
    },
)
```

## Implementation Plan

### Phase 1: Core Plugin Infrastructure (Week 1-2)

1. **Create Plugin Interfaces**
   - [ ] Define base interfaces (`IPlugin`, `ILoadGeneratorPlugin`, etc.)
   - [ ] Create plugin metadata structures
   - [ ] Define validation interfaces

2. **Implement Plugin Manager**
   - [ ] Entry point discovery mechanism
   - [ ] Plugin loading and validation
   - [ ] Error handling and logging

3. **Update Action Registry**
   - [ ] Integrate plugin manager
   - [ ] Merge plugin generators with built-in ones
   - [ ] Update error messages for plugin-related issues

### Phase 2: Event Hook System (Week 3)

1. **Define Event Types**
   - [ ] Create comprehensive event enum
   - [ ] Design event data structures
   - [ ] Document event lifecycle

2. **Implement Event Emitter**
   - [ ] Create event emission points in orchestrator
   - [ ] Add context collection for events
   - [ ] Implement async event handling

3. **Create Built-in Hooks**
   - [ ] Logging hook
   - [ ] Metrics collection hook
   - [ ] Notification hook template

### Phase 3: Plugin Development Tools (Week 4)

1. **Plugin Template/Scaffold**
   - [ ] Create cookiecutter template
   - [ ] Include example generators
   - [ ] Add testing setup

2. **Plugin CLI Commands**
   - [ ] `lhp plugin list` - List installed plugins
   - [ ] `lhp plugin info <name>` - Show plugin details
   - [ ] `lhp plugin validate <path>` - Validate plugin

3. **Plugin Testing Framework**
   - [ ] Test utilities for plugin developers
   - [ ] Mock contexts and fixtures
   - [ ] Integration test helpers

### Phase 4: Documentation and Examples (Week 5)

1. **Plugin Developer Guide**
   - [ ] Architecture overview
   - [ ] Step-by-step tutorial
   - [ ] Best practices

2. **Example Plugins**
   - [ ] S3 batch loader
   - [ ] Kafka source
   - [ ] Custom transformation
   - [ ] Slack notification hook

3. **API Reference**
   - [ ] Interface documentation
   - [ ] Event reference
   - [ ] Code examples

## Architecture Changes Required

### 1. Configuration Schema Updates

```yaml
# lhp.yaml additions
plugins:
  enabled: true
  directories:
    - ~/.lhp/plugins
    - ./plugins
  
  # Plugin-specific configuration
  config:
    s3_batch:
      default_region: us-east-1
      retry_attempts: 3
    
    slack_notifications:
      webhook_url: ${SLACK_WEBHOOK_URL}
      channel: "#data-pipeline-alerts"
```

### 2. Directory Structure

```
lakehouse-plumber/
├── src/lhp/
│   ├── plugin/              # New plugin system
│   │   ├── __init__.py
│   │   ├── interfaces.py    # Plugin interfaces
│   │   ├── manager.py       # Plugin manager
│   │   ├── events.py        # Event system
│   │   └── validators.py    # Plugin validators
│   ├── core/
│   │   ├── action_registry.py  # Modified to support plugins
│   │   └── orchestrator.py     # Modified to emit events
│   └── cli/
│       └── commands/
│           └── plugin.py    # New plugin commands
```

### 3. Backward Compatibility

- Existing configurations continue to work unchanged
- Built-in generators remain available
- Plugin system is opt-in via configuration

## Security Considerations

1. **Plugin Validation**
   - Verify plugin signatures (future enhancement)
   - Validate plugin metadata
   - Check version compatibility

2. **Sandboxing**
   - Run plugins with restricted permissions
   - Limit file system access
   - Monitor resource usage

3. **Configuration Security**
   - Validate plugin configurations
   - Sanitize user inputs
   - Secure credential handling

## Performance Considerations

1. **Lazy Loading**
   - Load plugins only when needed
   - Cache plugin metadata
   - Optimize discovery process

2. **Event Performance**
   - Asynchronous event handling
   - Event batching for high-frequency events
   - Configurable event filtering

## Testing Strategy

1. **Unit Tests**
   - Plugin interface compliance
   - Plugin manager functionality
   - Event system correctness

2. **Integration Tests**
   - Plugin discovery and loading
   - End-to-end pipeline with plugins
   - Event hook execution

3. **Plugin Tests**
   - Example plugin test suite
   - Plugin validation tests
   - Performance benchmarks

## Migration Path

1. **Version 0.3.0**
   - Introduce plugin system (experimental)
   - Basic load/transform plugins
   - Simple event hooks

2. **Version 0.4.0**
   - Stable plugin API
   - Full event system
   - Plugin marketplace

3. **Version 1.0.0**
   - Plugin signing and verification
   - Advanced sandboxing
   - Enterprise features

## Conclusion

The proposed plugin system provides a robust, extensible architecture that allows lakehouse-plumber to grow beyond its current capabilities while maintaining backward compatibility and security. The entry point-based approach, combined with a comprehensive event system, enables both simple extensions and complex integrations.

The phased implementation plan ensures that each component is thoroughly tested before moving to the next phase, reducing risk and ensuring quality. With proper documentation and examples, plugin development will be accessible to the community, fostering an ecosystem of extensions.