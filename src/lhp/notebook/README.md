# LakehousePlumber Databricks Notebook Interface

This module provides a comprehensive interface for running LakehousePlumber in Databricks notebooks, where CLI tools are not available.

## Overview

The notebook interface allows you to:
- ✅ Generate DLT pipelines programmatically
- ✅ Validate pipeline configurations
- ✅ Use interactive widgets for parameter input
- ✅ Deploy projects to Databricks workspaces
- ✅ Work with project files stored in Git repositories

## Key Components

### 1. NotebookInterface
Main programmatic interface for pipeline operations.

### 2. WidgetInterface  
Interactive Databricks widgets for user-friendly parameter input.

### 3. DatabricksDeployment
Utilities for packaging and deploying projects to Databricks workspaces.

## Installation

### Option 1: Package Installation (Recommended)
```python
# Install the package with notebook support
%pip install lakehouse-plumber[notebook]
```

### Option 2: Manual Setup
1. Upload your project files to Databricks workspace
2. Install dependencies:
```python
%pip install pydantic>=2.0 jinja2>=3.0 pyyaml>=6.0 jsonschema>=4.0
```

## Quick Start

### Basic Usage
```python
# Import the interface
from lhp.notebook.interface import NotebookInterface

# Create interface (auto-detects project root)
interface = NotebookInterface()

# List available pipelines
pipelines = interface.list_pipelines()
print(f"Available pipelines: {pipelines}")

# Validate a pipeline
result = interface.validate_pipeline("my_pipeline", "dev")
if result['valid']:
    print("✅ Pipeline is valid!")

# Generate pipeline code
result = interface.generate_pipeline("my_pipeline", "dev", dry_run=True)
if result['success']:
    print(f"Would generate {len(result['files_generated'])} files")
```

### Widget Interface
```python
# Import widget interface
from lhp.notebook.widgets import quick_widget_setup

# Create interactive widgets
widget_interface = quick_widget_setup()

# After configuring widgets, generate pipeline
result = widget_interface.generate_from_widgets()
```

### Quick Functions
```python
# Quick validation
from lhp.notebook.interface import quick_validate
result = quick_validate("my_pipeline", "dev")

# Quick generation
from lhp.notebook.interface import quick_generate
result = quick_generate("my_pipeline", "dev")
```

## Project Structure

Your project should follow this structure:
```
my_project/
├── src/lhp/           # LakehousePlumber source code
├── pipelines/         # Pipeline YAML files
├── substitutions/     # Environment configurations
├── presets/          # Preset configurations
├── schemas/          # Schema definitions
└── setup.py          # Package configuration
```

## Working with Databricks Repos

### Using Git Integration
1. Set up Databricks Repos with your Git repository
2. Clone your project to `/Repos/username/project-name`
3. Initialize the interface:
```python
interface = NotebookInterface("/Repos/username/project-name")
```

### File Organization
- Keep pipeline configs in `pipelines/` directory
- Environment settings in `substitutions/` directory
- Generated code outputs to `/tmp/lhp_generated/` by default

## API Reference

### NotebookInterface

#### Constructor
```python
interface = NotebookInterface(project_root="/path/to/project")
```

#### Methods

**`list_pipelines() -> List[str]`**
- Returns list of available pipeline names

**`list_environments() -> List[str]`**
- Returns list of available environments

**`validate_pipeline(pipeline_name: str, env: str = "dev") -> Dict[str, Any]`**
- Validates pipeline configuration
- Returns validation results with errors/warnings

**`generate_pipeline(pipeline_name: str, env: str = "dev", dry_run: bool = False, output_dir: str = None) -> Dict[str, Any]`**
- Generates pipeline code
- Returns generation results

**`generate_all_pipelines(env: str = "dev", dry_run: bool = False) -> Dict[str, Any]`**
- Generates all pipelines
- Returns summary results

**`show_generated_code(filename: str) -> Optional[str]`**
- Displays generated code for a file
- Returns code content

**`get_execution_stats() -> Dict[str, Any]`**
- Returns execution statistics

### WidgetInterface

#### Constructor
```python
widget_interface = WidgetInterface(project_root="/path/to/project")
```

#### Methods

**`create_pipeline_widgets(default_pipeline: str = None, default_env: str = "dev", include_options: bool = True)`**
- Creates interactive widgets for pipeline configuration

**`generate_from_widgets() -> Dict[str, Any]`**
- Generates pipeline using widget values

**`validate_from_widgets() -> Dict[str, Any]`**
- Validates pipeline using widget values

**`remove_widgets()`**
- Removes all created widgets

### DatabricksDeployment

#### Constructor
```python
deployment = DatabricksDeployment(project_root="/path/to/project")
```

#### Methods

**`package_project(output_path: str = "/tmp/lhp_package.zip", include_tests: bool = False, include_docs: bool = False) -> str`**
- Packages project for deployment
- Returns path to package file

**`upload_to_workspace(workspace_path: str = "/Workspace/Users/shared/lhp", package_path: str = None) -> bool`**
- Uploads package to Databricks workspace
- Returns success status

**`create_setup_notebook(workspace_path: str, output_path: str = "/tmp/lhp_setup.py") -> str`**
- Creates setup notebook for Databricks
- Returns path to setup notebook

## Usage Examples

### Example 1: Validation Workflow
```python
from lhp.notebook.interface import NotebookInterface

# Create interface
interface = NotebookInterface()

# Validate all pipelines
pipelines = interface.list_pipelines()
for pipeline in pipelines:
    result = interface.validate_pipeline(pipeline, "dev")
    if not result['valid']:
        print(f"❌ {pipeline}: {result['errors']}")
    else:
        print(f"✅ {pipeline}: Valid")
```

### Example 2: Batch Generation
```python
from lhp.notebook.interface import NotebookInterface

# Generate all pipelines for production
interface = NotebookInterface()
result = interface.generate_all_pipelines("prod", dry_run=False)

if result['success']:
    print(f"✅ Generated {result['total_pipelines']} pipelines")
    print(f"📁 Output: /tmp/lhp_generated/")
else:
    print(f"❌ {result['failed']} pipelines failed")
```

### Example 3: Interactive Development
```python
from lhp.notebook.widgets import quick_widget_setup

# Create interactive interface
widget_interface = quick_widget_setup()

# Configure using widgets above, then:
result = widget_interface.generate_from_widgets()

# Show generated code
if result['success']:
    for filename in result['files_generated']:
        widget_interface.interface.show_generated_code(filename)
```

### Example 4: Deployment
```python
from lhp.notebook.deployment import quick_deploy

# Deploy project to workspace
success = quick_deploy(
    project_root="/Repos/username/my-project",
    workspace_path="/Workspace/Users/shared/lhp"
)

if success:
    print("✅ Project deployed successfully")
    print("📝 Run the setup notebook to complete installation")
```

## Best Practices

### 1. Project Organization
- Use Databricks Repos for version control
- Keep project files in shared workspace locations
- Use environment-specific substitution files

### 2. Environment Management
- Create separate substitution files for dev/staging/prod
- Use meaningful environment names
- Test configurations in development first

### 3. Generated Code Management
- Use dry_run=True to preview changes
- Copy generated code to dedicated DLT notebooks
- Don't edit generated code directly

### 4. Error Handling
- Always check return values for success/failure
- Use validation before generation
- Monitor execution statistics

### 5. Performance
- Use batch operations for multiple pipelines
- Cache interface instances when possible
- Consider memory usage for large projects

## Troubleshooting

### Common Issues

**Import Errors**
```python
# Solution: Add project path to sys.path
import sys
sys.path.insert(0, "/path/to/project/src")
```

**Project Root Detection**
```python
# Solution: Explicitly specify project root
interface = NotebookInterface("/explicit/path/to/project")
```

**Widget Issues**
```python
# Solution: Remove and recreate widgets
widget_interface.remove_widgets()
widget_interface.create_pipeline_widgets()
```

**File Permission Errors**
```python
# Solution: Use /tmp/ directory for outputs
interface.generate_pipeline("pipeline", "dev", output_dir="/tmp/output")
```

### Debug Mode
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check interface state
interface = NotebookInterface()
print(f"Project root: {interface.project_root}")
print(f"Pipelines: {interface.list_pipelines()}")
print(f"Environments: {interface.list_environments()}")
```

## Limitations

Based on Databricks notebook constraints:

1. **No CLI Access**: All operations must be done programmatically
2. **File System**: Limited to `/tmp/`, workspace, and mounted storage
3. **Dependencies**: Must be installed via `%pip install`
4. **State Management**: Variables persist across notebook cells
5. **Output Size**: Large outputs may be truncated

## Migration from CLI

### CLI Command → Notebook Equivalent

```bash
# CLI
lhp validate --env dev --pipeline my_pipeline

# Notebook
interface.validate_pipeline("my_pipeline", "dev")
```

```bash
# CLI
lhp generate --env prod --pipeline my_pipeline

# Notebook
interface.generate_pipeline("my_pipeline", "prod")
```

```bash
# CLI
lhp list-pipelines

# Notebook
interface.list_pipelines()
```

## Contributing

To contribute to the notebook interface:

1. Add new methods to `NotebookInterface`
2. Update widget options in `WidgetInterface`
3. Extend deployment utilities in `DatabricksDeployment`
4. Add examples to this README
5. Update API documentation

## Support

For issues and questions:
- Check the troubleshooting section
- Review execution statistics for performance issues
- Use debug logging for detailed error information
- Validate configurations before generation 