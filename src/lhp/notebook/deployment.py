"""Deployment utilities for Databricks notebooks.

This module provides utilities for deploying LakehousePlumber projects
to Databricks notebooks, including file upload, dependency management,
and workspace integration.
"""

import os
import sys
import zipfile
import tempfile
from pathlib import Path
from typing import List, Union
import json
import base64
from datetime import datetime

from .interface import NotebookInterface


class DatabricksDeployment:
    """Deployment utilities for Databricks notebooks."""

    def __init__(self, project_root: Union[str, Path] = None):
        """Initialize the deployment helper.

        Args:
            project_root: Root directory of the LakehousePlumber project
        """
        self.interface = NotebookInterface(project_root)
        self.project_root = self.interface.project_root

    def package_project(
        self,
        output_path: str = "/tmp/lhp_package.zip",
        include_tests: bool = False,
        include_docs: bool = False,
    ) -> str:
        """Package the project for Databricks deployment.

        Args:
            output_path: Path to output zip file
            include_tests: Whether to include test files
            include_docs: Whether to include documentation

        Returns:
            Path to the created package
        """
        output_path = Path(output_path)

        # Create temporary directory for packaging
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Copy essential files
            essential_dirs = ["src", "pipelines", "substitutions", "presets", "schemas"]
            essential_files = ["setup.py", "requirements.txt", "pyproject.toml"]

            if include_tests:
                essential_dirs.append("tests")

            if include_docs:
                essential_dirs.extend(["docs", "README.md"])

            # Copy directories
            for dir_name in essential_dirs:
                src_dir = self.project_root / dir_name
                if src_dir.exists():
                    self._copy_directory(src_dir, temp_path / dir_name)

            # Copy files
            for file_name in essential_files:
                src_file = self.project_root / file_name
                if src_file.exists():
                    self._copy_file(src_file, temp_path / file_name)

            # Create deployment info
            deployment_info = {
                "project_name": "lakehouse-plumber",
                "version": "0.1.0",
                "deployment_type": "databricks_notebook",
                "created_at": str(datetime.now()),
                "python_version": sys.version,
                "dependencies": self._get_dependencies(),
            }

            with open(temp_path / "deployment_info.json", "w") as f:
                json.dump(deployment_info, f, indent=2)

            # Create zip file
            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(temp_path):
                    for file in files:
                        file_path = Path(root) / file
                        arcname = file_path.relative_to(temp_path)
                        zipf.write(file_path, arcname)

        print(f"📦 Project packaged successfully: {output_path}")
        return str(output_path)

    def upload_to_workspace(
        self,
        workspace_path: str = "/Workspace/Users/shared/lhp",
        package_path: str = None,
    ) -> bool:
        """Upload the project to Databricks workspace.

        Args:
            workspace_path: Target path in Databricks workspace
            package_path: Path to the package zip file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Package if not provided
            if package_path is None:
                package_path = self.package_project()

            # Upload to workspace
            with open(package_path, "rb") as f:
                package_data = f.read()

            # Create base64 encoded content
            encoded_content = base64.b64encode(package_data).decode("utf-8")

            # Upload using dbutils
            dbutils.fs.put(
                f"{workspace_path}/lhp_package.zip", encoded_content, overwrite=True
            )

            print(f"✅ Package uploaded to: {workspace_path}/lhp_package.zip")

            # Create extraction script
            extract_script = f"""
# Extract LakehousePlumber package
import zipfile
import os
from pathlib import Path

# Extract package
with zipfile.ZipFile("{workspace_path}/lhp_package.zip", 'r') as zipf:
    zipf.extractall("{workspace_path}")

# Add to Python path
import sys
sys.path.insert(0, "{workspace_path}/src")

print("✅ LakehousePlumber package extracted and ready to use")
print("📝 You can now import: from lhp.notebook.interface import NotebookInterface")
"""

            # Save extraction script
            dbutils.fs.put(
                f"{workspace_path}/extract_lhp.py", extract_script, overwrite=True
            )

            print(f"✅ Extraction script created: {workspace_path}/extract_lhp.py")
            print(
                "🚀 Run the extraction script to set up LakehousePlumber in your workspace"
            )

            return True

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")
            print("💡 Use the Databricks CLI or workspace UI to upload the package")
            return False

        except Exception as e:
            print(f"❌ Error uploading to workspace: {e}")
            return False

    def create_setup_notebook(
        self,
        workspace_path: str = "/Workspace/Users/shared/lhp",
        output_path: str = "/tmp/lhp_setup.py",
    ) -> str:
        """Create a setup notebook for Databricks.

        Args:
            workspace_path: Workspace path where package is uploaded
            output_path: Path to output setup notebook

        Returns:
            Path to the created setup notebook
        """
        setup_content = f"""# Databricks notebook source
# MAGIC %md
# MAGIC # LakehousePlumber Setup
# MAGIC 
# MAGIC This notebook sets up LakehousePlumber in your Databricks workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# Install required packages
%pip install pydantic>=2.0 jinja2>=3.0 pyyaml>=6.0 jsonschema>=4.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract LakehousePlumber Package

# COMMAND ----------

# Extract LakehousePlumber package
import zipfile
import os
import sys
from pathlib import Path

# Extract package
package_path = "{workspace_path}/lhp_package.zip"
extract_path = "{workspace_path}"

if os.path.exists(package_path):
    with zipfile.ZipFile(package_path, 'r') as zipf:
        zipf.extractall(extract_path)
    
    # Add to Python path
    src_path = f"{{extract_path}}/src"
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    print("✅ LakehousePlumber package extracted and ready to use")
else:
    print("❌ Package not found. Please upload the package first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Installation

# COMMAND ----------

# Test import
try:
    from lhp.notebook.interface import NotebookInterface, quick_generate, quick_validate
    from lhp.notebook.widgets import WidgetInterface, quick_widget_setup
    
    print("✅ LakehousePlumber imported successfully!")
    
    # Create interface
    interface = NotebookInterface("{workspace_path}")
    
    # List available pipelines
    pipelines = interface.list_pipelines()
    environments = interface.list_environments()
    
    print(f"📁 Available pipelines: {{pipelines}}")
    print(f"🌍 Available environments: {{environments}}")
    
    print("🚀 Setup complete! You can now use LakehousePlumber in your notebooks.")
    
except ImportError as e:
    print(f"❌ Import failed: {{e}}")
    print("💡 Make sure the package was extracted correctly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Usage Examples

# COMMAND ----------

# Example 1: Quick validation
# result = quick_validate("my_pipeline", "dev", "{workspace_path}")

# Example 2: Quick generation
# result = quick_generate("my_pipeline", "dev", "{workspace_path}")

# Example 3: Widget interface
# widget_interface = quick_widget_setup("{workspace_path}")
# # Use the widgets above to configure and run: widget_interface.generate_from_widgets()

# Example 4: Full interface
# interface = NotebookInterface("{workspace_path}")
# result = interface.generate_pipeline("my_pipeline", "dev", dry_run=True)
# interface.show_generated_code("my_pipeline.py")

print("📝 Uncomment the examples above to try them out!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. Configure your pipeline YAML files in the `pipelines/` directory
# MAGIC 2. Set up environment substitutions in the `substitutions/` directory  
# MAGIC 3. Use the interface methods to generate your DLT pipelines
# MAGIC 4. Copy the generated code to your DLT notebooks
"""

        with open(output_path, "w") as f:
            f.write(setup_content)

        print(f"📝 Setup notebook created: {output_path}")
        print("💡 Upload this notebook to your Databricks workspace and run it")

        return output_path

    def create_usage_examples(self, output_path: str = "/tmp/lhp_examples.py") -> str:
        """Create usage examples notebook.

        Args:
            output_path: Path to output examples notebook

        Returns:
            Path to the created examples notebook
        """
        examples_content = """# Databricks notebook source
# MAGIC %md
# MAGIC # LakehousePlumber Usage Examples
# MAGIC 
# MAGIC This notebook demonstrates how to use LakehousePlumber in Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC Make sure you've run the setup notebook first.

# COMMAND ----------

# Import LakehousePlumber
import sys
sys.path.insert(0, "/Workspace/Users/shared/lhp/src")

from lhp.notebook.interface import NotebookInterface, quick_generate, quick_validate
from lhp.notebook.widgets import WidgetInterface, quick_widget_setup

# Create interface
interface = NotebookInterface("/Workspace/Users/shared/lhp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: List Available Pipelines and Environments

# COMMAND ----------

# List available pipelines
pipelines = interface.list_pipelines()
environments = interface.list_environments()

print("📁 Available pipelines:")
for pipeline in pipelines:
    print(f"  • {pipeline}")

print("\\n🌍 Available environments:")
for env in environments:
    print(f"  • {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Validate a Pipeline

# COMMAND ----------

# Validate a specific pipeline
if pipelines:
    pipeline_name = pipelines[0]  # Use first available pipeline
    result = interface.validate_pipeline(pipeline_name, "dev")
    
    if result['valid']:
        print(f"✅ Pipeline '{pipeline_name}' is valid!")
    else:
        print(f"❌ Pipeline '{pipeline_name}' has errors:")
        for error in result['errors']:
            print(f"  • {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Generate Pipeline (Dry Run)

# COMMAND ----------

# Generate pipeline code (dry run)
if pipelines:
    pipeline_name = pipelines[0]
    result = interface.generate_pipeline(pipeline_name, "dev", dry_run=True)
    
    if result['success']:
        print(f"📄 Would generate {len(result['files_generated'])} files:")
        for filename in result['files_generated']:
            print(f"  • {filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Generate Pipeline (Actual)

# COMMAND ----------

# Generate pipeline code (actual generation)
if pipelines:
    pipeline_name = pipelines[0]
    result = interface.generate_pipeline(pipeline_name, "dev", output_dir="/tmp/lhp_output")
    
    if result['success']:
        print(f"✅ Generated {len(result['files_generated'])} files")
        print(f"📁 Output directory: {result['output_directory']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Show Generated Code

# COMMAND ----------

# Show generated code
if pipelines:
    pipeline_name = pipelines[0]
    filename = f"{pipeline_name}.py"
    
    code = interface.show_generated_code(filename)
    
    if code:
        print("📄 Generated code is displayed above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Widget Interface

# COMMAND ----------

# Create widget interface
widget_interface = quick_widget_setup("/Workspace/Users/shared/lhp")

# COMMAND ----------

# Generate from widgets
# Run this after configuring the widgets above
result = widget_interface.generate_from_widgets()

if result['success']:
    print("✅ Pipeline generated successfully using widgets!")
else:
    print(f"❌ Generation failed: {result.get('error', 'Unknown error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Generate All Pipelines

# COMMAND ----------

# Generate all pipelines
result = interface.generate_all_pipelines("dev", dry_run=True)

if result['success']:
    print(f"✅ All {result['total_pipelines']} pipelines validated successfully")
else:
    print(f"❌ {result['failed']} pipelines failed validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 8: Execution Statistics

# COMMAND ----------

# Get execution statistics
stats = interface.get_execution_stats()

print("📊 Execution Statistics:")
print(f"  • Pipelines generated: {stats['pipelines_generated']}")
print(f"  • Files generated: {stats['files_generated']}")
print(f"  • Total execution time: {stats['execution_time']:.2f}s")
print(f"  • Last execution: {stats['last_execution']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tips for Production Use
# MAGIC 
# MAGIC 1. **Project Structure**: Keep your project files in a shared workspace location
# MAGIC 2. **Environment Management**: Use different substitution files for dev/staging/prod
# MAGIC 3. **Version Control**: Use Databricks Repos to sync your project with Git
# MAGIC 4. **Generated Code**: Copy generated DLT code to dedicated notebooks for execution
# MAGIC 5. **Automation**: Use the interface methods in scheduled jobs for automated generation
"""

        with open(output_path, "w") as f:
            f.write(examples_content)

        print(f"📝 Examples notebook created: {output_path}")
        print("💡 Upload this notebook to your Databricks workspace for usage examples")

        return output_path

    def _copy_directory(self, src: Path, dst: Path):
        """Copy directory recursively."""
        import shutil

        shutil.copytree(src, dst, dirs_exist_ok=True)

    def _copy_file(self, src: Path, dst: Path):
        """Copy a single file."""
        import shutil

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    def _get_dependencies(self) -> List[str]:
        """Get list of dependencies from setup.py."""
        setup_file = self.project_root / "setup.py"
        dependencies = []

        if setup_file.exists():
            try:
                with open(setup_file, "r") as f:
                    content = f.read()

                # Simple extraction of install_requires
                if "install_requires=" in content:
                    start = content.find("install_requires=")
                    end = content.find("]", start)
                    if end != -1:
                        # This is a simplified extraction
                        dependencies = [
                            "pydantic>=2.0",
                            "jinja2>=3.0",
                            "pyyaml>=6.0",
                            "jsonschema>=4.0",
                        ]
            except Exception as e:
                print(f"Warning: Could not parse dependencies from setup.py: {e}")

        return dependencies


# Convenience functions
def quick_deploy(
    project_root: Union[str, Path] = None,
    workspace_path: str = "/Workspace/Users/shared/lhp",
) -> bool:
    """Quick deployment to Databricks workspace.

    Args:
        project_root: Root directory of the LakehousePlumber project
        workspace_path: Target path in Databricks workspace

    Returns:
        True if successful, False otherwise
    """
    deployment = DatabricksDeployment(project_root)

    # Package project
    package_path = deployment.package_project()

    # Upload to workspace
    success = deployment.upload_to_workspace(workspace_path, package_path)

    if success:
        # Create setup notebook
        setup_notebook = deployment.create_setup_notebook(workspace_path)
        examples_notebook = deployment.create_usage_examples()

        print("\\n🚀 Deployment Summary:")
        print(f"  • Package: {package_path}")
        print(f"  • Workspace: {workspace_path}")
        print(f"  • Setup notebook: {setup_notebook}")
        print(f"  • Examples notebook: {examples_notebook}")
        print("\\n📝 Next steps:")
        print("  1. Upload the setup notebook to your Databricks workspace")
        print("  2. Run the setup notebook to extract and configure LakehousePlumber")
        print("  3. Use the examples notebook to learn how to use the interface")

    return success
