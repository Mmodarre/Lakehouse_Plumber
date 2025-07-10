"""Widget-based interface for interactive use in Databricks notebooks.

This module provides Databricks widgets integration for LakehousePlumber,
allowing users to create interactive forms for pipeline generation.
"""

from typing import Dict, Any, Union
from pathlib import Path

from .interface import NotebookInterface


class WidgetInterface:
    """Widget-based interface for interactive notebook usage."""

    def __init__(self, project_root: Union[str, Path] = None):
        """Initialize the widget interface.

        Args:
            project_root: Root directory of the LakehousePlumber project
        """
        self.interface = NotebookInterface(project_root)
        self.widgets_created = False

    def create_pipeline_widgets(
        self,
        default_pipeline: str = None,
        default_env: str = "dev",
        include_options: bool = True,
    ):
        """Create Databricks widgets for pipeline generation.

        Args:
            default_pipeline: Default pipeline name
            default_env: Default environment
            include_options: Whether to include advanced options
        """
        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Get available pipelines and environments
            pipelines = self.interface.list_pipelines()
            environments = self.interface.list_environments()

            # Create dropdown for pipeline selection
            if pipelines:
                pipeline_choices = pipelines
                if default_pipeline and default_pipeline in pipelines:
                    default_pipeline_value = default_pipeline
                else:
                    default_pipeline_value = pipelines[0]

                dbutils.widgets.dropdown(
                    "pipeline", default_pipeline_value, pipeline_choices, "Pipeline"
                )
            else:
                dbutils.widgets.text("pipeline", "", "Pipeline (No pipelines found)")

            # Create dropdown for environment selection
            if environments:
                env_choices = environments
                if default_env and default_env in environments:
                    default_env_value = default_env
                else:
                    default_env_value = environments[0]

                dbutils.widgets.dropdown(
                    "environment", default_env_value, env_choices, "Environment"
                )
            else:
                dbutils.widgets.text("environment", "dev", "Environment")

            # Advanced options
            if include_options:
                dbutils.widgets.dropdown(
                    "dry_run", "False", ["True", "False"], "Dry Run"
                )

                dbutils.widgets.text(
                    "output_dir", "/tmp/lhp_generated", "Output Directory"
                )

            self.widgets_created = True

            print("✅ Widgets created successfully!")
            print("📝 Use the widgets above to configure your pipeline generation")
            print("🚀 Run generate_from_widgets() to generate the pipeline")

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")
            print("💡 Use the direct interface methods instead")

        except Exception as e:
            print(f"❌ Error creating widgets: {e}")

    def generate_from_widgets(self) -> Dict[str, Any]:
        """Generate pipeline using values from widgets.

        Returns:
            Generation results
        """
        if not self.widgets_created:
            print("❌ Widgets not created yet. Run create_pipeline_widgets() first.")
            return {"success": False, "error": "Widgets not created"}

        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Get values from widgets
            pipeline = dbutils.widgets.get("pipeline")
            environment = dbutils.widgets.get("environment")
            dry_run = dbutils.widgets.get("dry_run") == "True"
            output_dir = dbutils.widgets.get("output_dir")

            if not pipeline:
                print("❌ Pipeline name is required")
                return {"success": False, "error": "Pipeline name is required"}

            # Generate pipeline
            result = self.interface.generate_pipeline(
                pipeline_name=pipeline,
                env=environment,
                dry_run=dry_run,
                output_dir=output_dir,
            )

            return result

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")
            return {"success": False, "error": "dbutils not available"}

        except Exception as e:
            print(f"❌ Error generating from widgets: {e}")
            return {"success": False, "error": str(e)}

    def validate_from_widgets(self) -> Dict[str, Any]:
        """Validate pipeline using values from widgets.

        Returns:
            Validation results
        """
        if not self.widgets_created:
            print("❌ Widgets not created yet. Run create_pipeline_widgets() first.")
            return {"valid": False, "errors": ["Widgets not created"]}

        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Get values from widgets
            pipeline = dbutils.widgets.get("pipeline")
            environment = dbutils.widgets.get("environment")

            if not pipeline:
                print("❌ Pipeline name is required")
                return {"valid": False, "errors": ["Pipeline name is required"]}

            # Validate pipeline
            result = self.interface.validate_pipeline(
                pipeline_name=pipeline, env=environment
            )

            return result

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")
            return {"valid": False, "errors": ["dbutils not available"]}

        except Exception as e:
            print(f"❌ Error validating from widgets: {e}")
            return {"valid": False, "errors": [str(e)]}

    def remove_widgets(self):
        """Remove all created widgets."""
        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Remove widgets
            dbutils.widgets.removeAll()
            self.widgets_created = False

            print("✅ All widgets removed")

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")

        except Exception as e:
            print(f"❌ Error removing widgets: {e}")

    def create_multi_pipeline_widgets(self):
        """Create widgets for multi-pipeline operations."""
        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Get available environments
            environments = self.interface.list_environments()

            # Create dropdown for environment selection
            if environments:
                env_choices = environments
                default_env_value = environments[0]

                dbutils.widgets.dropdown(
                    "environment", default_env_value, env_choices, "Environment"
                )
            else:
                dbutils.widgets.text("environment", "dev", "Environment")

            # Options for multi-pipeline generation
            dbutils.widgets.dropdown("dry_run", "False", ["True", "False"], "Dry Run")

            dbutils.widgets.text("output_dir", "/tmp/lhp_generated", "Output Directory")

            self.widgets_created = True

            print("✅ Multi-pipeline widgets created successfully!")
            print(
                "📝 Use the widgets above to configure your multi-pipeline generation"
            )
            print("🚀 Run generate_all_from_widgets() to generate all pipelines")

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")

        except Exception as e:
            print(f"❌ Error creating multi-pipeline widgets: {e}")

    def generate_all_from_widgets(self) -> Dict[str, Any]:
        """Generate all pipelines using values from widgets.

        Returns:
            Generation results
        """
        if not self.widgets_created:
            print(
                "❌ Widgets not created yet. Run create_multi_pipeline_widgets() first."
            )
            return {"success": False, "error": "Widgets not created"}

        try:
            # Import dbutils (only available in Databricks)
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)

            # Get values from widgets
            environment = dbutils.widgets.get("environment")
            dry_run = dbutils.widgets.get("dry_run") == "True"

            # Generate all pipelines
            result = self.interface.generate_all_pipelines(
                env=environment, dry_run=dry_run
            )

            return result

        except ImportError:
            print("❌ dbutils not available - not running in Databricks environment")
            return {"success": False, "error": "dbutils not available"}

        except Exception as e:
            print(f"❌ Error generating all from widgets: {e}")
            return {"success": False, "error": str(e)}


# Convenience functions for notebook use
def create_widget_interface(project_root: Union[str, Path] = None) -> WidgetInterface:
    """Create a widget interface instance.

    Args:
        project_root: Root directory of the LakehousePlumber project

    Returns:
        WidgetInterface instance
    """
    return WidgetInterface(project_root)


def quick_widget_setup(
    project_root: Union[str, Path] = None,
    default_pipeline: str = None,
    default_env: str = "dev",
):
    """Quick setup function for widget-based interface.

    Args:
        project_root: Root directory of the LakehousePlumber project
        default_pipeline: Default pipeline name
        default_env: Default environment

    Returns:
        WidgetInterface instance with widgets created
    """
    interface = create_widget_interface(project_root)
    interface.create_pipeline_widgets(default_pipeline, default_env)
    return interface
