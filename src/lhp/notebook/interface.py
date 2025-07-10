"""Databricks Notebook Interface for LakehousePlumber.

This module provides a notebook-friendly interface for running LakehousePlumber
in Databricks notebooks where CLI is not available.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from ..core.orchestrator import ActionOrchestrator
from ..parsers.yaml_parser import YAMLParser


class NotebookInterface:
    """Main interface for running LakehousePlumber in Databricks notebooks."""

    def __init__(self, project_root: Union[str, Path] = None):
        """Initialize the notebook interface.

        Args:
            project_root: Root directory of the LakehousePlumber project.
                         If None, will try to detect from current working directory.
        """
        self.logger = logging.getLogger(__name__)
        self._setup_logging()

        # Set up project root
        if project_root is None:
            self.project_root = self._detect_project_root()
        else:
            self.project_root = Path(project_root)

        if not self.project_root.exists():
            raise ValueError(f"Project root does not exist: {self.project_root}")

        self.logger.info(
            f"Initialized LakehousePlumber notebook interface with project root: {self.project_root}"
        )

        # Initialize core components
        self.orchestrator = ActionOrchestrator(self.project_root)
        self.yaml_parser = YAMLParser()

        # Track generated files and execution state
        self.generated_files: Dict[str, str] = {}
        self.execution_stats = {
            "pipelines_generated": 0,
            "files_generated": 0,
            "execution_time": 0,
            "last_execution": None,
        }

    def _setup_logging(self):
        """Set up logging for notebook environment."""
        # Configure logging to be notebook-friendly
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

    def _detect_project_root(self) -> Path:
        """Detect project root from current working directory.

        Returns:
            Path to project root
        """
        # In Databricks, we might be in a Repos folder
        current_dir = Path.cwd()

        # Look for LakehousePlumber project indicators
        indicators = ["lhp.yaml", "pipelines", "src/lhp", "setup.py"]

        # Check current directory and parent directories
        for parent in [current_dir] + list(current_dir.parents):
            if any((parent / indicator).exists() for indicator in indicators):
                return parent

        # Fallback to current directory
        self.logger.warning("Could not detect project root, using current directory")
        return current_dir

    def list_pipelines(self) -> List[str]:
        """List all available pipelines.

        Returns:
            List of pipeline names
        """
        pipelines_dir = self.project_root / "pipelines"
        if not pipelines_dir.exists():
            return []

        pipelines = [p.name for p in pipelines_dir.iterdir() if p.is_dir()]
        return sorted(pipelines)

    def list_environments(self) -> List[str]:
        """List all available environments.

        Returns:
            List of environment names
        """
        substitutions_dir = self.project_root / "substitutions"
        if not substitutions_dir.exists():
            return []

        envs = [f.stem for f in substitutions_dir.glob("*.yaml")]
        return sorted(envs)

    def validate_pipeline(self, pipeline_name: str, env: str = "dev") -> Dict[str, Any]:
        """Validate a pipeline configuration.

        Args:
            pipeline_name: Name of the pipeline to validate
            env: Environment to validate for

        Returns:
            Dictionary with validation results
        """
        start_time = datetime.now()

        try:
            errors, warnings = self.orchestrator.validate_pipeline(pipeline_name, env)

            result = {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings,
                "pipeline": pipeline_name,
                "environment": env,
                "validation_time": (datetime.now() - start_time).total_seconds(),
            }

            # Print results in notebook-friendly format
            self._print_validation_results(result)

            return result

        except Exception as e:
            error_result = {
                "valid": False,
                "errors": [str(e)],
                "warnings": [],
                "pipeline": pipeline_name,
                "environment": env,
                "validation_time": (datetime.now() - start_time).total_seconds(),
            }

            self._print_validation_results(error_result)
            return error_result

    def generate_pipeline(
        self,
        pipeline_name: str,
        env: str = "dev",
        dry_run: bool = False,
        output_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Generate pipeline code.

        Args:
            pipeline_name: Name of the pipeline to generate
            env: Environment to generate for
            dry_run: If True, only show what would be generated
            output_dir: Output directory (defaults to /tmp/lhp_generated)

        Returns:
            Dictionary with generation results
        """
        start_time = datetime.now()

        try:
            # Set default output directory for Databricks
            if output_dir is None:
                output_dir = "/tmp/lhp_generated"

            output_path = Path(output_dir)

            if not dry_run:
                # Generate files
                generated_files = self.orchestrator.generate_pipeline(
                    pipeline_name, env, output_path / pipeline_name
                )

                # Store generated files
                self.generated_files.update(generated_files)

                # Update stats
                self.execution_stats["pipelines_generated"] += 1
                self.execution_stats["files_generated"] += len(generated_files)
                self.execution_stats["last_execution"] = datetime.now()

            else:
                # Dry run - just validate and show structure
                generated_files = self.orchestrator.generate_pipeline(
                    pipeline_name, env, output_dir=None  # Don't write files
                )

            execution_time = (datetime.now() - start_time).total_seconds()
            self.execution_stats["execution_time"] += execution_time

            result = {
                "success": True,
                "pipeline": pipeline_name,
                "environment": env,
                "files_generated": list(generated_files.keys()),
                "output_directory": (
                    str(output_path / pipeline_name) if not dry_run else None
                ),
                "dry_run": dry_run,
                "execution_time": execution_time,
                "generated_code": generated_files if dry_run else None,
            }

            # Print results in notebook-friendly format
            self._print_generation_results(result)

            return result

        except Exception as e:
            error_result = {
                "success": False,
                "pipeline": pipeline_name,
                "environment": env,
                "error": str(e),
                "execution_time": (datetime.now() - start_time).total_seconds(),
            }

            self._print_generation_results(error_result)
            return error_result

    def generate_all_pipelines(
        self, env: str = "dev", dry_run: bool = False
    ) -> Dict[str, Any]:
        """Generate all pipelines.

        Args:
            env: Environment to generate for
            dry_run: If True, only show what would be generated

        Returns:
            Dictionary with generation results
        """
        pipelines = self.list_pipelines()

        if not pipelines:
            print("❌ No pipelines found")
            return {"success": False, "error": "No pipelines found"}

        results = {}
        total_start_time = datetime.now()

        print(f"🚀 Generating {len(pipelines)} pipelines for environment: {env}")

        for pipeline in pipelines:
            print(f"\n📁 Processing pipeline: {pipeline}")
            result = self.generate_pipeline(pipeline, env, dry_run)
            results[pipeline] = result

        total_time = (datetime.now() - total_start_time).total_seconds()

        # Summary
        successful = sum(1 for r in results.values() if r.get("success", False))
        failed = len(pipelines) - successful

        print("\n📊 Generation Summary:")
        print(f"   Environment: {env}")
        print(f"   Total pipelines: {len(pipelines)}")
        print(f"   Successful: {successful}")
        print(f"   Failed: {failed}")
        print(f"   Total time: {total_time:.2f}s")

        return {
            "success": failed == 0,
            "environment": env,
            "total_pipelines": len(pipelines),
            "successful": successful,
            "failed": failed,
            "total_time": total_time,
            "results": results,
        }

    def show_generated_code(self, filename: str) -> Optional[str]:
        """Show generated code for a specific file.

        Args:
            filename: Name of the generated file

        Returns:
            Generated code content or None if not found
        """
        if filename not in self.generated_files:
            available_files = list(self.generated_files.keys())
            print(f"❌ File '{filename}' not found")
            print(f"📁 Available files: {available_files}")
            return None

        code = self.generated_files[filename]
        print(f"📄 Generated code for: {filename}")
        print("─" * 60)
        print(code)
        print("─" * 60)

        return code

    def get_execution_stats(self) -> Dict[str, Any]:
        """Get execution statistics.

        Returns:
            Dictionary with execution statistics
        """
        return self.execution_stats.copy()

    def _print_validation_results(self, result: Dict[str, Any]):
        """Print validation results in notebook-friendly format."""
        pipeline = result["pipeline"]
        env = result["environment"]

        if result["valid"]:
            print(f"✅ Pipeline '{pipeline}' is valid for environment '{env}'")
        else:
            print(
                f"❌ Pipeline '{pipeline}' has validation errors for environment '{env}'"
            )

            if result["errors"]:
                print("\n🚨 Errors:")
                for error in result["errors"]:
                    print(f"   • {error}")

            if result["warnings"]:
                print("\n⚠️  Warnings:")
                for warning in result["warnings"]:
                    print(f"   • {warning}")

        print(f"\n⏱️  Validation time: {result['validation_time']:.2f}s")

    def _print_generation_results(self, result: Dict[str, Any]):
        """Print generation results in notebook-friendly format."""
        pipeline = result["pipeline"]
        env = result["environment"]

        if result["success"]:
            if result["dry_run"]:
                print(f"📋 Dry run for pipeline '{pipeline}' in environment '{env}'")
                print(f"📄 Would generate {len(result['files_generated'])} files:")
                for filename in result["files_generated"]:
                    print(f"   • {filename}")
            else:
                print(f"✅ Generated pipeline '{pipeline}' for environment '{env}'")
                print(f"📁 Output directory: {result['output_directory']}")
                print(f"📄 Generated {len(result['files_generated'])} files:")
                for filename in result["files_generated"]:
                    print(f"   • {filename}")
        else:
            print(
                f"❌ Failed to generate pipeline '{pipeline}' for environment '{env}'"
            )
            print(f"🚨 Error: {result['error']}")

        print(f"\n⏱️  Execution time: {result['execution_time']:.2f}s")


# Convenience functions for notebook use
def create_interface(project_root: Union[str, Path] = None) -> NotebookInterface:
    """Create a notebook interface instance.

    Args:
        project_root: Root directory of the LakehousePlumber project

    Returns:
        NotebookInterface instance
    """
    return NotebookInterface(project_root)


def quick_generate(
    pipeline_name: str, env: str = "dev", project_root: Union[str, Path] = None
) -> Dict[str, Any]:
    """Quick generation function for single pipeline.

    Args:
        pipeline_name: Name of the pipeline to generate
        env: Environment to generate for
        project_root: Root directory of the LakehousePlumber project

    Returns:
        Generation results
    """
    interface = create_interface(project_root)
    return interface.generate_pipeline(pipeline_name, env)


def quick_validate(
    pipeline_name: str, env: str = "dev", project_root: Union[str, Path] = None
) -> Dict[str, Any]:
    """Quick validation function for single pipeline.

    Args:
        pipeline_name: Name of the pipeline to validate
        env: Environment to validate for
        project_root: Root directory of the LakehousePlumber project

    Returns:
        Validation results
    """
    interface = create_interface(project_root)
    return interface.validate_pipeline(pipeline_name, env)
