"""Tests for pipeline field functionality - generated code constants."""

import tempfile
from pathlib import Path

import yaml

from lhp.core.coordination import ActionOrchestrator


class TestPipelineFieldFunctionality:
    """Test pipeline field functionality for the orchestrator."""

    def test_generated_constants_have_correct_values(self):
        """Test that generated constants PIPELINE_ID and FLOWGROUP_ID have correct values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Change to project directory (required for Path.cwd() calls in code_generator)
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(str(project_root))

                # Create project structure
                (project_root / "pipelines" / "test").mkdir(parents=True)
                (project_root / "substitutions").mkdir()
                (project_root / "templates").mkdir()
                (project_root / "presets").mkdir()

                # Create flowgroup
                flowgroup_dict = {
                    "pipeline": "test",  # Match the search term
                    "flowgroup": "customer_ingestion",
                    "actions": [
                        {
                            "name": "load_customers",
                            "type": "load",
                            "target": "v_customers",
                            "source": {"type": "sql", "sql": "SELECT * FROM customers"},
                        },
                        {
                            "name": "write_customers",
                            "type": "write",
                            "source": "v_customers",
                            "write_target": {
                                "type": "streaming_table",
                                "catalog": "test_cat",
                                "schema": "bronze",
                                "table": "customers",
                                "create_table": True,
                            },
                        },
                    ],
                }

                # Save flowgroup
                flowgroup_file = (
                    project_root / "pipelines" / "test" / "customer_ingestion.yaml"
                )
                with open(flowgroup_file, "w") as f:
                    yaml.dump(flowgroup_dict, f)

                # Create substitution file
                sub_file = project_root / "substitutions" / "dev.yaml"
                with open(sub_file, "w") as f:
                    yaml.dump({"dev": {}}, f)

                # Generate code (output_dir required to read back content for
                # assertions; payload diet stripped content from the return).
                output_dir = project_root / "generated"
                orchestrator = ActionOrchestrator(project_root)
                generated_filenames = orchestrator.generate_pipeline_by_field(
                    pipeline_field="test", env="dev", output_dir=output_dir
                )

                # Get generated code
                assert len(generated_filenames) == 1
                code = (output_dir / "test" / generated_filenames[0]).read_text()

                # Verify the fixed behavior where constants have correct values
                assert (
                    'PIPELINE_ID = "test"' in code
                )  # Should be pipeline field from YAML
                assert 'FLOWGROUP_ID = "customer_ingestion"' in code
            finally:
                os.chdir(original_cwd)  # Should be flowgroup field
