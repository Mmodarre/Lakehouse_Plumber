import tempfile
from pathlib import Path

import yaml

from lhp.api.facade import LakehousePlumberApplicationFacade
from tests.helpers import read_generated_pipeline


class TestPipelineFieldFunctionality:
    def test_generated_constants_have_correct_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Change to project directory (required for Path.cwd() calls in code_generator)
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(str(project_root))

                (project_root / "pipelines" / "test").mkdir(parents=True)
                (project_root / "substitutions").mkdir()
                (project_root / "templates").mkdir()
                (project_root / "presets").mkdir()

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

                flowgroup_file = (
                    project_root / "pipelines" / "test" / "customer_ingestion.yaml"
                )
                with open(flowgroup_file, "w") as f:
                    yaml.dump(flowgroup_dict, f)

                sub_file = project_root / "substitutions" / "dev.yaml"
                with open(sub_file, "w") as f:
                    yaml.dump({"dev": {}}, f)

                # Generate code (output_dir required to read back content for
                # assertions; payload diet stripped content from the return).
                output_dir = project_root / "generated"
                facade = LakehousePlumberApplicationFacade.for_project(
                    project_root, enforce_version=False
                )
                generated_files = read_generated_pipeline(
                    facade,
                    pipeline_field="test",
                    env="dev",
                    output_dir=output_dir,
                )

                assert len(generated_files) == 1
                code = next(iter(generated_files.values()))

                assert 'PIPELINE_ID = "test"' in code
                assert 'FLOWGROUP_ID = "customer_ingestion"' in code
            finally:
                os.chdir(original_cwd)
