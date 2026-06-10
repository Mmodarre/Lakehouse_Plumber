import tempfile
from pathlib import Path

import pytest
import yaml

from lhp.api import collect_response
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.errors import LHPError
from tests.helpers import read_generated_pipeline


class TestPresetTemplateCombination:
    def test_preset_applies_to_template_generated_write_actions(self):
        """Presets applied before template expansion caused preset config to never reach template-generated actions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            self._create_project_directories(project_root)
            self._create_test_preset(project_root)
            self._create_test_template(project_root)
            self._create_test_flowgroup(project_root)
            self._create_substitutions(project_root)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            generated_files = read_generated_pipeline(
                facade,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            generated_code = generated_files.get("preset_template_test.py", "")

            assert "delta.enableRowTracking" in generated_code, (
                "Preset table property should be applied to template-generated write action"
            )

            assert "delta.autoOptimize.optimizeWrite" in generated_code, (
                "Preset table property should be applied to template-generated write action"
            )

            assert "customer_data" in generated_code, (
                "Template should be expanded with parameters"
            )

            assert "dp.create_streaming_table" in generated_code, (
                "Template should generate streaming table write action"
            )

    def test_preset_only_flowgroup_still_works(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            self._create_project_directories(project_root)
            self._create_test_preset(project_root)

            preset_only_flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "preset_only_test",
                "presets": ["delta_optimization"],
                "actions": [
                    {
                        "name": "load_test_data",
                        "type": "load",
                        "source": {"type": "sql", "sql": "SELECT 1 as test_col"},
                        "target": "v_test_data",
                    },
                    {
                        "name": "write_direct",
                        "type": "write",
                        "source": "v_test_data",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "bronze",
                            "schema": "test",
                            "table": "direct_write",
                            "table_properties": {"manual": "property"},
                        },
                    },
                ],
            }

            flowgroup_file = (
                project_root / "pipelines" / "test_pipeline" / "preset_only_test.yaml"
            )
            with open(flowgroup_file, "w") as f:
                yaml.dump(preset_only_flowgroup, f)

            self._create_substitutions(project_root)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            generated_files = read_generated_pipeline(
                facade,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            generated_code = generated_files.get("preset_only_test.py", "")

            assert "delta.enableRowTracking" in generated_code, (
                "Preset should be applied to direct write action"
            )

            assert "manual" in generated_code, (
                "Original table properties should be preserved"
            )

    def test_template_only_flowgroup_still_works(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            self._create_project_directories(project_root)
            self._create_test_template(project_root)

            template_only_flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "template_only_test",
                "use_template": "data_ingestion",
                "template_parameters": {
                    "table_name": "orders_data",
                    "target_catalog": "silver",
                    "target_schema": "orders",
                },
            }

            flowgroup_file = (
                project_root / "pipelines" / "test_pipeline" / "template_only_test.yaml"
            )
            with open(flowgroup_file, "w") as f:
                yaml.dump(template_only_flowgroup, f)

            self._create_substitutions(project_root)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            generated_files = read_generated_pipeline(
                facade,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            generated_code = generated_files.get("template_only_test.py", "")

            assert "orders_data" in generated_code, (
                "Template should be expanded with parameters"
            )

            assert "template_generated" in generated_code, (
                "Template properties should be preserved"
            )

            # Should NOT have preset properties
            assert "delta.enableRowTracking" not in generated_code, (
                "Should not have preset properties when no preset is used"
            )

    def test_preset_inheritance_with_templates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            self._create_project_directories(project_root)

            base_preset = {
                "name": "base_optimization",
                "version": "1.0",
                "defaults": {
                    "write_actions": {
                        "streaming_table": {
                            "table_properties": {"base.setting": "true"}
                        }
                    }
                },
            }

            base_preset_file = project_root / "presets" / "base_optimization.yaml"
            with open(base_preset_file, "w") as f:
                yaml.dump(base_preset, f)

            child_preset = {
                "name": "enhanced_optimization",
                "version": "1.0",
                "extends": "base_optimization",
                "defaults": {
                    "write_actions": {
                        "streaming_table": {
                            "table_properties": {
                                "child.setting": "enhanced",
                                "delta.enableRowTracking": "true",
                            }
                        }
                    }
                },
            }

            child_preset_file = project_root / "presets" / "enhanced_optimization.yaml"
            with open(child_preset_file, "w") as f:
                yaml.dump(child_preset, f)

            self._create_test_template(project_root)

            inheritance_flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "inheritance_test",
                "presets": ["enhanced_optimization"],
                "use_template": "data_ingestion",
                "template_parameters": {
                    "table_name": "inherited_data",
                    "target_catalog": "gold",
                    "target_schema": "analytics",
                },
            }

            flowgroup_file = (
                project_root / "pipelines" / "test_pipeline" / "inheritance_test.yaml"
            )
            with open(flowgroup_file, "w") as f:
                yaml.dump(inheritance_flowgroup, f)

            self._create_substitutions(project_root)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            generated_files = read_generated_pipeline(
                facade,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            generated_code = generated_files.get("inheritance_test.py", "")

            assert "base.setting" in generated_code, (
                "Base preset properties should be inherited"
            )

            assert "child.setting" in generated_code, (
                "Child preset properties should be applied"
            )

            assert "delta.enableRowTracking" in generated_code, (
                "Child preset should override/add properties"
            )

            assert "inherited_data" in generated_code, (
                "Template should be expanded with parameters"
            )

    def _create_project_directories(self, project_root: Path):
        for dir_name in [
            "presets",
            "templates",
            "pipelines/test_pipeline",
            "substitutions",
        ]:
            (project_root / dir_name).mkdir(parents=True)

    def _create_test_preset(self, project_root: Path):
        preset_content = {
            "name": "delta_optimization",
            "version": "1.0",
            "description": "Preset with Delta table optimizations",
            "defaults": {
                "write_actions": {
                    "streaming_table": {
                        "table_properties": {
                            "delta.enableRowTracking": "true",
                            "delta.autoOptimize.optimizeWrite": "true",
                            "delta.autoOptimize.autoCompact": "true",
                        }
                    }
                }
            },
        }

        preset_file = project_root / "presets" / "delta_optimization.yaml"
        with open(preset_file, "w") as f:
            yaml.dump(preset_content, f)

    def _create_test_template(self, project_root: Path):
        template_content = {
            "name": "data_ingestion",
            "version": "1.0",
            "description": "Template for data ingestion with write action",
            "parameters": [
                {"name": "table_name", "type": "string", "required": True},
                {"name": "target_catalog", "type": "string", "required": True},
                {"name": "target_schema", "type": "string", "required": True},
            ],
            "actions": [
                {
                    "name": "load_{{ table_name }}",
                    "type": "load",
                    "source": {
                        "type": "sql",
                        "sql": "SELECT * FROM source_{{ table_name }}",
                    },
                    "target": "v_{{ table_name }}_raw",
                },
                {
                    "name": "write_{{ table_name }}",
                    "type": "write",
                    "source": "v_{{ table_name }}_raw",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "{{ target_catalog }}",
                        "schema": "{{ target_schema }}",
                        "table": "{{ table_name }}",
                        "table_properties": {"source": "template_generated"},
                    },
                },
            ],
        }

        template_file = project_root / "templates" / "data_ingestion.yaml"
        with open(template_file, "w") as f:
            yaml.dump(template_content, f)

    def _create_test_flowgroup(self, project_root: Path):
        flowgroup_content = {
            "pipeline": "test_pipeline",
            "flowgroup": "preset_template_test",
            "presets": ["delta_optimization"],
            "use_template": "data_ingestion",
            "template_parameters": {
                "table_name": "customer_data",
                "target_catalog": "bronze",
                "target_schema": "customers",
            },
        }

        flowgroup_file = (
            project_root / "pipelines" / "test_pipeline" / "preset_template_test.yaml"
        )
        with open(flowgroup_file, "w") as f:
            yaml.dump(flowgroup_content, f)

    def _create_substitutions(self, project_root: Path):
        substitutions = {
            "catalog": "dev_catalog",
            "raw_schema": "raw",
            "bronze_schema": "bronze",
        }

        substitution_file = project_root / "substitutions" / "dev.yaml"
        with open(substitution_file, "w") as f:
            yaml.dump(substitutions, f)

    def test_template_preset_applies_to_template_actions(self):
        """Template-declared presets apply to generated actions even when the flowgroup declares none."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            self._create_project_directories(project_root)

            preset = {
                "name": "template_test_preset",
                "version": "1.0",
                "defaults": {
                    "write_actions": {
                        "streaming_table": {
                            "table_properties": {
                                "template.preset.applied": "true",
                                "delta.enableRowTracking": "true",
                            }
                        }
                    }
                },
            }
            with open(project_root / "presets" / "template_test_preset.yaml", "w") as f:
                yaml.dump(preset, f)

            template = {
                "name": "template_with_preset",
                "version": "1.0",
                "presets": ["template_test_preset"],
                "parameters": [
                    {"name": "table_name", "type": "string", "required": True}
                ],
                "actions": [
                    {
                        "name": "load_{{ table_name }}",
                        "type": "load",
                        "source": {"type": "sql", "sql": "SELECT 1 as id"},
                        "target": "v_{{ table_name }}",
                    },
                    {
                        "name": "write_{{ table_name }}",
                        "type": "write",
                        "source": "v_{{ table_name }}",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test_db",
                            "table": "{{ table_name }}",
                        },
                    },
                ],
            }
            with open(
                project_root / "templates" / "template_with_preset.yaml", "w"
            ) as f:
                yaml.dump(template, f)

            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "template_preset_test",
                "use_template": "template_with_preset",
                "template_parameters": {"table_name": "test_table"},
            }
            with open(
                project_root
                / "pipelines"
                / "test_pipeline"
                / "template_preset_test.yaml",
                "w",
            ) as f:
                yaml.dump(flowgroup, f)

            self._create_substitutions(project_root)

            lhp_config = {"name": "test_project", "version": "1.0"}
            with open(project_root / "lhp.yaml", "w") as f:
                yaml.dump(lhp_config, f)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            result = read_generated_pipeline(
                facade,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )
            generated_code = result.get("template_preset_test.py", "")

            assert "template.preset.applied" in generated_code, (
                "Template preset property should be applied to generated actions"
            )
            assert "delta.enableRowTracking" in generated_code, (
                "Template preset property should be applied to generated actions"
            )

    def test_template_and_flowgroup_presets_both_apply(self):
        """Both template and flowgroup presets apply; flowgroup preset takes precedence on shared keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            self._create_project_directories(project_root)

            template_preset = {
                "name": "template_preset",
                "version": "1.0",
                "defaults": {
                    "write_actions": {
                        "streaming_table": {
                            "table_properties": {
                                "from.template": "true",
                                "shared.property": "template_value",
                            }
                        }
                    }
                },
            }
            with open(project_root / "presets" / "template_preset.yaml", "w") as f:
                yaml.dump(template_preset, f)

            flowgroup_preset = {
                "name": "flowgroup_preset",
                "version": "1.0",
                "defaults": {
                    "write_actions": {
                        "streaming_table": {
                            "table_properties": {
                                "from.flowgroup": "true",
                                "shared.property": "flowgroup_value",
                            }
                        }
                    }
                },
            }
            with open(project_root / "presets" / "flowgroup_preset.yaml", "w") as f:
                yaml.dump(flowgroup_preset, f)

            template = {
                "name": "template_with_preset",
                "version": "1.0",
                "presets": ["template_preset"],
                "parameters": [
                    {"name": "table_name", "type": "string", "required": True}
                ],
                "actions": [
                    {
                        "name": "load_{{ table_name }}",
                        "type": "load",
                        "source": {"type": "sql", "sql": "SELECT 1 as id"},
                        "target": "v_{{ table_name }}",
                    },
                    {
                        "name": "write_{{ table_name }}",
                        "type": "write",
                        "source": "v_{{ table_name }}",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test_db",
                            "table": "{{ table_name }}",
                        },
                    },
                ],
            }
            with open(
                project_root / "templates" / "template_with_preset.yaml", "w"
            ) as f:
                yaml.dump(template, f)

            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "combined_preset_test",
                "presets": ["flowgroup_preset"],
                "use_template": "template_with_preset",
                "template_parameters": {"table_name": "test_table"},
            }
            with open(
                project_root
                / "pipelines"
                / "test_pipeline"
                / "combined_preset_test.yaml",
                "w",
            ) as f:
                yaml.dump(flowgroup, f)

            self._create_substitutions(project_root)

            # Create lhp.yaml
            lhp_config = {"name": "test_project", "version": "1.0"}
            with open(project_root / "lhp.yaml", "w") as f:
                yaml.dump(lhp_config, f)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            result = read_generated_pipeline(
                facade,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )
            generated_code = result.get("combined_preset_test.py", "")

            # Both presets should be applied
            assert "from.template" in generated_code, (
                "Template preset properties should be applied"
            )
            assert "from.flowgroup" in generated_code, (
                "Flowgroup preset properties should be applied"
            )
            # Flowgroup preset should override shared property
            assert "flowgroup_value" in generated_code, (
                "Flowgroup preset should override template preset for shared properties"
            )

    def test_template_with_missing_preset_raises_error(self):
        """Unknown preset is rejected via collect_response (raises LHPError, not a not-success DTO)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            self._create_project_directories(project_root)

            # Create template referencing non-existent preset
            template = {
                "name": "template_with_missing_preset",
                "version": "1.0",
                "presets": ["non_existent_preset"],
                "parameters": [
                    {"name": "table_name", "type": "string", "required": True}
                ],
                "actions": [
                    {
                        "name": "load_{{ table_name }}",
                        "type": "load",
                        "source": {"type": "sql", "sql": "SELECT 1"},
                        "target": "v_{{ table_name }}",
                    },
                    {
                        "name": "write_{{ table_name }}",
                        "type": "write",
                        "source": "v_{{ table_name }}",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test_db",
                            "table": "{{ table_name }}",
                        },
                    },
                ],
            }
            with open(
                project_root / "templates" / "template_with_missing_preset.yaml", "w"
            ) as f:
                yaml.dump(template, f)

            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "missing_preset_test",
                "use_template": "template_with_missing_preset",
                "template_parameters": {"table_name": "test_table"},
            }
            with open(
                project_root
                / "pipelines"
                / "test_pipeline"
                / "missing_preset_test.yaml",
                "w",
            ) as f:
                yaml.dump(flowgroup, f)

            self._create_substitutions(project_root)

            # Create lhp.yaml
            lhp_config = {"name": "test_project", "version": "1.0"}
            with open(project_root / "lhp.yaml", "w") as f:
                yaml.dump(lhp_config, f)

            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )

            with pytest.raises(LHPError) as exc_info:
                collect_response(
                    facade.generation.generate_pipelines(
                        pipeline_filter="test_pipeline",
                        env="dev",
                        output_dir=None,
                    )
                )
            error_msg = str(exc_info.value)
            assert "non_existent_preset" in error_msg
            assert (
                "unknown" in error_msg.lower()
                or "not found" in error_msg.lower()
                or "not a valid preset" in error_msg.lower()
            )
