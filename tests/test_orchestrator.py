"""Tests for Action Orchestrator - Step 4.5.8."""

import pytest
import tempfile
from pathlib import Path
import yaml

from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import FlowGroup, Action, ActionType, TransformType


class TestActionOrchestrator:
    """Test action orchestrator functionality."""
    
    def create_test_project(self, tmpdir):
        """Create a test project structure with sample files."""
        project_root = Path(tmpdir)
        
        # Create directories
        (project_root / "pipelines" / "test_pipeline").mkdir(parents=True)
        (project_root / "presets").mkdir()
        (project_root / "templates").mkdir()
        (project_root / "substitutions").mkdir()
        
        # Create substitution file
        substitutions = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze",
                "landing_path": "/mnt/dev/landing"
            },
            "secrets": {
                "default_scope": "dev_secrets",
                "scopes": {
                    "db": "dev_db_secrets"
                }
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(substitutions, f)
        
        # Create preset file
        preset = {
            "name": "bronze_layer",
            "version": "1.0",
            "defaults": {
                "load_actions": {
                    "cloudfiles": {
                        "schema_evolution_mode": "addNewColumns",
                        "rescue_data_column": "_rescued_data"
                    }
                }
            }
        }
        with open(project_root / "presets" / "bronze_layer.yaml", "w") as f:
            yaml.dump(preset, f)
        
        # Create a simple flowgroup
        flowgroup = {
            "pipeline": "test_pipeline",
            "flowgroup": "test_flowgroup",
            "presets": ["bronze_layer"],
            "actions": [
                {
                    "name": "load_customers",
                    "type": "load",
                    "target": "v_customers_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "{landing_path}/customers",
                        "format": "json"
                    }
                },
                {
                    "name": "clean_customers",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": "v_customers_raw",
                    "target": "v_customers_clean",
                    "sql": "SELECT * FROM v_customers_raw WHERE is_valid = true"
                },
                {
                    "name": "write_customers",
                    "type": "write",
                    "source": "v_customers_clean",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "{bronze_schema}",
                        "table": "customers",
                        "create_table": True
                    }
                }
            ]
        }
        with open(project_root / "pipelines" / "test_pipeline" / "test_flowgroup.yaml", "w") as f:
            yaml.dump(flowgroup, f)
        
        return project_root
    
    def test_orchestrator_initialization(self):
        """Test orchestrator initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            orchestrator = ActionOrchestrator(project_root)
            
            assert orchestrator.project_root == project_root
            assert orchestrator.yaml_parser is not None
            assert orchestrator.preset_manager is not None
            assert orchestrator.template_engine is not None
            assert orchestrator.action_registry is not None
    
    def test_discover_flowgroups(self):
        """Test flowgroup discovery."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            orchestrator = ActionOrchestrator(project_root)
            
            pipeline_dir = project_root / "pipelines" / "test_pipeline"
            flowgroups = orchestrator._discover_flowgroups(pipeline_dir)
            
            assert len(flowgroups) == 1
            assert flowgroups[0].flowgroup == "test_flowgroup"
            assert len(flowgroups[0].actions) == 3
    
    def test_generate_pipeline(self):
        """Test complete pipeline generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            orchestrator = ActionOrchestrator(project_root)
            
            # Generate pipeline
            output_dir = project_root / "generated"
            generated_files = orchestrator.generate_pipeline("test_pipeline", "dev", output_dir)
            
            # Verify files were generated
            assert len(generated_files) == 1
            assert "test_flowgroup.py" in generated_files
            
            # Verify generated code content
            code = generated_files["test_flowgroup.py"]
            
            # Check header
            assert "# Generated by LakehousePlumber" in code
            assert "# Pipeline: test_pipeline" in code
            assert "# FlowGroup: test_flowgroup" in code
            
            # Check imports
            assert "import dlt" in code
            
            # Check generated functions
            assert "@dlt.view()" in code
            assert "def v_customers_raw():" in code
            assert "def v_customers_clean():" in code
            
            # Check substitutions were applied
            assert "/mnt/dev/landing/customers" in code  # {landing_path} substituted
            assert 'name="bronze.customers"' in code  # {bronze_schema} substituted in table name
            
            # Check preset defaults were applied
            assert "addNewColumns" in code
            assert "_rescued_data" in code
            
            # Verify file was written
            assert (output_dir / "test_flowgroup.py").exists()
    
    def test_flowgroup_with_secret_substitution(self):
        """Test flowgroup with secret references generates valid Python code."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            
            # Create flowgroup with secrets
            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "secret_flowgroup",
                "actions": [
                    {
                        "name": "load_from_db",
                        "type": "load",
                        "target": "v_db_data",
                        "source": {
                            "type": "jdbc",
                            "url": "jdbc:postgresql://${secret:db/host}:5432/mydb",
                            "user": "${secret:db/username}",
                            "password": "${secret:db/password}",
                            "driver": "org.postgresql.Driver",
                            "table": "customers"
                        }
                    },
                    {
                        "name": "write_data",
                        "type": "write",
                        "source": "v_db_data",
                        "write_target": {
                            "type": "streaming_table",
                            "database": "silver",
                            "table": "customers",
                            "create_table": True
                        }
                    }
                ]
            }
            with open(project_root / "pipelines" / "test_pipeline" / "secret_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup, f)
            
            orchestrator = ActionOrchestrator(project_root)
            generated_files = orchestrator.generate_pipeline("test_pipeline", "dev")
            
            # Verify valid f-string generation for secrets
            code = generated_files["secret_flowgroup.py"]
            
            # Check for valid f-string syntax with secrets (not broken inline secrets)
            # URL should be an f-string with host secret
            assert 'f"jdbc:postgresql://{dbutils.secrets.get(scope=' in code
            assert "'dev_db_secrets'" in code and "'host'" in code
            
            # User and password should be direct dbutils calls (entire string is secret)
            assert 'dbutils.secrets.get(scope=' in code
            # Check for either single or double quotes around the key names
            assert ('key="username"' in code or "key='username'" in code)
            assert ('key="password"' in code or "key='password'" in code)
            
            # Verify the generated code is syntactically valid Python
            try:
                compile(code, '<string>', 'exec')
                # If compilation succeeds, the code is valid
                assert True
            except SyntaxError:
                pytest.fail("Generated code with secrets is not valid Python syntax")
    
    def test_template_expansion(self):
        """Test template expansion in flowgroup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            
            # Create a template
            template = {
                "name": "standard_ingestion",
                "version": "1.0",
                "parameters": [
                    {"name": "source_table", "type": "string", "required": True},
                    {"name": "target_database", "type": "string", "required": True}
                ],
                "actions": [
                    {
                        "name": "load_{{ source_table }}",
                        "type": "load",
                        "target": "v_{{ source_table }}_raw",
                        "source": {
                            "type": "delta",
                            "database": "source",
                            "table": "{{ source_table }}"
                        }
                    },
                    {
                        "name": "write_{{ source_table }}",
                        "type": "write",
                        "source": "v_{{ source_table }}_raw",
                        "write_target": {
                            "type": "streaming_table",
                            "database": "{{ target_database }}",
                            "table": "{{ source_table }}",
                            "create_table": True
                        }
                    }
                ]
            }
            with open(project_root / "templates" / "standard_ingestion.yaml", "w") as f:
                yaml.dump(template, f)
            
            # Create flowgroup using template
            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "template_flowgroup",
                "use_template": "standard_ingestion",
                "template_parameters": {
                    "source_table": "orders",
                    "target_database": "silver"
                }
            }
            with open(project_root / "pipelines" / "test_pipeline" / "template_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup, f)
            
            orchestrator = ActionOrchestrator(project_root)
            generated_files = orchestrator.generate_pipeline("test_pipeline", "dev")
            
            # Verify template was expanded
            code = generated_files["template_flowgroup.py"]
            assert "def v_orders_raw():" in code
            assert 'spark.read.table("source.orders")' in code  # Delta table reference
            assert 'name="silver.orders"' in code  # Full table name in streaming table
    
    def test_validation_errors(self):
        """Test validation error handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            
            # Create invalid flowgroup (missing required fields)
            invalid_flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "invalid_flowgroup",
                "actions": [
                    {
                        "name": "invalid_action",
                        "type": "load"
                        # Missing target and source
                    }
                ]
            }
            with open(project_root / "pipelines" / "test_pipeline" / "invalid_flowgroup.yaml", "w") as f:
                yaml.dump(invalid_flowgroup, f)
            
            orchestrator = ActionOrchestrator(project_root)
            
            # Should raise validation error
            with pytest.raises(ValueError, match="validation failed"):
                orchestrator.generate_pipeline("test_pipeline", "dev")
    
    def test_dependency_resolution(self):
        """Test that actions are generated in dependency order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            
            # Create flowgroup with complex dependencies
            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "dependency_flowgroup",
                "actions": [
                    {
                        "name": "join_ab",
                        "type": "transform",
                        "transform_type": "sql",
                        "source": ["v_a", "v_b"],
                        "target": "v_ab",
                        "sql": "SELECT * FROM v_a JOIN v_b ON v_a.id = v_b.id"
                    },
                    {
                        "name": "load_b",
                        "type": "load",
                        "target": "v_b",
                        "source": {"type": "delta", "table": "table_b"}
                    },
                    {
                        "name": "load_a",
                        "type": "load",
                        "target": "v_a",
                        "source": {"type": "delta", "table": "table_a"}
                    },
                    {
                        "name": "write_result",
                        "type": "write",
                        "source": "v_ab",
                        "write_target": {
                            "type": "streaming_table",
                            "database": "gold",
                            "table": "result",
                            "create_table": True
                        }
                    }
                ]
            }
            with open(project_root / "pipelines" / "test_pipeline" / "dependency_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup, f)
            
            orchestrator = ActionOrchestrator(project_root)
            generated_files = orchestrator.generate_pipeline("test_pipeline", "dev")
            
            code = generated_files["dependency_flowgroup.py"]
            
            # Find positions of function definitions
            pos_a = code.find("def v_a():")
            pos_b = code.find("def v_b():")
            pos_ab = code.find("def v_ab():")
            
            # Verify dependency order: loads before join
            assert pos_a < pos_ab
            assert pos_b < pos_ab


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 