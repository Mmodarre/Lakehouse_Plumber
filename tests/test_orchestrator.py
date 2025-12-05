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
            flowgroups = orchestrator.discover_flowgroups(pipeline_dir)
            
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
            generated_files = orchestrator.generate_pipeline_by_field(
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=output_dir
            )
            
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
            assert "from pyspark import pipelines as dp" in code
            
            # Check generated functions
            assert "@dp.temporary_view()" in code
            assert "def v_customers_raw():" in code
            assert "def v_customers_clean():" in code
            
            # Check substitutions were applied
            assert "/mnt/dev/landing/customers" in code  # {landing_path} substituted
            assert 'name="bronze.customers"' in code  # {bronze_schema} substituted in table name
            
            # Check preset defaults were applied
            assert "addNewColumns" in code
            assert "_rescued_data" in code
    
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
            generated_files = orchestrator.generate_pipeline_by_field(
                pipeline_field="test_pipeline",
                env="dev"
            )
            
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
            generated_files = orchestrator.generate_pipeline_by_field(
                pipeline_field="test_pipeline",
                env="dev"
            )
            
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
                orchestrator.generate_pipeline_by_field(
                    pipeline_field="test_pipeline",
                    env="dev"
                )
    
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
            generated_files = orchestrator.generate_pipeline_by_field(
                pipeline_field="test_pipeline",
                env="dev"
            )
            
            code = generated_files["dependency_flowgroup.py"]
            
            # Find positions of function definitions
            pos_a = code.find("def v_a():")
            pos_b = code.find("def v_b():")
            pos_ab = code.find("def v_ab():")
            
            # Verify dependency order: loads before join
            assert pos_a < pos_ab
            assert pos_b < pos_ab


class TestGenerationAnalysis:
    """Test the new analyze_generation_requirements method."""
    
    def test_analyze_generation_requirements_force_mode(self):
        """Test analysis with force mode enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create minimal project structure
            (project_root / "substitutions").mkdir()
            (project_root / "pipelines" / "test_pipeline").mkdir(parents=True)
            
            orchestrator = ActionOrchestrator(project_root)
            
            # Test force mode
            analysis = orchestrator.analyze_generation_requirements(
                env="dev",
                pipeline_names=["test_pipeline"],
                include_tests=True,
                force=True,
                state_manager=None
            )
            
            # Verify force mode results
            assert analysis.has_work_to_do() == True
            assert "test_pipeline" in analysis.pipelines_needing_generation
            assert analysis.pipelines_needing_generation["test_pipeline"]["reason"] == "force"
            assert analysis.has_global_changes == False
            assert analysis.include_tests_context_applied == False
    
    def test_analyze_generation_requirements_no_state_manager(self):
        """Test analysis without state manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create minimal project structure  
            (project_root / "substitutions").mkdir()
            (project_root / "pipelines" / "test_pipeline").mkdir(parents=True)
            
            orchestrator = ActionOrchestrator(project_root)
            
            # Test without state manager
            analysis = orchestrator.analyze_generation_requirements(
                env="dev",
                pipeline_names=["test_pipeline"],
                include_tests=False,
                force=False,
                state_manager=None
            )
            
            # Verify no state tracking results
            assert analysis.has_work_to_do() == True
            assert "test_pipeline" in analysis.pipelines_needing_generation
            assert analysis.pipelines_needing_generation["test_pipeline"]["reason"] == "no_state_tracking"
            
    def test_generation_analysis_convenience_methods(self):
        """Test GenerationAnalysis convenience methods."""
        from lhp.core.orchestrator import GenerationAnalysis
        
        # Test with work to do
        analysis_with_work = GenerationAnalysis(
            pipelines_needing_generation={"pipeline1": {"new": ["file1.py"]}},
            pipelines_up_to_date={},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=True,
            total_new_files=1,
            total_stale_files=0,
            total_up_to_date_files=0,
            detailed_staleness_info={}
        )
        
        assert analysis_with_work.has_work_to_do() == True
        assert analysis_with_work.get_generation_reason("pipeline1") == "1 new"
        
        # Test without work to do
        analysis_up_to_date = GenerationAnalysis(
            pipelines_needing_generation={},
            pipelines_up_to_date={"pipeline1": 5},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=False,
            total_new_files=0,
            total_stale_files=0,
            total_up_to_date_files=5,
            detailed_staleness_info={}
        )
        
        assert analysis_up_to_date.has_work_to_do() == False
        assert analysis_up_to_date.get_generation_reason("pipeline1") == "up-to-date"


class TestOrchestratorDependencyInjection:
    """Test orchestrator dependency injection functionality."""
    
    def test_orchestrator_with_default_dependencies(self):
        """Test orchestrator initialization with default dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            (project_root / "substitutions").mkdir()
            
            # Should work with default dependencies
            orchestrator = ActionOrchestrator(project_root)
            
            # Verify dependencies are set
            assert orchestrator.dependencies is not None
            assert hasattr(orchestrator.dependencies, 'substitution_factory')
            assert hasattr(orchestrator.dependencies, 'file_writer_factory')
    
    def test_orchestrator_with_custom_dependencies(self):
        """Test orchestrator initialization with custom dependencies."""
        from lhp.core.factories import OrchestrationDependencies, DefaultSubstitutionFactory, DefaultFileWriterFactory
        from unittest.mock import Mock
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            (project_root / "substitutions").mkdir()
            
            # Create custom dependencies  
            mock_substitution_factory = Mock()
            mock_file_writer_factory = Mock()
            custom_deps = OrchestrationDependencies(
                substitution_factory=mock_substitution_factory,
                file_writer_factory=mock_file_writer_factory
            )
            
            # Initialize with custom dependencies
            orchestrator = ActionOrchestrator(project_root, dependencies=custom_deps)
            
            # Verify custom dependencies are used
            assert orchestrator.dependencies.substitution_factory == mock_substitution_factory
            assert orchestrator.dependencies.file_writer_factory == mock_file_writer_factory
    
    def test_dependency_factories_work(self):
        """Test that dependency factories can create instances."""
        from lhp.core.factories import DefaultSubstitutionFactory, DefaultFileWriterFactory
        
        with tempfile.TemporaryDirectory() as tmpdir:
            substitution_file = Path(tmpdir) / "test.yaml" 
            substitution_file.write_text("test: value")
            
            # Test substitution factory
            sub_factory = DefaultSubstitutionFactory()
            sub_manager = sub_factory.create(substitution_file, "test")
            assert sub_manager is not None
            
            # Test file writer factory
            writer_factory = DefaultFileWriterFactory()
            file_writer = writer_factory.create()
            assert file_writer is not None


class TestOrchestratorWithPipelineConfig:
    """Test ActionOrchestrator accepts and uses pipeline config."""
    
    def test_orchestrator_init_without_pipeline_config(self):
        """Orchestrator works without config (backward compatible)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create minimal project structure
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()
            
            # Initialize without pipeline config
            orchestrator = ActionOrchestrator(project_root, enforce_version=False)
            
            # Should initialize successfully
            assert orchestrator.project_root == project_root
            # pipeline_config_path should be None by default
            assert hasattr(orchestrator, 'pipeline_config_path')
            assert orchestrator.pipeline_config_path is None
    
    def test_orchestrator_init_with_pipeline_config(self):
        """Orchestrator accepts pipeline_config_path parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create minimal project structure
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()
            
            config_path = "templates/bundle/pipeline_config.yaml"
            
            # Initialize with pipeline config
            orchestrator = ActionOrchestrator(
                project_root, 
                enforce_version=False,
                pipeline_config_path=config_path
            )
            
            # Config path should be stored
            assert orchestrator.pipeline_config_path == config_path
    
    def test_orchestrator_stores_config_path(self):
        """Orchestrator stores config_path as instance variable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create minimal project structure
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()
            
            config_path = "custom/path/config.yaml"
            orchestrator = ActionOrchestrator(
                project_root, 
                enforce_version=False,
                pipeline_config_path=config_path
            )
            
            # Should be accessible as instance attribute
            assert hasattr(orchestrator, 'pipeline_config_path')
            assert orchestrator.pipeline_config_path == config_path
    
    def test_sync_bundle_resources_uses_config(self):
        """_sync_bundle_resources passes config to BundleManager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create full project structure for bundle support
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()
            (project_root / "databricks.yml").write_text("workspace:\n  host: https://test")
            
            output_dir = project_root / "generated"
            output_dir.mkdir()
            
            config_path = "test_config.yaml"
            orchestrator = ActionOrchestrator(
                project_root,
                enforce_version=False,
                pipeline_config_path=config_path
            )
            
            # Mock the BundleManager to verify config is passed
            from unittest.mock import patch, MagicMock
            
            with patch('lhp.bundle.manager.BundleManager') as mock_bundle_manager_class:
                mock_manager_instance = MagicMock()
                mock_bundle_manager_class.return_value = mock_manager_instance
                
                # Call sync method
                orchestrator._sync_bundle_resources(output_dir, "dev")
                
                # Verify BundleManager was created with config path
                mock_bundle_manager_class.assert_called_once_with(
                    project_root,
                    config_path
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 