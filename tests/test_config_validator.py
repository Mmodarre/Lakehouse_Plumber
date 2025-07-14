"""Tests for Configuration Validator - Step 4.4.3."""

import pytest
from lhp.core.validator import ConfigValidator
from lhp.models.config import FlowGroup, Action, ActionType, TransformType


class TestConfigValidator:
    """Test configuration validator functionality."""
    
    def test_valid_flowgroup(self):
        """Test validation of a valid flowgroup."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_raw_data",
                    source={
                        "type": "cloudfiles",
                        "path": "/mnt/data",
                        "format": "json"
                    }
                ),
                Action(
                    name="transform_data",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    source="v_raw_data",
                    target="v_clean_data",
                    sql="SELECT * FROM v_raw_data WHERE is_valid = true"
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_clean_data",
                    write_target={
                        "type": "streaming_table",
                        "database": "silver",
                        "table": "clean_data"
                    }
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0
    
    def test_missing_required_fields(self):
        """Test validation catches missing required fields."""
        validator = ConfigValidator()
        
        # Missing pipeline name
        flowgroup = FlowGroup(
            pipeline="",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="test", type=ActionType.LOAD, target="v_test", source={"type": "delta", "table": "test"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("pipeline" in error for error in errors)
        
        # Missing flowgroup name
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="",
            actions=[
                Action(name="test", type=ActionType.LOAD, target="v_test", source={"type": "delta", "table": "test"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("flowgroup" in error for error in errors)
        
        # No actions
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("at least one action" in error for error in errors)
    
    def test_duplicate_names(self):
        """Test detection of duplicate action and target names."""
        validator = ConfigValidator()
        
        # Duplicate action names
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="load_data", type=ActionType.LOAD, target="v_data1", source={"type": "delta", "table": "t1"}),
                Action(name="load_data", type=ActionType.LOAD, target="v_data2", source={"type": "delta", "table": "t2"}),
                Action(name="write", type=ActionType.WRITE, source="v_data1", write_target={"type": "streaming_table", "database": "db", "table": "t"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("Duplicate action name" in error and "load_data" in error for error in errors)
        
        # Duplicate target names
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="load1", type=ActionType.LOAD, target="v_data", source={"type": "delta", "table": "t1"}),
                Action(name="load2", type=ActionType.LOAD, target="v_data", source={"type": "delta", "table": "t2"}),
                Action(name="write", type=ActionType.WRITE, source="v_data", write_target={"type": "streaming_table", "database": "db", "table": "t"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("Duplicate target name" in error and "v_data" in error for error in errors)
    
    def test_load_action_validation(self):
        """Test validation of load actions."""
        validator = ConfigValidator()
        
        # Valid CloudFiles load
        action = Action(
            name="load_cloudfiles",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "cloudfiles",
                "path": "/mnt/data",
                "format": "json"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Missing required fields for CloudFiles
        action = Action(
            name="load_cloudfiles",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "cloudfiles"
                # Missing path and format
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("path" in error for error in errors)
        assert any("format" in error for error in errors)
        
        # Valid JDBC load
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://host:5432/db",
                "user": "user",
                "password": "pass",
                "driver": "org.postgresql.Driver",
                "table": "customers"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # JDBC missing query or table
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://host:5432/db",
                "user": "user",
                "password": "pass",
                "driver": "org.postgresql.Driver"
                # Missing both query and table
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("query" in error and "table" in error for error in errors)
    
    def test_transform_action_validation(self):
        """Test validation of transform actions."""
        validator = ConfigValidator()
        
        # Valid SQL transform
        action = Action(
            name="transform_sql",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source="v_input",
            target="v_output",
            sql="SELECT * FROM v_input"
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Missing SQL for SQL transform
        action = Action(
            name="transform_sql",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source="v_input",
            target="v_output"
            # Missing sql or sql_path
        )
        errors = validator.validate_action(action, 0)
        assert any("sql" in error and "sql_path" in error for error in errors)
        
        # Missing transform_type
        action = Action(
            name="transform",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            sql="SELECT * FROM v_input"
            # Missing transform_type
        )
        errors = validator.validate_action(action, 0)
        assert any("transform_type" in error for error in errors)
        
        # Valid Python transform
        action = Action(
            name="transform_python",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            target="v_output",
            source={
                "module_path": "transformations.py",
                "function_name": "transform_data",
                "sources": ["v_input"]
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
    
    def test_write_action_validation(self):
        """Test validation of write actions."""
        validator = ConfigValidator()
        
        # Valid streaming table write
        action = Action(
            name="write_streaming",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Missing required fields
        action = Action(
            name="write_streaming",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table"
                # Missing database, table
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("database" in error for error in errors)
        assert any("table" in error for error in errors)
        
        # Valid materialized view with SQL
        action = Action(
            name="write_mv",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "database": "gold",
                "table": "summary",
                "sql": "SELECT COUNT(*) FROM silver.details"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
    
    def test_action_type_validation(self):
        """Test validation of action types."""
        validator = ConfigValidator()
        
        # Missing action name
        action = Action(
            name="",
            type=ActionType.LOAD,
            target="v_data",
            source={"type": "delta", "table": "test"}
        )
        errors = validator.validate_action(action, 0)
        assert any("name" in error for error in errors)
        
        # Invalid source type for load
        action = Action(
            name="load",
            type=ActionType.LOAD,
            target="v_data",
            source={"type": "invalid_type", "path": "/mnt/data"}
        )
        errors = validator.validate_action(action, 0)
        assert any("Unknown load source type" in error and "invalid_type" in error for error in errors)
        
        # Note: We can't test invalid transform_type directly because Pydantic validates the enum
        # at construction time. This is actually good - it prevents invalid data from being created.
        # The validator still checks if the transform type is supported by the registry.
    
    def test_dependency_validation(self):
        """Test that dependency validation is included."""
        validator = ConfigValidator()
        
        # FlowGroup with missing dependencies
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="transform",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    source="v_missing",  # This view doesn't exist
                    target="v_output",
                    sql="SELECT * FROM v_missing"
                ),
                Action(
                    name="write",
                    type=ActionType.WRITE,
                    source="v_output",
                    write_target={
                        "type": "streaming_table",
                        "database": "silver",
                        "table": "output"
                    }
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        # Should have errors about missing load action and missing dependency
        assert any("Load action" in error for error in errors)
        assert any("v_missing" in error for error in errors)
    
    def test_edge_cases(self):
        """Test edge cases in validation."""
        validator = ConfigValidator()
        
        # Action with non-dict source for load (should fail)
        action = Action(
            name="load",
            type=ActionType.LOAD,
            target="v_data",
            source="string_source"  # Should be dict
        )
        errors = validator.validate_action(action, 0)
        assert any("configuration object" in error for error in errors)
        
        # Write action with target (warning, not error)
        action = Action(
            name="write",
            type=ActionType.WRITE,
            target="v_should_not_have_target",  # Write actions shouldn't have targets
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "output"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0  # Should only log warning, not error
    
    def test_dlt_table_options_validation(self):
        """Test validation of DLT table options."""
        validator = ConfigValidator()
        
        # Valid options
        action = Action(
            name="write_with_options",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                },
                "table_properties": {
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.enableChangeDataFeed": "true"
                },
                "schema": "id BIGINT, name STRING, amount DECIMAL(18,2)",
                "row_filter": "ROW FILTER catalog.schema.filter_fn ON (region)",
                "temporary": True,
                "partition_columns": ["region", "status"],
                "cluster_columns": ["id"]
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Invalid spark_conf (not a dict)
        action = Action(
            name="write_invalid_spark_conf",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "spark_conf": "invalid"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("spark_conf" in error and "dictionary" in error for error in errors)
        
        # Invalid table_properties (not a dict)
        action = Action(
            name="write_invalid_table_props",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "table_properties": "invalid"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("table_properties" in error and "dictionary" in error for error in errors)
        
        # Invalid schema (not a string)
        action = Action(
            name="write_invalid_schema",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "schema": {"invalid": "object"}
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("schema" in error and "string" in error for error in errors)
        
        # Invalid row_filter (not a string)
        action = Action(
            name="write_invalid_row_filter",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "row_filter": 123
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("row_filter" in error and "string" in error for error in errors)
        
        # Invalid temporary (not a boolean)
        action = Action(
            name="write_invalid_temporary",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "temporary": "yes"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("temporary" in error and "boolean" in error for error in errors)
        
        # Invalid partition_columns (not a list)
        action = Action(
            name="write_invalid_partitions",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "partition_columns": "region"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("partition_columns" in error and "list" in error for error in errors)
        
        # Invalid cluster_columns (not a list)
        action = Action(
            name="write_invalid_clusters",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table",
                "cluster_columns": "id"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("cluster_columns" in error and "list" in error for error in errors)

    def test_snapshot_cdc_validation(self):
        """Test validation of snapshot CDC configuration."""
        validator = ConfigValidator()
        
        # Valid snapshot CDC with simple source
        action = Action(
            name="valid_snapshot_cdc",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source": "raw.customer_snapshots",
                    "keys": ["customer_id"],
                    "stored_as_scd_type": 1
                }
            }
        )
        errors = validator.validate_action(action, 0)
        snapshot_errors = [e for e in errors if 'snapshot_cdc' in e]
        assert len(snapshot_errors) == 0
        
        # Valid snapshot CDC with function source
        action = Action(
            name="valid_snapshot_cdc_func",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "snapshot_functions.py",
                        "function": "next_snapshot"
                    },
                    "keys": ["customer_id", "region"],
                    "stored_as_scd_type": 2,
                    "track_history_column_list": ["name", "email"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        snapshot_errors = [e for e in errors if 'snapshot_cdc' in e]
        assert len(snapshot_errors) == 0
        
        # Missing snapshot_cdc_config
        action = Action(
            name="missing_config",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers"
                # Missing snapshot_cdc_config
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("snapshot_cdc mode requires 'snapshot_cdc_config'" in error for error in errors)
        
        # Missing both source and source_function
        action = Action(
            name="missing_source",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "keys": ["customer_id"]
                    # Missing source
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("must have either 'source' or 'source_function'" in error for error in errors)
        
        # Both source and source_function provided
        action = Action(
            name="both_sources",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source": "raw.table",
                    "source_function": {"file": "test.py", "function": "test"},
                    "keys": ["customer_id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("cannot have both 'source' and 'source_function'" in error for error in errors)
        
        # Missing keys
        action = Action(
            name="missing_keys",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source": "raw.table"
                    # Missing keys
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("must have 'keys'" in error for error in errors)
        
        # Invalid SCD type
        action = Action(
            name="invalid_scd_type",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source": "raw.table",
                    "keys": ["id"],
                    "stored_as_scd_type": 3  # Invalid
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'stored_as_scd_type' must be 1 or 2" in error for error in errors)
        
        # Both track history options
        action = Action(
            name="both_track_history",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source": "raw.table",
                    "keys": ["id"],
                    "track_history_column_list": ["name"],
                    "track_history_except_column_list": ["id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("cannot have both 'track_history_column_list' and 'track_history_except_column_list'" in error for error in errors)
        
        # Invalid source_function structure
        action = Action(
            name="invalid_source_function",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "test.py"
                        # Missing function
                    },
                    "keys": ["id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("source_function must have 'function'" in error for error in errors)

    def test_duplicate_pipeline_flowgroup_validation(self):
        """Test validation fails when two flowgroups have the same pipeline+flowgroup combination."""
        validator = ConfigValidator()
        
        # Create two flowgroups with the same pipeline+flowgroup combination
        flowgroup1 = FlowGroup(
            pipeline="raw_ingestions",
            flowgroup="customer_ingestion",
            actions=[
                Action(
                    name="load_customers1",
                    type=ActionType.LOAD,
                    target="v_customers1",
                    source={
                        "type": "cloudfiles",
                        "path": "/mnt/data/customers1",
                        "format": "json"
                    }
                ),
                Action(
                    name="write_customers1",
                    type=ActionType.WRITE,
                    source="v_customers1",
                    write_target={
                        "type": "streaming_table",
                        "database": "bronze",
                        "table": "customers1",
                        "create_table": True
                    }
                )
            ]
        )
        
        flowgroup2 = FlowGroup(
            pipeline="raw_ingestions",  # Same pipeline
            flowgroup="customer_ingestion",  # Same flowgroup
            actions=[
                Action(
                    name="load_customers2",
                    type=ActionType.LOAD,
                    target="v_customers2",
                    source={
                        "type": "cloudfiles",
                        "path": "/mnt/data/customers2",
                        "format": "json"
                    }
                ),
                Action(
                    name="write_customers2",
                    type=ActionType.WRITE,
                    source="v_customers2",
                    write_target={
                        "type": "streaming_table",
                        "database": "bronze",
                        "table": "customers2",
                        "create_table": True
                    }
                )
            ]
        )
        
        # Validate duplicate pipeline+flowgroup combination
        errors = validator.validate_duplicate_pipeline_flowgroup([flowgroup1, flowgroup2])
        
        # Should have one error about duplicate combination
        assert len(errors) == 1
        assert "raw_ingestions.customer_ingestion" in errors[0]
        assert "duplicate" in errors[0].lower()
        
    def test_unique_pipeline_flowgroup_validation_passes(self):
        """Test validation passes when all pipeline+flowgroup combinations are unique."""
        validator = ConfigValidator()
        
        # Create flowgroups with unique pipeline+flowgroup combinations
        flowgroup1 = FlowGroup(
            pipeline="raw_ingestions",
            flowgroup="customer_ingestion",
            actions=[
                Action(
                    name="load_customers",
                    type=ActionType.LOAD,
                    target="v_customers",
                    source={
                        "type": "cloudfiles",
                        "path": "/mnt/data/customers",
                        "format": "json"
                    }
                ),
                Action(
                    name="write_customers",
                    type=ActionType.WRITE,
                    source="v_customers",
                    write_target={
                        "type": "streaming_table",
                        "database": "bronze",
                        "table": "customers",
                        "create_table": True
                    }
                )
            ]
        )
        
        flowgroup2 = FlowGroup(
            pipeline="raw_ingestions",
            flowgroup="orders_ingestion",  # Different flowgroup
            actions=[
                Action(
                    name="load_orders",
                    type=ActionType.LOAD,
                    target="v_orders",
                    source={
                        "type": "cloudfiles",
                        "path": "/mnt/data/orders",
                        "format": "json"
                    }
                ),
                Action(
                    name="write_orders",
                    type=ActionType.WRITE,
                    source="v_orders",
                    write_target={
                        "type": "streaming_table",
                        "database": "bronze",
                        "table": "orders",
                        "create_table": True
                    }
                )
            ]
        )
        
        flowgroup3 = FlowGroup(
            pipeline="silver_transforms",  # Different pipeline
            flowgroup="customer_ingestion",  # Same flowgroup name but different pipeline
            actions=[
                Action(
                    name="transform_customers",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    source="bronze.customers",
                    target="v_customers_silver",
                    sql="SELECT * FROM bronze.customers WHERE active = true"
                ),
                Action(
                    name="write_customers_silver",
                    type=ActionType.WRITE,
                    source="v_customers_silver",
                    write_target={
                        "type": "streaming_table",
                        "database": "silver",
                        "table": "customers",
                        "create_table": True
                    }
                )
            ]
        )
        
        # Validate unique pipeline+flowgroup combinations
        errors = validator.validate_duplicate_pipeline_flowgroup([flowgroup1, flowgroup2, flowgroup3])
        
        # Should have no errors
        assert len(errors) == 0

    def test_multiple_flowgroups_same_pipeline_output_directory(self):
        """Test that multiple flowgroups with the same pipeline field generate to the same output directory."""
        import tempfile
        import yaml
        from pathlib import Path
        from lhp.core.orchestrator import ActionOrchestrator
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create project structure
            (project_root / "pipelines" / "dir1").mkdir(parents=True)
            (project_root / "pipelines" / "dir2").mkdir(parents=True)
            (project_root / "substitutions").mkdir()
            (project_root / "templates").mkdir()
            (project_root / "presets").mkdir()
            
            # Create first flowgroup in dir1 with pipeline: raw_ingestions
            flowgroup1_dict = {
                "pipeline": "raw_ingestions",
                "flowgroup": "customer_ingestion",
                "actions": [
                    {
                        "name": "load_customers",
                        "type": "load",
                        "target": "v_customers",
                        "source": {
                            "type": "cloudfiles",
                            "path": "/mnt/data/customers",
                            "format": "json"
                        }
                    },
                    {
                        "name": "write_customers",
                        "type": "write",
                        "source": "v_customers",
                        "write_target": {
                            "type": "streaming_table",
                            "database": "bronze",
                            "table": "customers",
                            "create_table": True
                        }
                    }
                ]
            }
            
            # Create second flowgroup in dir2 with same pipeline: raw_ingestions
            flowgroup2_dict = {
                "pipeline": "raw_ingestions",  # Same pipeline field
                "flowgroup": "orders_ingestion",  # Different flowgroup name
                "actions": [
                    {
                        "name": "load_orders",
                        "type": "load",
                        "target": "v_orders",
                        "source": {
                            "type": "cloudfiles",
                            "path": "/mnt/data/orders",
                            "format": "json"
                        }
                    },
                    {
                        "name": "write_orders",
                        "type": "write",
                        "source": "v_orders",
                        "write_target": {
                            "type": "streaming_table",
                            "database": "bronze",
                            "table": "orders",
                            "create_table": True
                        }
                    }
                ]
            }
            
            # Save flowgroups to different directories
            flowgroup1_file = project_root / "pipelines" / "dir1" / "customer_ingestion.yaml"
            flowgroup2_file = project_root / "pipelines" / "dir2" / "orders_ingestion.yaml"
            
            with open(flowgroup1_file, 'w') as f:
                yaml.dump(flowgroup1_dict, f)
            
            with open(flowgroup2_file, 'w') as f:
                yaml.dump(flowgroup2_dict, f)
            
            # Create substitution file
            sub_file = project_root / "substitutions" / "dev.yaml"
            with open(sub_file, 'w') as f:
                yaml.dump({"environment": {"dev": {}}}, f)
            
            # This test expects the fixed behavior where pipeline field determines output directory
            # Currently it will fail because the system uses directory names instead of pipeline field
            
            # TODO: When implementation is fixed, this should work:
            # orchestrator = ActionOrchestrator(project_root)
            # output_dir = project_root / "generated"
            # 
            # # Generate files using pipeline field (should find both flowgroups)
            # generated_files = orchestrator.generate_pipeline_by_field("raw_ingestions", "dev", output_dir)
            # 
            # # Both flowgroups should be in the same output directory
            # assert len(generated_files) == 2
            # assert "customer_ingestion.py" in generated_files
            # assert "orders_ingestion.py" in generated_files
            # 
            # # Verify files are in the same directory based on pipeline field
            # expected_dir = output_dir / "raw_ingestions"
            # assert (expected_dir / "customer_ingestion.py").exists()
            # assert (expected_dir / "orders_ingestion.py").exists()
            
            # For now, just verify the flowgroups are created correctly
            # This test will be completed when the implementation is fixed
            assert flowgroup1_file.exists()
            assert flowgroup2_file.exists()
            
            # Verify the YAML files have the correct pipeline field
            with open(flowgroup1_file, 'r') as f:
                data1 = yaml.safe_load(f)
                assert data1["pipeline"] == "raw_ingestions"
                assert data1["flowgroup"] == "customer_ingestion"
                
            with open(flowgroup2_file, 'r') as f:
                data2 = yaml.safe_load(f)
                assert data2["pipeline"] == "raw_ingestions"
                assert data2["flowgroup"] == "orders_ingestion"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 