"""Tests for Dependency Resolver - Step 4.3.5."""

import pytest
from lhp.core.dependency_resolver import DependencyResolver
from lhp.core.validator import ConfigValidator
from lhp.models.config import Action, ActionType, TransformType, FlowGroup


class TestDependencyResolver:
    """Test dependency resolver functionality."""
    
    def test_simple_dependency_chain(self):
        """Test resolving simple linear dependencies."""
        resolver = DependencyResolver()
        
        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "cloudfiles", "path": "/mnt/data"}
            ),
            Action(
                name="clean_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_raw_data",
                target="v_clean_data",
                sql="SELECT * FROM v_raw_data WHERE is_valid = true"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_clean_data", "table": "clean_data"}
            )
        ]
        
        ordered = resolver.resolve_dependencies(actions)
        
        # Verify order
        assert len(ordered) == 3
        assert ordered[0].name == "load_data"
        assert ordered[1].name == "clean_data"
        assert ordered[2].name == "write_data"
    
    def test_parallel_dependencies(self):
        """Test resolving actions that can run in parallel."""
        resolver = DependencyResolver()
        
        actions = [
            Action(
                name="load_customers",
                type=ActionType.LOAD,
                target="v_customers",
                source={"type": "delta", "table": "customers"}
            ),
            Action(
                name="load_orders",
                type=ActionType.LOAD,
                target="v_orders",
                source={"type": "delta", "table": "orders"}
            ),
            Action(
                name="join_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_customers", "v_orders"],
                target="v_customer_orders",
                sql="SELECT * FROM v_customers c JOIN v_orders o ON c.id = o.customer_id"
            ),
            Action(
                name="write_result",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_customer_orders", "table": "customer_orders"}
            )
        ]
        
        ordered = resolver.resolve_dependencies(actions)
        
        # Verify that loads can be in any order but before join
        load_names = {ordered[0].name, ordered[1].name}
        assert load_names == {"load_customers", "load_orders"}
        assert ordered[2].name == "join_data"
        assert ordered[3].name == "write_result"
    
    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies."""
        resolver = DependencyResolver()
        
        actions = [
            Action(
                name="action1",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_view2",
                target="v_view1",
                sql="SELECT * FROM v_view2"
            ),
            Action(
                name="action2",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_view3",
                target="v_view2",
                sql="SELECT * FROM v_view3"
            ),
            Action(
                name="action3",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_view1",
                target="v_view3",
                sql="SELECT * FROM v_view1"
            )
        ]
        
        with pytest.raises(ValueError, match="Circular dependency detected"):
            resolver.resolve_dependencies(actions)
    
    def test_validate_relationships(self):
        """Test relationship validation."""
        resolver = DependencyResolver()
        
        # Valid flowgroup
        valid_actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "cloudfiles", "path": "/mnt/data"}
            ),
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_raw_data",
                target="v_transformed",
                sql="SELECT * FROM v_raw_data"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_transformed", "table": "output"}
            )
        ]
        
        errors = resolver.validate_relationships(valid_actions)
        assert len(errors) == 0
        
        # Missing load action
        no_load_actions = [
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="external_table",  # External source
                target="v_transformed",
                sql="SELECT * FROM external_table"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_transformed", "table": "output"}
            )
        ]
        
        errors = resolver.validate_relationships(no_load_actions)
        assert any("must have at least one Load action" in error for error in errors)
        
        # Missing write action (without orphaned transforms)
        no_write_actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "cloudfiles", "path": "/mnt/data"}
            )
        ]
        
        errors = resolver.validate_relationships(no_write_actions)
        assert any("must have at least one Write action" in error for error in errors)
    
    def test_missing_dependency_detection(self):
        """Test detection of missing dependencies.

        NOTE: With registry-based detection, we can only detect missing internal
        dependencies, not distinguish between external tables and typos. This test
        now validates that a source referencing a non-existent internal view with
        NO load action still raises an error about missing load action.
        """
        resolver = DependencyResolver()

        actions = [
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_missing_view",  # This view is not produced by any action
                target="v_transformed",
                sql="SELECT * FROM v_missing_view"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_transformed", "table": "output"}
            )
        ]

        errors = resolver.validate_relationships(actions)
        # Since there's no load action and v_missing_view is treated as external,
        # we should get a "must have at least one Load action" error
        assert any("must have at least one Load action" in error for error in errors)
    
    def test_orphaned_action_detection(self):
        """Test detection of orphaned actions."""
        resolver = DependencyResolver()
        
        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "cloudfiles", "path": "/mnt/data"}
            ),
            Action(
                name="orphaned_transform",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_raw_data",
                target="v_orphaned",
                sql="SELECT * FROM v_raw_data"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_raw_data", "table": "output"}
            )
        ]
        
        try:
            errors = resolver.validate_relationships(actions)
            assert any("orphaned_transform" in error and "no other action references it" in error for error in errors)
        except Exception as e:
            # Handle LHPError by converting to string (like the validator does)
            error_str = str(e)
            assert "orphaned_transform" in error_str and "no other action references it" in error_str
    
    def test_complex_dependency_graph(self):
        """Test resolving complex dependency graph."""
        resolver = DependencyResolver()
        
        actions = [
            # Load actions
            Action(name="load_a", type=ActionType.LOAD, target="v_a", source={"type": "delta", "table": "a"}),
            Action(name="load_b", type=ActionType.LOAD, target="v_b", source={"type": "delta", "table": "b"}),
            Action(name="load_c", type=ActionType.LOAD, target="v_c", source={"type": "delta", "table": "c"}),
            
            # Transform actions
            Action(
                name="join_ab",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_a", "v_b"],
                target="v_ab",
                sql="SELECT * FROM v_a JOIN v_b"
            ),
            Action(
                name="join_bc",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_b", "v_c"],
                target="v_bc",
                sql="SELECT * FROM v_b JOIN v_c"
            ),
            Action(
                name="final_join",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_ab", "v_bc"],
                target="v_final",
                sql="SELECT * FROM v_ab JOIN v_bc"
            ),
            
            # Write action
            Action(
                name="write_final",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_final", "table": "final_result"}
            )
        ]
        
        ordered = resolver.resolve_dependencies(actions)
        
        # Verify dependency order
        action_positions = {action.name: i for i, action in enumerate(ordered)}
        
        # Loads should come first
        assert action_positions["load_a"] < action_positions["join_ab"]
        assert action_positions["load_b"] < action_positions["join_ab"]
        assert action_positions["load_b"] < action_positions["join_bc"]
        assert action_positions["load_c"] < action_positions["join_bc"]
        
        # Joins should be ordered correctly
        assert action_positions["join_ab"] < action_positions["final_join"]
        assert action_positions["join_bc"] < action_positions["final_join"]
        
        # Write should be last
        assert action_positions["final_join"] < action_positions["write_final"]
    
    def test_execution_stages(self):
        """Test grouping actions into execution stages."""
        resolver = DependencyResolver()
        
        actions = [
            Action(name="load_a", type=ActionType.LOAD, target="v_a", source={"type": "delta", "table": "a"}),
            Action(name="load_b", type=ActionType.LOAD, target="v_b", source={"type": "delta", "table": "b"}),
            Action(
                name="transform_a",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_a",
                target="v_a_clean",
                sql="SELECT * FROM v_a"
            ),
            Action(
                name="transform_b",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_b",
                target="v_b_clean",
                sql="SELECT * FROM v_b"
            ),
            Action(
                name="join_ab",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_a_clean", "v_b_clean"],
                target="v_final",
                sql="SELECT * FROM v_a_clean JOIN v_b_clean"
            ),
            Action(
                name="write_result",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_final", "table": "result"}
            )
        ]
        
        stages = resolver.get_execution_stages(actions)
        
        # Should have 4 stages
        assert len(stages) == 4
        
        # Stage 1: Both loads can run in parallel
        assert len(stages[0]) == 2
        stage0_names = {action.name for action in stages[0]}
        assert stage0_names == {"load_a", "load_b"}
        
        # Stage 2: Both transforms can run in parallel
        assert len(stages[1]) == 2
        stage1_names = {action.name for action in stages[1]}
        assert stage1_names == {"transform_a", "transform_b"}
        
        # Stage 3: Join
        assert len(stages[2]) == 1
        assert stages[2][0].name == "join_ab"
        
        # Stage 4: Write
        assert len(stages[3]) == 1
        assert stages[3][0].name == "write_result"
    
    def test_external_source_handling(self):
        """Test handling of external sources (not produced by any action)."""
        resolver = DependencyResolver()

        actions = [
            Action(
                name="load_from_external",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="bronze.customers",  # External table (not produced by any action in this flowgroup)
                target="v_customers",
                sql="SELECT * FROM bronze.customers"
            ),
            Action(
                name="write_customers",
                type=ActionType.WRITE,
                source={"type": "streaming_table", "view": "v_customers", "table": "silver_customers"}
            )
        ]

        # Should not error on external source
        errors = resolver.validate_relationships(actions)
        assert not any("bronze.customers" in error for error in errors)

        # But should still validate other requirements
        assert any("must have at least one Load action" in error for error in errors)

    def test_snapshot_cdc_source_function_no_false_dependencies(self):
        """Test that snapshot CDC with source_function should not create false dependencies.
        
        This reproduces Error 1 from the ACMI project where a snapshot CDC action
        with source_function has a redundant action.source field that creates a
        false dependency error: "depends on 'v_part_bronze_snapshot' which is not produced by any action"
        """
        validator = ConfigValidator()
        
        # Create a snapshot CDC write action with source_function
        # This mimics the part_silver_dim.yaml configuration
        snapshot_cdc_action = Action(
            name="write_part_silver_snapshot",
            type=ActionType.WRITE,
            source="v_part_bronze_snapshot",  # This is redundant for snapshot CDC with source_function
            write_target={
                "type": "streaming_table",
                "database": "catalog.silver_schema",
                "table": "part_dim",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "py_functions/part_snapshot_func.py",
                        "function": "next_snapshot_and_version"
                    },
                    "keys": ["part_id"],
                    "stored_as_scd_type": 2,
                    "track_history_except_column_list": ["_source_file_path", "_processing_timestamp"]
                }
            }
        )
        
        # Create flowgroup to test complete validation flow
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="part_silver_dim",
            actions=[snapshot_cdc_action]
        )
        
        # After fix: This should pass because source_function is self-contained
        errors = validator.validate_flowgroup(flowgroup)
        
        # Should NOT have false dependency error for snapshot CDC with source_function
        dependency_errors = [e for e in errors if "depends on 'v_part_bronze_snapshot' which is not produced by any action" in e]
        assert len(dependency_errors) == 0, f"Should not have false dependency error for snapshot CDC with source_function. Got errors: {errors}"

    def test_snapshot_cdc_source_function_no_load_action_required(self):
        """Test that flowgroup with only snapshot CDC + source_function should be valid.
        
        This reproduces Error 2 from the ACMI project where a flowgroup containing
        only a snapshot CDC action with source_function fails validation with:
        "FlowGroup must have at least one Load action"
        
        However, snapshot CDC with source_function is self-contained and should not
        require a separate load action.
        """
        validator = ConfigValidator()
        
        # Create a flowgroup with only snapshot CDC + source_function
        # This should be valid because the source_function provides the data
        snapshot_cdc_action = Action(
            name="write_part_silver_snapshot",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "catalog.silver_schema", 
                "table": "part_dim",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "py_functions/part_snapshot_func.py",
                        "function": "next_snapshot_and_version"
                    },
                    "keys": ["part_id"],
                    "stored_as_scd_type": 2
                }
            }
        )
        
        # Create flowgroup to test complete validation flow
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="part_silver_dim",
            actions=[snapshot_cdc_action]  # Only one action, no LOAD actions
        )
        
        # After fix: This should pass because snapshot CDC with source_function is self-contained
        errors = validator.validate_flowgroup(flowgroup)
        
        # Should NOT have load action requirement error for self-contained snapshot CDC
        load_action_errors = [e for e in errors if "FlowGroup must have at least one Load action" in e]
        assert len(load_action_errors) == 0, f"Should not require load action for self-contained snapshot CDC flowgroup. Got errors: {errors}"

    def test_normal_write_action_unchanged(self):
        """Test that normal (non-CDC) write actions still work as before."""
        resolver = DependencyResolver()
        
        # Create normal load and write actions (traditional pattern)
        load_action = Action(
            name="load_customer_data",
            type=ActionType.LOAD,
            target="v_customer_raw",
            source={"type": "cloudfiles", "path": "/data/customers", "format": "parquet"}
        )
        
        write_action = Action(
            name="write_customer_table",
            type=ActionType.WRITE,
            source="v_customer_raw",  # Traditional source reference
            write_target={
                "type": "streaming_table",
                "database": "catalog.bronze",
                "table": "customers"
            }
        )
        
        actions = [load_action, write_action]
        
        # Should pass validation - normal pattern unchanged
        errors = resolver.validate_relationships(actions)
        assert len(errors) == 0, f"Normal write actions should still work. Got errors: {errors}"
        
        # Verify dependency detection works
        sources = resolver._get_action_sources(write_action)
        assert sources == ["v_customer_raw"], f"Normal write action should extract source correctly. Got: {sources}"

    def test_cdc_mode_with_explicit_source(self):
        """Test that CDC mode (not snapshot_cdc) with explicit source still works."""
        resolver = DependencyResolver()
        
        # Create CDC write action with explicit source in cdc_config
        cdc_action = Action(
            name="write_customer_cdc",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "catalog.bronze",
                "table": "customers",
                "mode": "cdc",
                "cdc_config": {
                    "source": "v_customer_changes",
                    "keys": ["customer_id"]
                }
            }
        )
        
        # Should extract source from cdc_config, not action.source
        sources = resolver._get_action_sources(cdc_action)
        assert sources == ["v_customer_changes"], f"CDC action should extract source from cdc_config. Got: {sources}"

    def test_snapshot_cdc_with_explicit_source(self):
        """Test that snapshot_cdc with explicit source (not source_function) still works."""
        resolver = DependencyResolver()
        
        # Create snapshot CDC with explicit source reference
        snapshot_action = Action(
            name="write_customer_snapshot",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "catalog.silver",
                "table": "customers",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source": "v_customer_snapshots",  # Explicit source, no source_function
                    "keys": ["customer_id"]
                }
            }
        )
        
        # Should extract source from snapshot_cdc_config.source
        sources = resolver._get_action_sources(snapshot_action)
        assert sources == ["v_customer_snapshots"], f"Snapshot CDC with explicit source should work. Got: {sources}"

    def test_mixed_action_types_coexist(self):
        """Test that different action types (normal, CDC, snapshot CDC) can coexist."""
        validator = ConfigValidator()
        
        # Mix of different action types
        load_action = Action(
            name="load_raw_data",
            type=ActionType.LOAD,
            target="v_raw_data",
            source={"type": "cloudfiles", "path": "/data", "format": "parquet"}
        )
        
        normal_write = Action(
            name="write_bronze_normal",
            type=ActionType.WRITE,
            source="v_raw_data",
            write_target={
                "type": "streaming_table",
                "database": "bronze",
                "table": "normal_table"
            }
        )
        
        snapshot_cdc_self_contained = Action(
            name="write_silver_snapshot",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "snapshot_table",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "functions/snapshot.py",
                        "function": "get_snapshot"
                    },
                    "keys": ["id"]
                }
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="mixed_pipeline",
            flowgroup="mixed_flowgroup",
            actions=[load_action, normal_write, snapshot_cdc_self_contained]
        )
        
        # Should validate successfully - mix of patterns should coexist
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0, f"Mixed action types should coexist. Got errors: {errors}"

    def test_malformed_cdc_config_fallback(self):
        """Test that malformed CDC configs fall back to action.source gracefully."""
        resolver = DependencyResolver()

        # CDC action with malformed config - missing source in cdc_config
        malformed_cdc = Action(
            name="write_malformed_cdc",
            type=ActionType.WRITE,
            source="v_fallback_source",  # Should fallback to this
            write_target={
                "type": "streaming_table",
                "database": "bronze",
                "table": "malformed",
                "mode": "cdc",
                "cdc_config": {}  # Empty config, no source
            }
        )

        # Should fallback to action.source
        sources = resolver._get_action_sources(malformed_cdc)
        assert sources == ["v_fallback_source"], f"Malformed CDC should fallback to action.source. Got: {sources}"

    def test_non_v_prefix_internal_views(self):
        """Test that internal views without v_ prefix are recognized correctly."""
        resolver = DependencyResolver()

        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="raw_customer_data",  # ← No v_ prefix
                source={"type": "cloudfiles", "path": "/data/customers"}
            ),
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="raw_customer_data",  # ← References non-v_ internal view
                target="staging_customer",   # ← No v_ prefix
                sql="SELECT * FROM raw_customer_data WHERE active = true"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source="staging_customer",
                write_target={
                    "type": "streaming_table",
                    "database": "catalog.bronze",
                    "table": "customer"
                }
            )
        ]

        # Should pass validation - all dependencies exist
        errors = resolver.validate_relationships(actions)

        # Should not have missing dependency errors
        missing_dep_errors = [e for e in errors if "not produced by any action" in e]
        assert len(missing_dep_errors) == 0, f"Should not have missing dependency errors. Got: {missing_dep_errors}"

        # Verify dependency order is correct
        ordered = resolver.resolve_dependencies(actions)
        assert ordered[0].name == "load_data"
        assert ordered[1].name == "transform_data"
        assert ordered[2].name == "write_data"

    def test_mixed_naming_conventions(self):
        """Test that v_ and non-v_ prefixed targets can coexist."""
        resolver = DependencyResolver()

        actions = [
            Action(
                name="load_a",
                type=ActionType.LOAD,
                target="v_data_a",  # ← Uses v_ prefix
                source={"type": "delta", "table": "source_a"}
            ),
            Action(
                name="load_b",
                type=ActionType.LOAD,
                target="raw_data_b",  # ← No v_ prefix
                source={"type": "delta", "table": "source_b"}
            ),
            Action(
                name="transform_merged",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_data_a", "raw_data_b"],  # ← Mixed references
                target="staging_merged",  # ← No v_ prefix
                sql="SELECT * FROM v_data_a JOIN raw_data_b"
            ),
            Action(
                name="write_result",
                type=ActionType.WRITE,
                source="staging_merged",
                write_target={
                    "type": "streaming_table",
                    "database": "catalog.silver",
                    "table": "merged_data"
                }
            )
        ]

        # Should pass validation - mixed naming is fine
        errors = resolver.validate_relationships(actions)
        missing_dep_errors = [e for e in errors if "not produced by any action" in e]
        assert len(missing_dep_errors) == 0, f"Should handle mixed naming conventions. Got: {missing_dep_errors}"

        # Verify all dependencies are detected
        ordered = resolver.resolve_dependencies(actions)
        action_positions = {action.name: i for i, action in enumerate(ordered)}

        assert action_positions["load_a"] < action_positions["transform_merged"]
        assert action_positions["load_b"] < action_positions["transform_merged"]
        assert action_positions["transform_merged"] < action_positions["write_result"]

    def test_internal_dependency_typo_detection(self):
        """Test that typos in internal view references are detected.

        NOTE: Registry-based detection can only catch typos if we know the correct
        target exists. If a source references a non-existent name, the system treats
        it as external. To detect typos, the dependency chain must break in a
        detectable way.
        """
        resolver = DependencyResolver()

        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="customer_data",
                source={"type": "delta", "table": "customers"}
            ),
            Action(
                name="transform_correct",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="customer_data",  # ← Correct reference
                target="enriched_data",
                sql="SELECT * FROM customer_data"
            ),
            Action(
                name="transform_depends_on_typo",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="enriched_dta",  # ← Typo: 'enriched_dta' instead of 'enriched_data'
                target="final_data",
                sql="SELECT * FROM enriched_dta"
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source="final_data",
                write_target={
                    "type": "streaming_table",
                    "database": "catalog.silver",
                    "table": "customer"
                }
            )
        ]

        # With the typo, enriched_dta is not in targets and not caught as error
        # because it's treated as external. However, enriched_data becomes orphaned!
        # The orphaned action detection will catch this.
        with pytest.raises(Exception) as exc_info:
            errors = resolver.validate_relationships(actions)

        # Should detect orphaned transform (enriched_data is not used)
        assert "transform_correct" in str(exc_info.value) or "enriched_data" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 