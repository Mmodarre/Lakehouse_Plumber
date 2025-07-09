"""Tests for Dependency Resolver - Step 4.3.5."""

import pytest
from lhp.core.dependency_resolver import DependencyResolver
from lhp.models.config import Action, ActionType, TransformType


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
        """Test detection of missing dependencies."""
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
        assert any("v_missing_view" in error and "not produced by any action" in error for error in errors)
    
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
                source="bronze.customers",  # External table (doesn't start with v_)
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 