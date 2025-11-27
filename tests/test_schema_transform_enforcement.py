"""Tests for schema transform enforcement modes (strict vs permissive)."""

import pytest
from lhp.models.config import Action, ActionType, TransformType, ProjectConfig, ProjectOperationalMetadataConfig, MetadataColumnConfig
from lhp.generators.transform.schema import SchemaTransformGenerator


class TestSchemaTransformEnforcement:
    """Test strict and permissive enforcement modes."""
    
    def test_strict_mode_drops_unmapped_columns(self):
        """Test that strict mode drops columns not in the schema."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="strict_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
c_custkey -> customer_id
c_name -> customer_name
            """,
            enforcement="strict",
            target="v_clean",
            readMode="batch"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # In strict mode, should use df.select() to drop unmapped columns
        assert "df.select(" in code
        assert '"customer_id"' in code
        assert '"customer_name"' in code
    
    def test_strict_mode_keeps_mapped_columns(self):
        """Test that strict mode keeps all mapped columns."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="strict_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
a -> col_a
b -> col_b
c -> col_c
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # Check that all renamed columns are in the select
        assert '"col_a"' in code
        assert '"col_b"' in code
        assert '"col_c"' in code
    
    def test_strict_mode_preserves_metadata_columns(self):
        """Test that strict mode preserves operational metadata columns."""
        generator = SchemaTransformGenerator()
        
        # Create project config with operational metadata
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    ),
                    "_source_file": MetadataColumnConfig(
                        expression="F.input_file_name()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        action = Action(
            name="strict_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
c_custkey -> customer_id
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {"project_config": project_config}
        code = generator.generate(action, context)
        
        # Check that metadata columns are in the select
        assert '"_ingestion_timestamp"' in code
        assert '"_source_file"' in code
        assert '"customer_id"' in code
    
    def test_strict_mode_checks_column_existence(self):
        """Test that strict mode generates code that checks for column existence before selecting."""
        generator = SchemaTransformGenerator()
        
        # Create project config with operational metadata
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_processing_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    ),
                    "_source_file_path": MetadataColumnConfig(
                        expression="F.input_file_name()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        action = Action(
            name="strict_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
c_custkey -> customer_id
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {"project_config": project_config}
        code = generator.generate(action, context)
        
        # Verify that schema columns are added directly (not checked)
        assert "columns_to_select = [" in code
        assert '"customer_id"' in code
        
        # Verify that metadata columns are checked for existence at runtime
        assert "available_columns = set(df.columns)" in code
        assert "for meta_col in metadata_columns:" in code
        assert "if meta_col in available_columns:" in code
        assert "columns_to_select.append(meta_col)" in code
        assert "df.select(*columns_to_select)" in code
    
    def test_permissive_mode_keeps_all_columns(self):
        """Test that permissive mode keeps all columns including unmapped ones."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="permissive_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
c_custkey -> customer_id
            """,
            enforcement="permissive",
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # In permissive mode, should NOT use df.select()
        # All columns pass through, only specified ones are renamed/cast
        assert "df.select(" not in code
        assert "df.withColumnRenamed(\"c_custkey\", \"customer_id\")" in code
    
    def test_permissive_mode_applies_renames_and_casts(self):
        """Test that permissive mode still applies renames and casts."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="permissive_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
c_custkey -> customer_id: BIGINT
c_name -> customer_name
account_balance: DECIMAL(18,2)
            """,
            enforcement="permissive",
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # Check renames
        assert "df.withColumnRenamed(\"c_custkey\", \"customer_id\")" in code
        assert "df.withColumnRenamed(\"c_name\", \"customer_name\")" in code
        
        # Check casts
        assert "F.col(\"customer_id\").cast(\"BIGINT\")" in code
        assert "F.col(\"account_balance\").cast(\"DECIMAL(18,2)\")" in code
    
    def test_default_enforcement_is_permissive(self):
        """Test that default enforcement mode is permissive when not specified."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="default_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
c_custkey -> customer_id
            """,
            # No enforcement specified - defaults to permissive
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # Should behave like permissive (no df.select)
        assert "df.select(" not in code
        assert "df.withColumnRenamed(\"c_custkey\", \"customer_id\")" in code
    
    def test_strict_mode_with_type_casting_only(self):
        """Test strict mode with only type casting (no renames)."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="strict_casting",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
customer_id: BIGINT
amount: DECIMAL(18,2)
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # Should still use select in strict mode
        assert "df.select(" in code
        assert '"customer_id"' in code
        assert '"amount"' in code
        assert "F.col(\"customer_id\").cast(\"BIGINT\")" in code
        assert "F.col(\"amount\").cast(\"DECIMAL(18,2)\")" in code
    
    def test_metadata_columns_not_transformed(self):
        """Test that metadata columns are never renamed or cast."""
        generator = SchemaTransformGenerator()
        
        # Create project config with operational metadata
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    ),
                    "_source_file": MetadataColumnConfig(
                        expression="F.input_file_name()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        action = Action(
            name="transform_with_metadata",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
_ingestion_timestamp -> renamed_timestamp
c_custkey -> customer_id: BIGINT
_source_file: STRING
            """,
            target="v_clean"
        )
        
        context = {"project_config": project_config}
        code = generator.generate(action, context)
        
        # Metadata columns should NOT be renamed
        assert "withColumnRenamed(\"_ingestion_timestamp\"" not in code
        
        # Metadata columns should NOT be cast
        assert "_source_file" not in code or "cast" not in code or code.index("_source_file") > code.index("return df")
        
        # Regular columns should be transformed
        assert "withColumnRenamed(\"c_custkey\", \"customer_id\")" in code
        assert "F.col(\"customer_id\").cast(\"BIGINT\")" in code


class TestSchemaTransformColumnOrdering:
    """Test that column ordering is preserved correctly."""
    
    def test_column_order_preserved_in_strict_mode(self):
        """Test that columns appear in the order defined in the schema."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="ordered_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
d_col -> fourth
a_col -> first
c_col -> third
b_col -> second
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # Extract the order of columns in columns_to_select list
        select_idx = code.index("columns_to_select = [")
        select_section = code[select_idx:select_idx + 500]
        
        # Columns should appear in the order they were defined in schema_inline
        first_pos = select_section.index('"fourth"')
        second_pos = select_section.index('"first"')
        third_pos = select_section.index('"third"')
        fourth_pos = select_section.index('"second"')
        
        # Check they appear in defined order
        assert first_pos < second_pos < third_pos < fourth_pos
    
    def test_metadata_columns_appear_last(self):
        """Test that metadata columns appear after data columns in select."""
        generator = SchemaTransformGenerator()
        
        # Create project config with operational metadata
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    ),
                    "_source_file": MetadataColumnConfig(
                        expression="F.input_file_name()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        action = Action(
            name="ordered_with_metadata",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
a -> col_a
b -> col_b
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {"project_config": project_config}
        code = generator.generate(action, context)
        
        # Extract the columns_to_select section and metadata_columns section
        # Data columns are in columns_to_select
        select_idx = code.index("columns_to_select = [")
        select_section = code[select_idx:select_idx + 200]
        
        # Metadata columns are in metadata_columns list
        meta_idx = code.index("metadata_columns = [")
        meta_section = code[meta_idx:meta_idx + 200]
        
        # Verify data columns are in columns_to_select
        assert '"col_a"' in select_section
        assert '"col_b"' in select_section
        
        # Verify metadata columns are in metadata_columns (not in initial columns_to_select)
        assert '"_ingestion_timestamp"' in meta_section
        assert '"_source_file"' in meta_section
        assert '"_ingestion_timestamp"' not in select_section
        assert '"_source_file"' not in select_section
    
    def test_mixed_mapping_and_casting_order(self):
        """Test order with both column mapping and type casting."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="mixed_ordered_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            schema_inline="""
a -> first: STRING
second: BIGINT
c -> third
            """,
            enforcement="strict",
            target="v_clean"
        )
        
        context = {}
        code = generator.generate(action, context)
        
        # Check df.select() has all columns
        assert '"first"' in code
        assert '"second"' in code
        assert '"third"' in code
        
        # Verify transformations are applied
        assert "withColumnRenamed(\"a\", \"first\")" in code
        assert "withColumnRenamed(\"c\", \"third\")" in code
        assert "F.col(\"second\").cast(\"BIGINT\")" in code
        assert "F.col(\"first\").cast(\"STRING\")" in code

