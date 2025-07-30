"""Tests for transform action generators of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
import yaml
from lhp.models.config import Action, ActionType, TransformType, FlowGroup
from lhp.generators.transform import (
    SQLTransformGenerator,
    DataQualityTransformGenerator,
    SchemaTransformGenerator,
    PythonTransformGenerator,
    TempTableTransformGenerator
)


class TestTransformGenerators:
    """Test transform action generators."""
    
    def test_sql_transform_generator(self):
        """Test SQL transform generator."""
        generator = SQLTransformGenerator()
        action = Action(
            name="transform_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source=["v_customers"],
            target="v_customers_clean",
            sql="SELECT * FROM v_customers WHERE email IS NOT NULL"
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view(comment=" in code
        assert "v_customers_clean" in code
        assert "df = spark.sql(" in code
        assert "return df" in code
        assert "SELECT * FROM v_customers WHERE email IS NOT NULL" in code
    
    def test_data_quality_generator(self):
        """Test data quality transform generator."""
        generator = DataQualityTransformGenerator()
        
        # Create expectations file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            expectations = {
                "email IS NOT NULL": {"action": "warn", "name": "email_not_null"},
                "age >= 18": {"action": "drop", "name": "age_check"},
                "id IS NOT NULL": {"action": "fail", "name": "id_not_null"}
            }
            yaml.dump(expectations, f)
            expectations_file = f.name
        
        action = Action(
            name="validate_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_customers_clean",
            target="v_customers_validated",
            readMode="stream",
            expectations_file=expectations_file
        )
        
        code = generator.generate(action, {"spec_dir": Path(expectations_file).parent})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_customers_validated" in code
        assert "@dlt.expect_all_or_fail" in code
        assert "@dlt.expect_all_or_drop" in code
        assert "@dlt.expect_all" in code
        
        # Clean up
        Path(expectations_file).unlink()
    
    def test_python_transform_generator(self):
        """Test Python transform generator."""
        generator = PythonTransformGenerator()
        action = Action(
            name="enrich_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            target="v_customers_enriched",
            source="v_customers_validated",
            module_path="transformations/enrich_customers.py",
            function_name="enrich_customers",
            parameters={"enrichment_type": "full"}
        )
        
        # Create temporary Python file for the test
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            transformations_dir = tmpdir_path / "transformations"
            transformations_dir.mkdir(parents=True)
            (transformations_dir / "enrich_customers.py").write_text("""
def enrich_customers(df, spark, parameters):
    enrichment_type = parameters.get("enrichment_type", "basic")
    return df.withColumn("enrichment_type", enrichment_type)
""")
            
            code = generator.generate(action, {
                "output_dir": tmpdir_path / "generated",
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="test_flowgroup", 
                    actions=[]
                )
            })
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_customers_enriched" in code
        assert "enrich_customers" in code
        assert 'spark.read.table("v_customers_validated")' in code
        # Check that the function is called correctly (imports are managed separately)
        assert "enrich_customers(v_customers_validated_df, spark, parameters)" in code
    
    def test_python_transform_merge_behavior(self):
        """Test merge behavior when custom_python_functions/ already exists with different files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create first Python function
            transforms_dir1 = tmpdir_path / "transforms1"
            transforms_dir1.mkdir(parents=True)
            (transforms_dir1 / "function1.py").write_text("""
def transform_customers(df, spark, parameters):
    return df.withColumn("source", "function1")
""")
            
            # Create second Python function in different directory
            transforms_dir2 = tmpdir_path / "transforms2"  
            transforms_dir2.mkdir(parents=True)
            (transforms_dir2 / "function2.py").write_text("""
def transform_orders(df, spark, parameters):
    return df.withColumn("source", "function2")
""")
            
            generator = PythonTransformGenerator()
            output_dir = tmpdir_path / "generated"
            
            # Generate first action - should create custom_python_functions/
            action1 = Action(
                name="transform_customers",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_customers_transformed",
                source="v_customers",
                module_path="transforms1/function1.py",
                function_name="transform_customers"
            )
            
            context1 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="flowgroup1",
                    actions=[]
                )
            }
            
            code1 = generator.generate(action1, context1)
            
            # Verify first function was copied
            custom_functions_dir = output_dir / "custom_python_functions"
            assert custom_functions_dir.exists(), "custom_python_functions directory should exist"
            assert (custom_functions_dir / "__init__.py").exists(), "__init__.py should exist"
            assert (custom_functions_dir / "function1.py").exists(), "function1.py should be copied"
            
            # Store content of first function
            function1_content = (custom_functions_dir / "function1.py").read_text()
            
            # Generate second action - should merge with existing directory
            action2 = Action(
                name="transform_orders",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_orders_transformed",
                source="v_orders",
                module_path="transforms2/function2.py",
                function_name="transform_orders"
            )
            
            context2 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="flowgroup2",
                    actions=[]
                )
            }
            
            code2 = generator.generate(action2, context2)
            
            # Verify merge behavior - both files should exist
            assert (custom_functions_dir / "function1.py").exists(), "function1.py should still exist after merge"
            assert (custom_functions_dir / "function2.py").exists(), "function2.py should be added during merge"
            assert (custom_functions_dir / "__init__.py").exists(), "__init__.py should still exist"
            
            # Verify first function wasn't overwritten
            assert (custom_functions_dir / "function1.py").read_text() == function1_content, "function1.py content should be unchanged"
            
            # Verify second function has expected content  
            function2_content = (custom_functions_dir / "function2.py").read_text()
            assert "transform_orders" in function2_content, "function2.py should contain transform_orders function"
            assert "WARNING" in function2_content, "function2.py should have warning header"
            
            # Verify function calls are generated correctly
            assert "transform_customers(v_customers_df, spark, parameters)" in code1
            assert "transform_orders(v_orders_df, spark, parameters)" in code2
            
            # Verify that the generator added the correct imports (check import list)
            assert "from custom_python_functions.function1 import transform_customers" in generator.imports
            
            # Create new generator instance for second action to check its imports
            generator2 = PythonTransformGenerator()
            code2_new = generator2.generate(action2, context2)
            assert "from custom_python_functions.function2 import transform_orders" in generator2.imports
    
    def test_temp_table_generator(self):
        """Test temporary table generator."""
        generator = TempTableTransformGenerator()
        action = Action(
            name="staging_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.TEMP_TABLE,
            target="customers_staging",
            source={
                "source": "v_customers_enriched",
                "comment": "Staging table for customers"
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code uses correct pattern
        assert "@dlt.table(" in code
        assert "temporary=True" in code
        assert "def customers_staging():" in code
        # Verify it does NOT use the old incorrect pattern
        assert "dlt.create_streaming_table" not in code
        assert "customers_staging_temp" not in code

    def test_python_transform_nested_paths(self):
        """Test Python files in nested subdirectories (transformations/customer/cleaner.py)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create deeply nested Python function
            nested_dir = tmpdir_path / "transformations" / "customer" / "advanced"
            nested_dir.mkdir(parents=True)
            nested_file = nested_dir / "cleaner.py"
            nested_file.write_text("""
def clean_customer_data(df, spark, parameters):
    # Advanced customer data cleaning
    return df.filter("email IS NOT NULL").dropDuplicates(["customer_id"])

def validate_customer_data(df, spark, parameters):
    # Customer data validation
    return df.filter("customer_id > 0")
""")
            
            # Also create a simple nested structure for comparison
            simple_nested_dir = tmpdir_path / "utils" / "data"
            simple_nested_dir.mkdir(parents=True)
            simple_nested_file = simple_nested_dir / "processor.py"
            simple_nested_file.write_text("""
def process_data(df, spark, parameters):
    return df.withColumn("processed", "true")
""")
            
            generator = PythonTransformGenerator()
            output_dir = tmpdir_path / "generated"
            
            # Test deeply nested path
            action1 = Action(
                name="clean_customers",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_customers_clean",
                source="v_customers_raw",
                module_path="transformations/customer/advanced/cleaner.py",
                function_name="clean_customer_data"
            )
            
            context1 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="customer_processing",
                    actions=[]
                )
            }
            
            code1 = generator.generate(action1, context1)
            
            # Verify nested file was copied correctly
            custom_functions_dir = output_dir / "custom_python_functions"
            copied_file = custom_functions_dir / "cleaner.py"
            assert copied_file.exists(), "Nested file should be copied to custom_python_functions/"
            
            # Verify copied file content
            copied_content = copied_file.read_text()
            assert "clean_customer_data" in copied_content, "Copied file should contain the function"
            assert "validate_customer_data" in copied_content, "Copied file should contain all functions"
            assert "WARNING" in copied_content, "Copied file should have warning header"
            assert "transformations/customer/advanced/cleaner.py" in copied_content, "Warning should show original path"
            
            # Verify generated code uses correct function name
            assert "clean_customer_data(v_customers_raw_df, spark, parameters)" in code1
            
            # Test simple nested path
            generator2 = PythonTransformGenerator()
            action2 = Action(
                name="process_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_data_processed",
                source="v_data_raw",
                module_path="utils/data/processor.py",
                function_name="process_data"
            )
            
            context2 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="data_processing",
                    actions=[]
                )
            }
            
            code2 = generator2.generate(action2, context2)
            
            # Verify second nested file was copied correctly
            processor_file = custom_functions_dir / "processor.py"
            assert processor_file.exists(), "Second nested file should be copied"
            
            # Verify both files coexist
            assert copied_file.exists(), "First nested file should still exist"
            assert processor_file.exists(), "Second nested file should exist"
            
            # Verify imports are correct for nested paths
            assert "from custom_python_functions.cleaner import clean_customer_data" in generator.imports
            assert "from custom_python_functions.processor import process_data" in generator2.imports
            
            # Verify module names are extracted correctly (should be file stem, not full path)
            processor_content = processor_file.read_text()
            assert "utils/data/processor.py" in processor_content, "Warning should show original nested path"

    def test_python_transform_module_naming_same_pipeline(self):
        """Test module naming conflicts within same pipeline - should add directory prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create two Python files with the same name but in different directories
            # First: transformations/cleaner.py
            transforms_dir = tmpdir_path / "transformations"
            transforms_dir.mkdir(parents=True)
            (transforms_dir / "cleaner.py").write_text("""
def clean_data(df, spark, parameters):
    return df.filter("status = 'active'")
""")
            
            # Second: utils/cleaner.py (same filename, different directory)
            utils_dir = tmpdir_path / "utils"
            utils_dir.mkdir(parents=True)
            (utils_dir / "cleaner.py").write_text("""
def clean_data(df, spark, parameters):
    return df.dropDuplicates()
""")
            
            # Third: data/processing/cleaner.py (same filename, nested directory)
            nested_dir = tmpdir_path / "data" / "processing"
            nested_dir.mkdir(parents=True)
            (nested_dir / "cleaner.py").write_text("""
def clean_data(df, spark, parameters):
    return df.fillna("unknown")
""")
            
            output_dir = tmpdir_path / "generated"
            
            # Generate first action - should use base name
            generator1 = PythonTransformGenerator()
            action1 = Action(
                name="clean_transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_clean_data",
                source="v_raw_data",
                module_path="transformations/cleaner.py",
                function_name="clean_data"
            )
            
            context1 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",  # Same pipeline
                    flowgroup="flowgroup1",
                    actions=[]
                )
            }
            
            code1 = generator1.generate(action1, context1)
            
            # Verify first file uses base name (no conflict yet)
            custom_functions_dir = output_dir / "custom_python_functions"
            assert (custom_functions_dir / "cleaner.py").exists(), "First file should use base name"
            
            # Generate second action - should add prefix due to conflict
            generator2 = PythonTransformGenerator()
            action2 = Action(
                name="clean_util_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_util_clean",
                source="v_raw_util",
                module_path="utils/cleaner.py",
                function_name="clean_data"
            )
            
            context2 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",  # Same pipeline - should cause conflict
                    flowgroup="flowgroup2",
                    actions=[]
                )
            }
            
            code2 = generator2.generate(action2, context2)
            
            # Verify second file uses prefixed name
            assert (custom_functions_dir / "cleaner.py").exists(), "Original file should still exist"
            assert (custom_functions_dir / "utils_cleaner.py").exists(), "Second file should use directory prefix"
            
            # Generate third action - should also add prefix
            generator3 = PythonTransformGenerator()
            action3 = Action(
                name="clean_nested_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_nested_clean",
                source="v_raw_nested",
                module_path="data/processing/cleaner.py",
                function_name="clean_data"
            )
            
            context3 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",  # Same pipeline
                    flowgroup="flowgroup3",
                    actions=[]
                )
            }
            
            code3 = generator3.generate(action3, context3)
            
            # Verify third file uses nested directory prefix
            assert (custom_functions_dir / "data_processing_cleaner.py").exists(), "Third file should use nested prefix"
            
            # Verify all three files coexist
            assert (custom_functions_dir / "cleaner.py").exists()
            assert (custom_functions_dir / "utils_cleaner.py").exists()
            assert (custom_functions_dir / "data_processing_cleaner.py").exists()
            
            # Verify imports use correct module names
            assert "from custom_python_functions.cleaner import clean_data" in generator1.imports
            assert "from custom_python_functions.utils_cleaner import clean_data" in generator2.imports
            assert "from custom_python_functions.data_processing_cleaner import clean_data" in generator3.imports
            
            # Verify file contents have different function implementations
            content1 = (custom_functions_dir / "cleaner.py").read_text()
            content2 = (custom_functions_dir / "utils_cleaner.py").read_text()
            content3 = (custom_functions_dir / "data_processing_cleaner.py").read_text()
            
            assert "status = 'active'" in content1, "First file should have filter by status"
            assert "dropDuplicates" in content2, "Second file should have dropDuplicates"
            assert "fillna" in content3, "Third file should have fillna"

    def test_python_transform_module_naming_different_pipelines(self):
        """Test module naming conflicts across different pipelines - should be valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create two Python files with the same name
            # First: transformations/processor.py for pipeline1
            transforms_dir = tmpdir_path / "transformations"
            transforms_dir.mkdir(parents=True)
            (transforms_dir / "processor.py").write_text("""
def process_orders(df, spark, parameters):
    return df.withColumn("processed_by", "pipeline1")
""")
            
            # Second: utils/processor.py for pipeline2
            utils_dir = tmpdir_path / "utils"
            utils_dir.mkdir(parents=True)
            (utils_dir / "processor.py").write_text("""
def process_customers(df, spark, parameters):
    return df.withColumn("processed_by", "pipeline2")
""")
            
            # Generate first action for pipeline1
            generator1 = PythonTransformGenerator()
            action1 = Action(
                name="process_orders",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_orders_processed",
                source="v_orders_raw",
                module_path="transformations/processor.py",
                function_name="process_orders"
            )
            
            output_dir1 = tmpdir_path / "generated" / "pipeline1"
            context1 = {
                "output_dir": output_dir1,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="pipeline1",  # Different pipeline
                    flowgroup="orders_processing",
                    actions=[]
                )
            }
            
            code1 = generator1.generate(action1, context1)
            
            # Generate second action for pipeline2
            generator2 = PythonTransformGenerator()
            action2 = Action(
                name="process_customers",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_customers_processed",
                source="v_customers_raw",
                module_path="utils/processor.py",
                function_name="process_customers"
            )
            
            output_dir2 = tmpdir_path / "generated" / "pipeline2"
            context2 = {
                "output_dir": output_dir2,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="pipeline2",  # Different pipeline
                    flowgroup="customers_processing",
                    actions=[]
                )
            }
            
            code2 = generator2.generate(action2, context2)
            
            # Verify both files use base name (no conflict across different pipelines)
            custom_functions_dir1 = output_dir1 / "custom_python_functions"
            custom_functions_dir2 = output_dir2 / "custom_python_functions"
            
            assert (custom_functions_dir1 / "processor.py").exists(), "Pipeline1 should use base name"
            assert (custom_functions_dir2 / "processor.py").exists(), "Pipeline2 should use base name"
            
            # Verify no prefixed versions were created (since different pipelines)
            pipeline1_files = list(custom_functions_dir1.glob("*processor*.py"))
            pipeline2_files = list(custom_functions_dir2.glob("*processor*.py"))
            
            assert len(pipeline1_files) == 1, "Pipeline1 should have only one processor file"
            assert len(pipeline2_files) == 1, "Pipeline2 should have only one processor file"
            assert pipeline1_files[0].name == "processor.py", "Pipeline1 file should use base name"
            assert pipeline2_files[0].name == "processor.py", "Pipeline2 file should use base name"
            
            # Verify file contents are different (from different source files)
            content1 = (custom_functions_dir1 / "processor.py").read_text()
            content2 = (custom_functions_dir2 / "processor.py").read_text()
            
            assert "process_orders" in content1, "Pipeline1 should have process_orders function"
            assert "pipeline1" in content1, "Pipeline1 should have pipeline1 identifier"
            assert "process_customers" in content2, "Pipeline2 should have process_customers function"
            assert "pipeline2" in content2, "Pipeline2 should have pipeline2 identifier"
            
            # Verify imports use base name for both pipelines
            assert "from custom_python_functions.processor import process_orders" in generator1.imports
            assert "from custom_python_functions.processor import process_customers" in generator2.imports
            
            # Verify function calls in generated code
            assert "process_orders(v_orders_raw_df, spark, parameters)" in code1
            assert "process_customers(v_customers_raw_df, spark, parameters)" in code2
            
            # Verify warning headers show correct original paths
            assert "transformations/processor.py" in content1, "Pipeline1 warning should show transformations path"
            assert "utils/processor.py" in content2, "Pipeline2 warning should show utils path"

    def test_python_transform_import_generation_syntax(self):
        """Test that generated imports are syntactically correct and importable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Test various module and function name combinations
            test_cases = [
                # (module_path, function_name, expected_module_name)
                ("transformations/data_cleaner.py", "clean_data", "data_cleaner"),
                ("utils/data_processor.py", "process_data", "data_processor"),
                ("advanced/ml/model_trainer.py", "train_model", "model_trainer"),  # Nested path (no conflict)
                ("simple_transform.py", "transform", "simple_transform"),  # Root level file
                ("validators/email_validator.py", "validate_email", "email_validator"),
            ]
            
            generator = PythonTransformGenerator()
            output_dir = tmpdir_path / "generated"
            
            # Create Python files for test cases
            for i, (module_path, function_name, expected_module) in enumerate(test_cases):
                # Create directory structure
                file_path = tmpdir_path / module_path
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Create Python file with function
                file_path.write_text(f"""
def {function_name}(df, spark, parameters):
    return df.withColumn("test_case", "{i}")

def helper_function():
    pass
""")
                
                # Generate action
                action = Action(
                    name=f"test_action_{i}",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.PYTHON,
                    target=f"v_output_{i}",
                    source=f"v_input_{i}",
                    module_path=module_path,
                    function_name=function_name
                )
                
                context = {
                    "output_dir": output_dir,
                    "spec_dir": tmpdir_path,
                    "flowgroup": FlowGroup(
                        pipeline="test_pipeline",
                        flowgroup=f"flowgroup_{i}",
                        actions=[]
                    )
                }
                
                code = generator.generate(action, context)
                
                # Test import syntax
                expected_import = f"from custom_python_functions.{expected_module} import {function_name}"
                assert expected_import in generator.imports, f"Expected import '{expected_import}' not found in generator imports"
                
                # Test that import is syntactically valid Python
                try:
                    compile(expected_import, '<string>', 'exec')
                except SyntaxError as e:
                    assert False, f"Generated import '{expected_import}' has syntax error: {e}"
                
                # Test function call in generated code
                expected_call = f"{function_name}(v_input_{i}_df, spark, parameters)"
                assert expected_call in code, f"Expected function call '{expected_call}' not found in generated code"
            
            # Test conflict resolution by creating two files with same name
            conflict_dir1 = tmpdir_path / "dir1"
            conflict_dir2 = tmpdir_path / "dir2"
            conflict_dir1.mkdir(parents=True)
            conflict_dir2.mkdir(parents=True)
            
            # Create two files with same name but different content
            (conflict_dir1 / "processor.py").write_text("""
def process(df, spark, parameters):
    return df.withColumn("source", "dir1")
""")
            (conflict_dir2 / "processor.py").write_text("""
def process(df, spark, parameters):
    return df.withColumn("source", "dir2")
""")
            
            # Generate first action (should use base name)
            action_conflict1 = Action(
                name="conflict_test_1",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_conflict_1",
                source="v_input_conflict_1",
                module_path="dir1/processor.py",
                function_name="process"
            )
            
            context_conflict1 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="conflict_flowgroup_1",
                    actions=[]
                )
            }
            
            code_conflict1 = generator.generate(action_conflict1, context_conflict1)
            
            # Generate second action (should trigger conflict resolution and use prefix)
            action_conflict2 = Action(
                name="conflict_test_2",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_conflict_2",
                source="v_input_conflict_2",
                module_path="dir2/processor.py",
                function_name="process"
            )
            
            context_conflict2 = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",  # Same pipeline - should cause conflict
                    flowgroup="conflict_flowgroup_2",
                    actions=[]
                )
            }
            
            code_conflict2 = generator.generate(action_conflict2, context_conflict2)
            
            # Verify conflict resolution imports
            assert "from custom_python_functions.processor import process" in generator.imports
            assert "from custom_python_functions.dir2_processor import process" in generator.imports
            
            # Test special characters and edge cases
            edge_cases_dir = tmpdir_path / "edge_cases"
            edge_cases_dir.mkdir(parents=True)
            
            # Test with numbers and underscores
            (edge_cases_dir / "data_v2_processor.py").write_text("""
def process_data_v2(df, spark, parameters):
    return df.withColumn("version", "2")
""")
            
            generator_edge = PythonTransformGenerator()
            action_edge = Action(
                name="edge_test",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_edge_output",
                source="v_edge_input",
                module_path="edge_cases/data_v2_processor.py",
                function_name="process_data_v2"
            )
            
            context_edge = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="edge_flowgroup",
                    actions=[]
                )
            }
            
            code_edge = generator_edge.generate(action_edge, context_edge)
            
            # Verify edge case import
            edge_import = "from custom_python_functions.data_v2_processor import process_data_v2"
            assert edge_import in generator_edge.imports
            
            # Test that all generated imports are unique and valid
            all_imports = set()
            for test_gen in [generator, generator_edge]:
                for imp in test_gen.imports:
                    if "custom_python_functions" in imp:
                        assert imp not in all_imports, f"Duplicate import found: {imp}"
                        all_imports.add(imp)
                        
                        # Test syntax validity
                        try:
                            compile(imp, '<string>', 'exec')
                        except SyntaxError as e:
                            assert False, f"Invalid import syntax: {imp} - {e}"
            
            # Verify copied files exist and are importable
            custom_functions_dir = output_dir / "custom_python_functions"
            assert (custom_functions_dir / "__init__.py").exists(), "__init__.py should exist"
            
            # Verify that Python package structure is correct
            init_content = (custom_functions_dir / "__init__.py").read_text()
            assert "Generated package for custom Python functions" in init_content
            
            # Test that module names don't conflict with Python keywords
            python_keywords = ['def', 'class', 'import', 'from', 'if', 'else', 'for', 'while', 'try', 'except']
            for imp in all_imports:
                for keyword in python_keywords:
                    assert f".{keyword} " not in imp, f"Import uses Python keyword: {imp}"
                    assert f"_{keyword}_" not in imp, f"Import contains Python keyword: {imp}"

    def test_python_transform_multiple_source_views(self):
        """Test Python transforms with multiple input DataFrames (list of sources)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create Python function that handles multiple dataframes
            transforms_dir = tmpdir_path / "transformations"
            transforms_dir.mkdir(parents=True)
            (transforms_dir / "multi_transformer.py").write_text("""
def join_customer_orders(dataframes, spark, parameters):
    customers_df, orders_df = dataframes
    join_type = parameters.get("join_type", "inner")
    return customers_df.join(orders_df, "customer_id", join_type)

def merge_data_sources(dataframes, spark, parameters):
    df1, df2, df3 = dataframes
    # Union all dataframes
    result = df1
    for df in dataframes[1:]:
        result = result.union(df)
    return result.distinct()
""")
            
            generator = PythonTransformGenerator()
            output_dir = tmpdir_path / "generated"
            
            # Test with 2 source views
            action_two_sources = Action(
                name="join_customer_orders",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_customer_orders",
                source=["v_customers", "v_orders"],  # Multiple sources as list
                module_path="transformations/multi_transformer.py",
                function_name="join_customer_orders",
                parameters={"join_type": "left"}
            )
            
            context = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="multi_source_test",
                    actions=[]
                )
            }
            
            code_two = generator.generate(action_two_sources, context)
            
            # Verify generated code for 2 sources
            assert "v_customers_df = spark.read.table(\"v_customers\")" in code_two
            assert "v_orders_df = spark.read.table(\"v_orders\")" in code_two
            assert "dataframes = [v_customers_df, v_orders_df]" in code_two
            assert "df = join_customer_orders(dataframes, spark, parameters)" in code_two
            assert "return df" in code_two
            
            # Verify parameters are properly formatted
            assert '"join_type": "left"' in code_two
            
            # Test with 3 source views
            generator2 = PythonTransformGenerator()
            action_three_sources = Action(
                name="merge_data_sources",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_merged_data",
                source=["v_source1", "v_source2", "v_source3"],  # Three sources
                module_path="transformations/multi_transformer.py",
                function_name="merge_data_sources",
                parameters={"operation": "union"}
            )
            
            code_three = generator2.generate(action_three_sources, context)
            
            # Verify generated code for 3 sources
            assert "v_source1_df = spark.read.table(\"v_source1\")" in code_three
            assert "v_source2_df = spark.read.table(\"v_source2\")" in code_three
            assert "v_source3_df = spark.read.table(\"v_source3\")" in code_three
            assert "dataframes = [v_source1_df, v_source2_df, v_source3_df]" in code_three
            assert "df = merge_data_sources(dataframes, spark, parameters)" in code_three
            
            # Test with stream mode
            action_stream = Action(
                name="stream_join",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_stream_result",
                source=["v_stream1", "v_stream2"],
                module_path="transformations/multi_transformer.py",
                function_name="join_customer_orders",
                readMode="stream"
            )
            
            generator3 = PythonTransformGenerator()
            code_stream = generator3.generate(action_stream, context)
            
            # Verify stream mode uses readStream
            assert "v_stream1_df = spark.readStream.table(\"v_stream1\")" in code_stream
            assert "v_stream2_df = spark.readStream.table(\"v_stream2\")" in code_stream
            assert "dataframes = [v_stream1_df, v_stream2_df]" in code_stream
            
            # Verify imports are generated correctly
            assert "from custom_python_functions.multi_transformer import join_customer_orders" in generator.imports
            assert "from custom_python_functions.multi_transformer import merge_data_sources" in generator2.imports
            
            # Verify copied files
            custom_functions_dir = output_dir / "custom_python_functions"
            copied_file = custom_functions_dir / "multi_transformer.py"
            assert copied_file.exists()
            
            # Verify copied file content includes both functions
            copied_content = copied_file.read_text()
            assert "join_customer_orders" in copied_content
            assert "merge_data_sources" in copied_content
            assert "WARNING" in copied_content  # Should have warning header

    def test_python_transform_no_source_views(self):
        """Test Python transforms that generate data (no source views)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create Python function that generates data
            generators_dir = tmpdir_path / "generators"
            generators_dir.mkdir(parents=True)
            (generators_dir / "data_generator.py").write_text("""
def generate_reference_data(spark, parameters):
    from pyspark.sql import Row
    
    data_type = parameters.get("data_type", "lookup")
    
    if data_type == "lookup":
        data = [
            Row(id=1, name="Category A", active=True),
            Row(id=2, name="Category B", active=True),
            Row(id=3, name="Category C", active=False),
        ]
    else:
        data = [
            Row(id=1, value="Default Value"),
        ]
    
    return spark.createDataFrame(data)

def generate_test_data(spark, parameters):
    count = parameters.get("count", 100)
    from pyspark.sql import functions as F
    
    # Generate test data
    df = spark.range(count).select(
        F.col("id").alias("test_id"),
        F.lit("test_value").alias("value"),
        F.current_timestamp().alias("created_at")
    )
    return df
""")
            
            generator = PythonTransformGenerator()
            output_dir = tmpdir_path / "generated"
            
            # Test data generator with empty list source
            action_empty_list = Action(
                name="generate_reference_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_reference_data",
                source=[],  # Empty list - no source views
                module_path="generators/data_generator.py",
                function_name="generate_reference_data",
                parameters={"data_type": "lookup"}
            )
            
            context = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="data_generation",
                    actions=[]
                )
            }
            
            code_empty = generator.generate(action_empty_list, context)
            
            # Verify generated code for no sources
            assert "# No source views - function generates data" in code_empty
            assert "parameters = " in code_empty
            assert "df = generate_reference_data(spark, parameters)" in code_empty
            assert "return df" in code_empty
            
            # Should not have any source view loading
            assert "spark.read.table" not in code_empty
            assert "spark.readStream.table" not in code_empty
            assert "dataframes = " not in code_empty
            
            # Verify parameters are formatted correctly
            assert '"data_type": "lookup"' in code_empty
            
            # Test another data generator with different parameters
            generator2 = PythonTransformGenerator()
            action_test_data = Action(
                name="generate_test_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_test_data",
                source=[],  # No sources
                module_path="generators/data_generator.py",
                function_name="generate_test_data",
                parameters={"count": 1000}
            )
            
            code_test = generator2.generate(action_test_data, context)
            
            # Verify test data generator
            assert "# No source views - function generates data" in code_test
            assert "df = generate_test_data(spark, parameters)" in code_test
            assert '"count": 1000' in code_test
            
            # Test with operational metadata
            generator3 = PythonTransformGenerator()
            action_with_metadata = Action(
                name="generate_with_metadata",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_generated_with_metadata",
                source=[],
                module_path="generators/data_generator.py",
                function_name="generate_reference_data",
                operational_metadata=["_pipeline_name"]
            )
            
            code_metadata = generator3.generate(action_with_metadata, context)
            
            # Verify operational metadata is added
            assert "# Add operational metadata columns" in code_metadata
            assert "df = df.withColumn('_pipeline_name'" in code_metadata
            
            # Verify imports are generated correctly
            assert "from custom_python_functions.data_generator import generate_reference_data" in generator.imports
            assert "from custom_python_functions.data_generator import generate_test_data" in generator2.imports
            
            # Verify copied file exists and contains both functions
            custom_functions_dir = output_dir / "custom_python_functions"
            copied_file = custom_functions_dir / "data_generator.py"
            assert copied_file.exists()
            
            copied_content = copied_file.read_text()
            assert "generate_reference_data" in copied_content
            assert "generate_test_data" in copied_content
            assert "WARNING" in copied_content

    def test_python_transform_end_to_end_comprehensive(self):
        """Test complete end-to-end Python transform workflow with all features."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create comprehensive Python functions that demonstrate all features
            functions_dir = tmpdir_path / "transformations"
            functions_dir.mkdir(parents=True)
            
            # Single source function
            (functions_dir / "customer_transformer.py").write_text("""
def transform_customers(df, spark, parameters):
    threshold = parameters.get("threshold", 100)
    return df.filter(f"amount > {threshold}").withColumn("processed", "single_source")

def validate_customers(df, spark, parameters):
    return df.filter("email IS NOT NULL AND customer_id > 0")
""")
            
            # Multi-source function
            (functions_dir / "order_processor.py").write_text("""
def join_orders_customers(dataframes, spark, parameters):
    orders_df, customers_df = dataframes
    join_type = parameters.get("join_type", "inner")
    return orders_df.join(customers_df, "customer_id", join_type)
""")
            
            # Data generator function  
            generators_dir = tmpdir_path / "generators"
            generators_dir.mkdir(parents=True)
            (generators_dir / "reference_data.py").write_text("""
def generate_lookup_data(spark, parameters):
    from pyspark.sql import Row
    data = [Row(id=1, name="Premium"), Row(id=2, name="Standard")]
    return spark.createDataFrame(data)
""")
            
            # Conflict testing - same name different directories
            utils_dir = tmpdir_path / "utils"
            utils_dir.mkdir(parents=True)
            (utils_dir / "customer_transformer.py").write_text("""
def transform_customers(df, spark, parameters):
    return df.withColumn("processed", "utils_version")
""")
            
            generator = PythonTransformGenerator()
            output_dir = tmpdir_path / "generated"
            
            # Test 1: Single source with operational metadata
            action1 = Action(
                name="transform_customers",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_customers_transformed",
                source="v_customers_raw",
                module_path="transformations/customer_transformer.py",
                function_name="transform_customers",
                parameters={"threshold": 500},
                operational_metadata=["_pipeline_name"],
                readMode="stream"
            )
            
            context = {
                "output_dir": output_dir,
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="comprehensive_test_pipeline",
                    flowgroup="customer_processing",
                    actions=[]
                )
            }
            
            code1 = generator.generate(action1, context)
            
            # Verify single source with stream mode and metadata
            assert "v_customers_raw_df = spark.readStream.table(\"v_customers_raw\")" in code1
            assert "df = transform_customers(v_customers_raw_df, spark, parameters)" in code1
            assert '"threshold": 500' in code1
            assert "df = df.withColumn('_pipeline_name'" in code1
            assert "return df" in code1
            
            # Test 2: Multiple sources with different parameters
            generator2 = PythonTransformGenerator()
            action2 = Action(
                name="join_orders_customers",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_orders_enriched",
                source=["v_orders", "v_customers_transformed"],
                module_path="transformations/order_processor.py",
                function_name="join_orders_customers",
                parameters={"join_type": "left"}
            )
            
            code2 = generator2.generate(action2, context)
            
            # Verify multiple sources
            assert "v_orders_df = spark.read.table(\"v_orders\")" in code2
            assert "v_customers_transformed_df = spark.read.table(\"v_customers_transformed\")" in code2
            assert "dataframes = [v_orders_df, v_customers_transformed_df]" in code2
            assert "df = join_orders_customers(dataframes, spark, parameters)" in code2
            assert '"join_type": "left"' in code2
            
            # Test 3: Data generator (no sources)
            generator3 = PythonTransformGenerator()
            action3 = Action(
                name="generate_lookup",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_lookup_data",
                source=[],
                module_path="generators/reference_data.py",
                function_name="generate_lookup_data"
            )
            
            code3 = generator3.generate(action3, context)
            
            # Verify data generator
            assert "# No source views - function generates data" in code3
            assert "df = generate_lookup_data(spark, parameters)" in code3
            assert "spark.read.table" not in code3
            
            # Test 4: Conflict resolution (same pipeline, same filename)
            generator4 = PythonTransformGenerator()
            action4 = Action(
                name="transform_customers_utils",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                target="v_customers_utils_transformed",
                source="v_customers_raw",
                module_path="utils/customer_transformer.py",
                function_name="transform_customers"
            )
            
            code4 = generator4.generate(action4, context)
            
            # Verify all files were created and tracked
            custom_functions_dir = output_dir / "custom_python_functions"
            assert (custom_functions_dir / "__init__.py").exists()
            assert (custom_functions_dir / "customer_transformer.py").exists()  # First one
            assert (custom_functions_dir / "order_processor.py").exists()
            assert (custom_functions_dir / "reference_data.py").exists()
            assert (custom_functions_dir / "utils_customer_transformer.py").exists()  # Prefixed
            
            # Verify imports are correct
            assert "from custom_python_functions.customer_transformer import transform_customers" in generator.imports
            assert "from custom_python_functions.order_processor import join_orders_customers" in generator2.imports
            assert "from custom_python_functions.reference_data import generate_lookup_data" in generator3.imports
            assert "from custom_python_functions.utils_customer_transformer import transform_customers" in generator4.imports
            
            # Verify all files have warning headers
            for file_path in [
                custom_functions_dir / "customer_transformer.py",
                custom_functions_dir / "order_processor.py", 
                custom_functions_dir / "reference_data.py",
                custom_functions_dir / "utils_customer_transformer.py"
            ]:
                content = file_path.read_text()
                assert "WARNING" in content
                assert "DO NOT EDIT THIS FILE DIRECTLY" in content
                
            # Verify conflict resolution worked correctly
            utils_content = (custom_functions_dir / "utils_customer_transformer.py").read_text()
            assert "utils_version" in utils_content
            
            regular_content = (custom_functions_dir / "customer_transformer.py").read_text()
            assert "single_source" in regular_content
            
            # Verify different function signatures are handled correctly
            assert "transform_customers(v_customers_raw_df, spark, parameters)" in code1  # Single source
            assert "join_orders_customers(dataframes, spark, parameters)" in code2  # Multiple sources  
            assert "generate_lookup_data(spark, parameters)" in code3  # No sources
            
            # Verify package structure
            init_content = (custom_functions_dir / "__init__.py").read_text()
            assert "Generated package for custom Python functions" in init_content


def test_transform_generator_imports():
    """Test that transform generators manage imports correctly."""
    # Transform generator with additional imports
    schema_gen = SchemaTransformGenerator()
    assert "import dlt" in schema_gen.imports
    assert "from pyspark.sql import functions as F" in schema_gen.imports
    assert "from pyspark.sql.types import StructType" in schema_gen.imports


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 