"""Test for improved SQL transform generation."""

import pytest
from pathlib import Path
from lhp.generators.transform.sql import SQLTransformGenerator
from lhp.models.config import Action


def test_sql_transform_generates_clean_code():
    """Test that SQL transform generates clean code without unnecessary temp views."""
    generator = SQLTransformGenerator()
    
    # Create a SQL transform action
    action = Action(
        name="customer_metrics",
        type="transform",
        transform_type="sql",
        target="v_customer_metrics",
        source=["v_customers", "v_orders"],
        sql="""SELECT 
            c.customer_id, 
            COUNT(o.order_id) as order_count
        FROM v_customers c
        JOIN v_orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id""",
        description="Calculate customer metrics"
    )
    
    # Generate code
    context = {"spec_dir": Path(".")}
    code = generator.generate(action, context)
    
    # Verify the generated code
    assert "@dlt.view(comment=" in code
    assert "Calculate customer metrics" in code
    assert "df = spark.sql(" in code
    assert "return df" in code
    
    # Verify it doesn't contain the old unnecessary code
    assert "dlt.read(" not in code
    assert "createOrReplaceTempView" not in code
    assert "result_df =" not in code
    
    # Verify the SQL is included
    assert "SELECT" in code
    assert "FROM v_customers c" in code
    assert "JOIN v_orders o" in code

    # Should not have dlt.read() calls for reading views
    assert "spark.read.table(" not in code  # SQL transforms don't need to read tables explicitly
    assert "spark.readStream.table(" not in code


def test_sql_transform_with_default_description():
    """Test SQL transform without explicit description uses default."""
    generator = SQLTransformGenerator()
    
    action = Action(
        name="simple_transform",
        type="transform",
        transform_type="sql",
        target="v_simple",
        source="v_source",
        sql="SELECT * FROM v_source WHERE active = true"
    )
    
    context = {"spec_dir": Path(".")}
    code = generator.generate(action, context)
    
    # Should have a default description
    assert '@dlt.view(comment="SQL transform: simple_transform")' in code


def test_sql_transform_with_sql_file(tmp_path):
    """Test SQL transform loading from file."""
    generator = SQLTransformGenerator()
    
    # Create a SQL file
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("""
        SELECT customer_id, SUM(amount) as total
        FROM orders
        GROUP BY customer_id
    """)
    
    action = Action(
        name="from_file",
        type="transform",
        transform_type="sql",
        target="v_from_file",
        source="orders",
        sql_path=str(sql_file),
        description="Query from file"
    )
    
    context = {"spec_dir": tmp_path}
    code = generator.generate(action, context)
    
    # Verify the SQL from file is included
    assert "SELECT customer_id, SUM(amount) as total" in code
    assert "FROM orders" in code
    assert "GROUP BY customer_id" in code 