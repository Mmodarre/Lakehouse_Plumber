"""Test CDC mode table creation fix."""

import pytest
from pathlib import Path
import tempfile

from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models.config import Action, ActionType

def test_cdc_mode_creates_table_and_flow():
    """Test that CDC mode generates both table creation and CDC flow."""
    
    # Create a CDC write action
    action = Action(
        name="write_customer_scd",
        type=ActionType.WRITE,
        source={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "dim_customer",
            "view": "v_customer_cleansed",
            "cdc_config": {
                "keys": ["customer_id"],
                "sequence_by": "_commit_timestamp",
                "scd_type": 2,
                "track_history_columns": ["name", "address", "phone"]
            }
        }
    )
    
    # Generate code
    generator = StreamingTableWriteGenerator()
    context = {
        "preset_config": {
            "defaults": {
                "write_actions": {
                    "streaming_table": {
                        "table_properties": {
                            "delta.enableChangeDataFeed": "true",
                            "quality": "silver"
                        }
                    }
                }
            }
        }
    }
    
    code = generator.generate(action, context)
    
    # Verify both table creation and CDC flow are present
    assert "dlt.create_streaming_table(" in code
    assert 'name="catalog.schema.dim_customer"' in code
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'target="catalog.schema.dim_customer"' in code
    assert 'keys=["customer_id"]' in code
    assert "stored_as_scd_type=2" in code
    assert 'track_history_column_list=["name", "address", "phone"]' in code
    
    # Ensure table is created before CDC flow
    table_pos = code.find("dlt.create_streaming_table(")
    cdc_pos = code.find("dlt.create_auto_cdc_flow(")
    assert table_pos < cdc_pos, "Table must be created before CDC flow" 
 