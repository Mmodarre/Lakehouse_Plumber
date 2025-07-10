"""Write action generators."""

from .streaming_table import StreamingTableWriteGenerator
from .materialized_view import MaterializedViewWriteGenerator

__all__ = ["StreamingTableWriteGenerator", "MaterializedViewWriteGenerator"]
