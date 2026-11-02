"""Public surface of the dependency-analysis sub-package."""

from ._producers import build_producer_indexes, match_produced_table
from ._read_modes import ProducerKind, producer_kind
from ._table_sites import PythonTableSite, PythonTableSitesResult, SourceSpan
from .analyzer import DependencyAnalyzer
from .builder import DependencyGraphBuilder
from .dependency_resolver import DependencyResolver
from .graph_cache import PersistentGraphCache
from .output import DependencyOutputFormatter
from .output_writer import DependencyOutputWriter
from .python_parser import collect_python_table_sites
from .service import DependencyAnalysisService

__all__ = [
    "DependencyAnalysisService",
    "DependencyAnalyzer",
    "DependencyGraphBuilder",
    "DependencyOutputFormatter",
    "DependencyOutputWriter",
    "DependencyResolver",
    "PersistentGraphCache",
    "ProducerKind",
    "PythonTableSite",
    "PythonTableSitesResult",
    "SourceSpan",
    "build_producer_indexes",
    "collect_python_table_sites",
    "match_produced_table",
    "producer_kind",
]
