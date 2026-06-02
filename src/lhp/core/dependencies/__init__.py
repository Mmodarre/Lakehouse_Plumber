"""Public surface of the dependency-analysis sub-package."""

from .analyzer import DependencyAnalyzer
from .builder import DependencyGraphBuilder
from .dependency_resolver import DependencyResolver
from .output import DependencyOutputManager
from .service import DependencyAnalysisService

__all__ = [
    "DependencyAnalysisService",
    "DependencyAnalyzer",
    "DependencyGraphBuilder",
    "DependencyOutputManager",
    "DependencyResolver",
]
