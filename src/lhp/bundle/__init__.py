"""
LHP Databricks Asset Bundle integration module.

This module provides functionality for integrating LHP with Databricks Asset Bundles,
including bundle detection, template fetching, and resource file management.
"""

__version__ = "1.0.0"
__author__ = "LHP Development Team"

# Re-export main classes for convenience
try:
    from .manager import BundleManager
    from .exceptions import BundleResourceError
    
    __all__ = [
        "BundleManager",
        "BundleResourceError",
    ]
except ImportError:
    # During development, some modules may not exist yet
    __all__ = [] 