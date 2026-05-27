"""
LHP Databricks Asset Bundle integration module.

This module provides functionality for integrating LHP with Databricks Asset Bundles,
including bundle detection, template fetching, and resource file management.

Note: the bundle exception classes (``BundleResourceError``,
``TemplateError``, ``YAMLProcessingError``, ``BundleConfigurationError``)
were reparented to :mod:`lhp.errors` in Phase D8. Import them from
``lhp.errors`` directly. The legacy ``from lhp.bundle.exceptions import ...``
path remains as a compatibility shim until Phase D9.
"""

__version__ = "1.0.0"
__author__ = "LHP Development Team"

# Re-export main classes for convenience
try:
    from .manager import BundleManager

    __all__ = [
        "BundleManager",
    ]
except ImportError:
    # During development, some modules may not exist yet
    __all__ = []
