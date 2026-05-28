"""
LHP Databricks Asset Bundle integration module.

This module provides functionality for integrating LHP with Databricks Asset Bundles,
including bundle detection, template fetching, and resource file management.

The bundle exception classes (``BundleResourceError``, ``TemplateError``,
``YAMLProcessingError``, ``BundleConfigurationError``) now live in
:mod:`lhp.errors`. Import them from ``lhp.errors`` directly. The legacy
``from lhp.bundle.exceptions import ...`` path remains as a compatibility shim.
"""

__version__ = "1.0.0"
__author__ = "LHP Development Team"

# Re-export main classes for convenience
from .detection import is_databricks_yml_present, should_enable_bundle_support

try:
    from .manager import BundleManager

    __all__ = [
        "BundleManager",
        "is_databricks_yml_present",
        "should_enable_bundle_support",
    ]
except ImportError:
    # During development, some modules may not exist yet
    __all__ = [
        "is_databricks_yml_present",
        "should_enable_bundle_support",
    ]
