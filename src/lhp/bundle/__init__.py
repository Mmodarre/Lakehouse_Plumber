"""Bundle exception classes now live in :mod:`lhp.errors`.

Import from ``lhp.errors`` directly. The legacy
``from lhp.bundle.exceptions import ...`` path remains as a compatibility shim.
"""

__version__ = "1.0.0"
__author__ = "LHP Development Team"

from .detection import is_databricks_yml_present, should_enable_bundle_support

try:
    from .manager import BundleManager

    __all__ = [
        "BundleManager",
        "is_databricks_yml_present",
        "should_enable_bundle_support",
    ]
except ImportError:
    __all__ = [
        "is_databricks_yml_present",
        "should_enable_bundle_support",
    ]
