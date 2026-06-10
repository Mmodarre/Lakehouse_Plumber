"""Bundle exception classes now live in :mod:`lhp.errors`.

Import from ``lhp.errors`` directly. The legacy
``from lhp.bundle.exceptions import ...`` path remains as a compatibility shim.
"""

__version__ = "1.0.0"
__author__ = "LHP Development Team"

from typing import TYPE_CHECKING, Any

from .detection import is_databricks_yml_present, should_enable_bundle_support

if TYPE_CHECKING:
    from .manager import BundleManager

__all__ = [
    "BundleManager",
    "is_databricks_yml_present",
    "should_enable_bundle_support",
]


def __getattr__(name: str) -> Any:
    # Lazy: resolving BundleManager keeps lhp.core.codegen (+ jinja2) off the
    # import path until it is actually accessed. A broken manager surfaces on
    # access (as an AttributeError), not at package import — matching prior
    # try/except behaviour.
    if name == "BundleManager":
        try:
            from .manager import BundleManager
        except ImportError as e:
            raise AttributeError(name) from e
        return BundleManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
