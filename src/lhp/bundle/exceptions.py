"""Compatibility shim — bundle exceptions are now defined in :mod:`lhp.errors`.

This shim allows existing ``from lhp.bundle.exceptions import …`` callers
to keep resolving until D9 removes them. New code should import from
:mod:`lhp.errors`.

# DELETE IN PHASE D9 once callers are migrated.
"""

from lhp.errors import (
    BundleConfigurationError,
    BundleResourceError,
    TemplateError,
    YAMLProcessingError,
)

__all__ = [
    "BundleConfigurationError",
    "BundleResourceError",
    "TemplateError",
    "YAMLProcessingError",
]
