"""Picklable test fakes for collaborators that cross the worker boundary.

The :mod:`lhp.core.coordination.executor` workers run in a ``spawn`` child via
``ProcessPoolExecutor``. Any state captured for or returned from those workers
is serialised with :mod:`pickle`. ``unittest.mock.MagicMock`` cannot pickle
cleanly across that boundary, so tests that exercise worker entry points use
the concrete fakes defined in this package instead.

Fakes here are intentionally minimal: each implements exactly the
method/attribute surface that the production code under test reads, and no
more. Calibration:

* Fakes for collaborators owned by this codebase
  (``FlowgroupResolutionService``, ``EnhancedSubstitutionManager``, ...). Mocks are
  reserved for assertions where the call itself is the contract under test.
* No diagnostic logging from inside a fake; tests assert on observable state.
"""

from tests.fakes.processing import (
    CallRecord,
    FakeCodeGenerator,
    FakeFlowgroupResolutionService,
    FakeProjectConfig,
    FakeSubstitutionManager,
    FakeTemplate,
    FakeTemplateEngine,
)

__all__ = [
    "CallRecord",
    "FakeCodeGenerator",
    "FakeFlowgroupResolutionService",
    "FakeProjectConfig",
    "FakeSubstitutionManager",
    "FakeTemplate",
    "FakeTemplateEngine",
]
