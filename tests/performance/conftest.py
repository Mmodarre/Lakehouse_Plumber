"""Opt-in guard for the release performance gate.

Skips any test marked ``@pytest.mark.performance`` unless the caller's ``-m``
expression mentions ``performance`` (i.e. they explicitly selected or
deselected the marker). Without this guard, the gate would silently run for
~13 minutes on any plain ``pytest tests/``, ``pytest tests/ -m "not slow"``,
or CI invocation whenever the heavy fixtures happen to be present.

Examples::

    pytest tests/                              # gate skipped
    pytest tests/ -m "not slow"                # gate skipped
    pytest tests/performance/                  # gate skipped, unit tests run
    pytest tests/performance/ -m performance   # gate runs
    pytest tests/performance/ -m "not performance"  # gate skipped (normal marker filter)
"""

import pytest


def pytest_collection_modifyitems(config, items):
    markexpr = config.option.markexpr or ""
    if "performance" in markexpr:
        return
    skip_perf = pytest.mark.skip(
        reason=(
            "performance gate; opt in with "
            "`pytest tests/performance/ -m performance`"
        )
    )
    for item in items:
        if item.get_closest_marker("performance") is not None:
            item.add_marker(skip_perf)
