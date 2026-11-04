"""``without_secret_recording``: substitute without leaving secret references.

Validation substitutes SQL bodies and files for its own checks; code
generation records their ``${secret:...}`` references when it renders them, so
the checks must not record them on the run's manager.
"""

from __future__ import annotations

import pytest

from lhp.core.processing import EnhancedSubstitutionManager, without_secret_recording

pytestmark = pytest.mark.unit


def _manager() -> EnhancedSubstitutionManager:
    manager = EnhancedSubstitutionManager(env="dev")
    manager.mappings["stage"] = "bronze"
    manager.default_secret_scope = "scope"
    return manager


def test_none_stays_none():
    assert without_secret_recording(None) is None


def test_the_copy_substitutes_like_the_manager_without_recording_secrets():
    manager = _manager()
    probe = without_secret_recording(manager)

    assert probe is not manager
    assert probe._process_string("${stage} ${secret:db/key}") == (
        manager._process_string("${stage} ${secret:db/key}")
    )
    manager.secret_references.clear()
    probe._process_string("${secret:db/other}")

    assert probe.secret_references
    assert manager.secret_references == set()


def test_the_copy_leaves_the_managers_existing_references_alone():
    manager = _manager()
    manager._process_string("${secret:db/key}")
    before = set(manager.secret_references)

    without_secret_recording(manager)._process_string("${secret:db/other}")

    assert manager.secret_references == before
