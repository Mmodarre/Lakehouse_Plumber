"""Contract test: ``WarningCollector`` public-surface invariants.

This pins the data-side of the warning-collection contract on
:class:`lhp.api.WarningCollector`: (1) it is exported from the canonical
public path ``lhp.api``, (2) :meth:`add` deduplicates on
``(category, message)`` while preserving insertion order, (3) instances
round-trip through ``pickle`` with both ``count`` and ``as_list()``
preserved, and (4) ``lhp.api.callbacks`` imports nothing from ``rich``
(constitution §9.6 — Rich belongs to ``lhp.cli``). The Rich Panel
renderer that consumes this collector is tested separately in
``tests/cli/test_warning_panel.py``; those render-side tests are the
counterpart to this surface contract and together cover the split.
"""
from __future__ import annotations

import ast
import importlib.util
import pickle

from lhp.api import WarningCollector


def test_warning_collector_is_public_api_export() -> None:
    import lhp.api as api

    assert "WarningCollector" in api.__all__, (
        "WarningCollector must be listed in lhp.api.__all__ — it is part "
        "of the public surface (constitution §1.8)."
    )
    from lhp.api import WarningCollector as Imported

    assert Imported is WarningCollector


def test_warning_collector_dedup_on_category_message_pair() -> None:
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")
    collector.add("deprecation", "msg-a")  # duplicate (category, message)

    assert collector.count == 2
    assert collector.as_list() == (
        ("deprecation", "msg-a"),
        ("other", "msg-b"),
    )


def test_warning_collector_pickle_round_trip_preserves_state() -> None:
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")

    restored = pickle.loads(pickle.dumps(collector))

    assert restored.count == collector.count
    assert restored.as_list() == collector.as_list()


def test_lhp_api_callbacks_imports_no_rich() -> None:
    # ast walk is preferred over dis bytecode inspection here: it is
    # equally rigorous for import detection and survives bytecode
    # encoding changes across Python versions.
    spec = importlib.util.find_spec("lhp.api.callbacks")
    assert spec is not None and spec.origin is not None
    with open(spec.origin, "r", encoding="utf-8") as src:
        tree = ast.parse(src.read(), filename=spec.origin)

    offending: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "rich" or alias.name.startswith("rich."):
                    offending.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == "rich" or mod.startswith("rich."):
                offending.append(mod)

    assert not offending, (
        f"lhp.api.callbacks must not import from rich (constitution §9.6). "
        f"Found: {offending}"
    )
