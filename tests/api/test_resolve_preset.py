"""Behavioral tests for ``InspectionFacade.resolve_preset``.

Companion to ``tests/api/test_preset_resolution_contract.py`` (which
exercises the :class:`PresetResolutionResult` dataclass in isolation):
these tests drive the facade method through the public entrypoint
``LakehousePlumberApplicationFacade.for_project(...).inspection.resolve_preset``
against a real on-disk project.

The shared E2E fixture project's presets carry no ``extends``, so a
minimal project is built in ``tmp_path`` instead (same approach as
``tests/api/test_build_substitution_view.py``) with:

- a three-level ``base_layer -> mid_layer -> leaf_layer`` chain that
  pins every merge rule (leaf wins per key, nested mappings merge
  recursively, ``operational_metadata`` lists concatenate with
  order-preserving dedup);
- a ``cycle_a <-> cycle_b`` pair (cycles are only detected at resolve
  time, so the project still loads);
- an ``orphan`` preset extending a preset that does not exist.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from lhp.api import LakehousePlumberApplicationFacade, PresetResolutionResult
from lhp.errors import LHPError

BASE_DEFAULTS = {
    "load_actions": {"cloudfiles": {"format": "parquet", "schema_hints": "auto"}},
    "operational_metadata": ["_ingest_ts", "_source_file"],
    "write_actions": {
        "streaming_table": {
            "table_properties": {
                "delta.enableRowTracking": "true",
                "delta.enableChangeDataFeed": "false",
            }
        }
    },
}


def _write_project(root: Path) -> None:
    (root / "lhp.yaml").write_text(
        textwrap.dedent("""\
            name: resolve_preset_test_project
            version: "1.0"
            """)
    )

    presets_dir = root / "presets"
    presets_dir.mkdir(parents=True, exist_ok=True)

    (presets_dir / "base_layer.yaml").write_text(
        textwrap.dedent("""\
            name: base_layer
            version: "1.0"
            defaults:
              load_actions:
                cloudfiles:
                  format: parquet
                  schema_hints: auto
              operational_metadata:
                - _ingest_ts
                - _source_file
              write_actions:
                streaming_table:
                  table_properties:
                    delta.enableRowTracking: "true"
                    delta.enableChangeDataFeed: "false"
            """)
    )

    # Overrides one nested table property, keeps the other; its
    # operational_metadata list overlaps base's on ``_source_file``.
    (presets_dir / "mid_layer.yaml").write_text(
        textwrap.dedent("""\
            name: mid_layer
            version: "1.0"
            extends: base_layer
            defaults:
              operational_metadata:
                - _source_file
                - _batch_id
              write_actions:
                streaming_table:
                  table_properties:
                    delta.enableChangeDataFeed: "true"
            """)
    )

    (presets_dir / "leaf_layer.yaml").write_text(
        textwrap.dedent("""\
            name: leaf_layer
            version: "1.0"
            extends: mid_layer
            defaults:
              load_actions:
                cloudfiles:
                  format: json
            """)
    )

    (presets_dir / "cycle_a.yaml").write_text(
        textwrap.dedent("""\
            name: cycle_a
            version: "1.0"
            extends: cycle_b
            defaults:
              load_actions: {}
            """)
    )
    (presets_dir / "cycle_b.yaml").write_text(
        textwrap.dedent("""\
            name: cycle_b
            version: "1.0"
            extends: cycle_a
            defaults:
              load_actions: {}
            """)
    )

    (presets_dir / "orphan.yaml").write_text(
        textwrap.dedent("""\
            name: orphan
            version: "1.0"
            extends: ghost_preset
            defaults:
              load_actions: {}
            """)
    )


@pytest.fixture
def facade(tmp_path: Path) -> LakehousePlumberApplicationFacade:
    _write_project(tmp_path)
    return LakehousePlumberApplicationFacade.for_project(
        tmp_path, enforce_version=False
    )


@pytest.mark.unit
class TestChainDerivation:
    def test_multi_level_chain_is_base_to_leaf(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        result = facade.inspection.resolve_preset("leaf_layer")
        assert isinstance(result, PresetResolutionResult)
        assert result.name == "leaf_layer"
        assert result.chain == ("base_layer", "mid_layer", "leaf_layer")

    def test_single_level_chain_is_just_the_name(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        result = facade.inspection.resolve_preset("base_layer")
        assert result.name == "base_layer"
        assert result.chain == ("base_layer",)


@pytest.mark.unit
class TestMergeSemantics:
    def test_single_level_merged_config_equals_defaults(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        result = facade.inspection.resolve_preset("base_layer")
        assert dict(result.merged_config) == BASE_DEFAULTS

    def test_leaf_override_wins_per_key(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        merged = facade.inspection.resolve_preset("leaf_layer").merged_config
        cloudfiles = merged["load_actions"]["cloudfiles"]  # type: ignore[index, call-overload]
        # Leaf overrides ``format`` but the recursive merge keeps the
        # sibling key it never mentions.
        assert cloudfiles["format"] == "json"
        assert cloudfiles["schema_hints"] == "auto"

    def test_nested_mappings_merge_recursively(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        merged = facade.inspection.resolve_preset("leaf_layer").merged_config
        props = merged["write_actions"]["streaming_table"]["table_properties"]  # type: ignore[index, call-overload]
        # mid overrides one property; base's other property survives.
        assert props["delta.enableChangeDataFeed"] == "true"
        assert props["delta.enableRowTracking"] == "true"

    def test_operational_metadata_lists_concat_with_dedup(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        merged = facade.inspection.resolve_preset("leaf_layer").merged_config
        # base [_ingest_ts, _source_file] + mid [_source_file, _batch_id]
        # → order-preserving concat with dedup, not replacement.
        assert merged["operational_metadata"] == [
            "_ingest_ts",
            "_source_file",
            "_batch_id",
        ]


@pytest.mark.unit
class TestErrorPropagation:
    def test_unknown_preset_raises_act_001(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        with pytest.raises(LHPError) as excinfo:
            facade.inspection.resolve_preset("no_such_preset")
        assert excinfo.value.code == "LHP-ACT-001"

    def test_missing_extends_target_raises_act_001(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        # ``orphan`` itself exists; its ``extends`` target does not.
        with pytest.raises(LHPError) as excinfo:
            facade.inspection.resolve_preset("orphan")
        assert excinfo.value.code == "LHP-ACT-001"

    def test_circular_extends_raises_dep_022(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        with pytest.raises(LHPError) as excinfo:
            facade.inspection.resolve_preset("cycle_a")
        assert excinfo.value.code == "LHP-DEP-022"
