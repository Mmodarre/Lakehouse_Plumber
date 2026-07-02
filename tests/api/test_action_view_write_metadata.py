"""Contract + behavioural tests for the ``ActionView`` write-metadata fields.

Covers the three optional write-metadata fields added to the public
:class:`ActionView` DTO (``write_mode`` / ``scd_type`` /
``target_full_name``) — load-bearing for the webapp Tables page — without
disturbing the pre-existing fields.

Two layers:

1. **DTO contract** — construction with and without the new fields,
   ``None`` defaults, immutability, and ``to_dict`` round-trip (§1.8).
   Also asserts the pre-existing fields keep their defaults / behaviour.
2. **Behavioural** — drive the public inspection facade over a small
   on-disk fixture (the ``_write_minimal_project`` pattern shared with
   ``test_inspection_orphans_behaviour.py``) and assert a standard write
   action yields ``write_mode="standard"`` plus the canonical
   ``catalog.schema.table`` target name, and a CDC write action yields
   ``write_mode="cdc"`` with ``scd_type`` populated. Substitution tokens
   in the target are resolved by the facade, not by the converter.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from lhp.api import (
    ActionView,
    LakehousePlumberApplicationFacade,
    ProcessedFlowgroupView,
    to_dict,
)

pytestmark = pytest.mark.unit

ENV = "dev"
PIPELINE = "bronze"
FLOWGROUP = "fg_targets"


class TestActionViewWriteMetadataContract:
    def test_construction_without_new_fields_defaults_to_none(self) -> None:
        view = ActionView(name="write_x", action_type="write")
        assert view.write_mode is None
        assert view.scd_type is None
        assert view.target_full_name is None

    def test_preexisting_fields_keep_their_defaults(self) -> None:
        # The new fields must not perturb the pre-existing surface.
        view = ActionView(name="load_x", action_type="load")
        assert view.target is None
        assert view.description is None
        assert view.transform_type is None
        assert view.test_type is None

    def test_construction_with_new_fields(self) -> None:
        view = ActionView(
            name="write_cdc",
            action_type="write",
            write_mode="cdc",
            scd_type=2,
            target_full_name="cat.sch.tbl",
        )
        assert view.write_mode == "cdc"
        assert view.scd_type == 2
        assert view.target_full_name == "cat.sch.tbl"

    def test_preexisting_and_new_fields_coexist(self) -> None:
        view = ActionView(
            name="write_full",
            action_type="write",
            target="v_source",
            description="a write",
            write_mode="standard",
            target_full_name="cat.sch.tbl",
        )
        # Pre-existing fields retain the supplied values.
        assert view.target == "v_source"
        assert view.description == "a write"
        # New fields retain theirs; scd_type stays at its None default.
        assert view.write_mode == "standard"
        assert view.target_full_name == "cat.sch.tbl"
        assert view.scd_type is None

    def test_is_frozen(self) -> None:
        from dataclasses import FrozenInstanceError

        view = ActionView(name="write_x", action_type="write")
        with pytest.raises(FrozenInstanceError):
            view.write_mode = "cdc"  # type: ignore[misc]

    def test_to_dict_round_trip_carries_new_fields(self) -> None:
        view = ActionView(
            name="write_cdc",
            action_type="write",
            target="v_source",
            write_mode="cdc",
            scd_type=2,
            target_full_name="cat.sch.tbl",
        )
        rebuilt = ActionView(**json.loads(json.dumps(to_dict(view))))
        assert rebuilt == view
        assert rebuilt.write_mode == "cdc"
        assert rebuilt.scd_type == 2
        assert rebuilt.target_full_name == "cat.sch.tbl"
        # Pre-existing field survives the round-trip too.
        assert rebuilt.target == "v_source"

    def test_to_dict_round_trip_defaults_none(self) -> None:
        view = ActionView(name="load_x", action_type="load")
        rebuilt = ActionView(**json.loads(json.dumps(to_dict(view))))
        assert rebuilt == view
        assert rebuilt.write_mode is None
        assert rebuilt.scd_type is None
        assert rebuilt.target_full_name is None


def _write_targets_project(root: Path) -> None:
    """One pipeline with a load plus two write actions: a *standard*
    streaming-table write and a *cdc* write (``scd_type: 2``). The target
    namespace uses the deprecated ``database: "${catalog}.${schema}"``
    form so the facade exercises both substitution resolution and the
    catalog/schema normalisation that yields the canonical full name.
    """
    (root / "lhp.yaml").write_text(
        textwrap.dedent("""\
            name: write_metadata_project
            version: "1.0"
            """)
    )
    for sub in ("presets", "templates"):
        (root / sub).mkdir(parents=True, exist_ok=True)

    subs = root / "substitutions"
    subs.mkdir(parents=True, exist_ok=True)
    (subs / f"{ENV}.yaml").write_text(
        textwrap.dedent(f"""\
            {ENV}:
              catalog: dev_catalog
              schema: bronze
            """)
    )

    fg_dir = root / "pipelines" / PIPELINE
    fg_dir.mkdir(parents=True, exist_ok=True)
    (fg_dir / "targets.yaml").write_text(
        textwrap.dedent(f"""\
            pipeline: {PIPELINE}
            flowgroup: {FLOWGROUP}
            actions:
              - name: load_raw
                type: load
                source:
                  type: sql
                  sql: "SELECT 1 AS id, current_timestamp() AS ts"
                target: v_raw
              - name: write_standard
                type: write
                source: v_raw
                write_target:
                  type: streaming_table
                  database: "${{catalog}}.${{schema}}"
                  table: standard_tbl
              - name: write_cdc
                type: write
                source: v_raw
                write_target:
                  type: streaming_table
                  mode: cdc
                  database: "${{catalog}}.${{schema}}"
                  table: cdc_tbl
                  cdc_config:
                    keys: [id]
                    sequence_by: ts
                    scd_type: 2
            """)
    )


@pytest.fixture
def facade(tmp_path: Path) -> LakehousePlumberApplicationFacade:
    _write_targets_project(tmp_path)
    return LakehousePlumberApplicationFacade.for_project(
        tmp_path, enforce_version=False
    )


class TestInspectionPopulatesWriteMetadata:
    def test_processed_view_exposes_each_action(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        view = facade.inspection.process_flowgroup(FLOWGROUP, env=ENV)
        assert isinstance(view, ProcessedFlowgroupView)
        assert {a.name for a in view.actions} == {
            "load_raw",
            "write_standard",
            "write_cdc",
        }

    def test_non_write_action_has_no_write_metadata(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        view = facade.inspection.process_flowgroup(FLOWGROUP, env=ENV)
        load = next(a for a in view.actions if a.name == "load_raw")
        assert load.write_mode is None
        assert load.scd_type is None
        assert load.target_full_name is None

    def test_standard_write_populates_mode_and_full_name(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        view = facade.inspection.process_flowgroup(FLOWGROUP, env=ENV)
        std = next(a for a in view.actions if a.name == "write_standard")
        assert std.write_mode == "standard"
        assert std.scd_type is None
        # Tokens resolved by the facade; namespace normalised to 3-part.
        assert std.target_full_name == "dev_catalog.bronze.standard_tbl"

    def test_cdc_write_populates_mode_scd_and_full_name(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        view = facade.inspection.process_flowgroup(FLOWGROUP, env=ENV)
        cdc = next(a for a in view.actions if a.name == "write_cdc")
        assert cdc.write_mode == "cdc"
        assert cdc.scd_type == 2
        assert cdc.target_full_name == "dev_catalog.bronze.cdc_tbl"
