"""Read-mode declarations: the one place each generator states how it reads.

``BaseActionGenerator.default_read_mode`` / ``fixed_read_mode`` declare the
read a generator's code makes; ``read_mode(action)`` is the mode an action asks
for and ``consumer_read_mode(action)`` the read its code actually makes.
``ActionRegistry`` exposes both per action, the way it exposes
``renders_operational_metadata``.
"""

from __future__ import annotations

from typing import Any

import pytest

from lhp.core.registry import ActionRegistry, BaseActionGenerator
from lhp.core.registry.action_registry import _REGISTERED_GENERATORS
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


def _load(source: dict, read_mode: str | None = None) -> Action:
    return Action(
        name="l", type=ActionType.LOAD, source=source, target="v", readMode=read_mode
    )


def _write(write_target: dict, read_mode: str | None = None, **fields: Any) -> Action:
    return Action(
        name="w",
        type=ActionType.WRITE,
        source="v",
        write_target=write_target,
        readMode=read_mode,
        **fields,
    )


def _declarations() -> dict:
    return {
        (category, str(getattr(key, "value", key))): (
            generator.default_read_mode,
            generator.fixed_read_mode,
        )
        for category, mapping in _REGISTERED_GENERATORS.items()
        for key, generator in mapping.items()
    }


class TestDeclarations:
    def test_every_registered_generator_declares_its_read(self) -> None:
        assert _declarations() == {
            ("load", "cloudfiles"): ("stream", True),
            ("load", "kafka"): ("stream", True),
            ("load", "delta"): ("batch", False),
            ("load", "custom_datasource"): ("stream", False),
            ("load", "jdbc"): ("batch", True),
            ("load", "sql"): (None, True),
            ("load", "python"): (None, True),
            ("transform", "sql"): (None, True),
            ("transform", "data_quality"): ("stream", False),
            ("transform", "schema"): ("stream", False),
            ("transform", "python"): ("batch", False),
            ("transform", "temp_table"): ("batch", False),
            ("write", "streaming_table"): ("stream", False),
            ("write", "materialized_view"): ("batch", True),
            ("write", "sink"): ("stream", True),
            ("test", "row_count"): ("batch", True),
            ("test", "uniqueness"): ("batch", True),
            ("test", "referential_integrity"): ("batch", True),
            ("test", "completeness"): ("batch", True),
            ("test", "range"): ("batch", True),
            ("test", "schema_match"): ("batch", True),
            ("test", "all_lookups_found"): ("batch", True),
            ("test", "custom_sql"): ("batch", True),
            ("test", "custom_expectations"): ("batch", True),
        }

    def test_only_the_loads_with_a_nested_read_mode_fall_back_to_it(self) -> None:
        fallback = {
            (category, str(getattr(key, "value", key)))
            for category, mapping in _REGISTERED_GENERATORS.items()
            for key, generator in mapping.items()
            if generator.source_read_mode_fallback
        }

        assert fallback == {
            ("load", "cloudfiles"),
            ("load", "kafka"),
            ("load", "delta"),
        }
        assert BaseActionGenerator.source_read_mode_fallback is False

    @pytest.mark.parametrize("sub_type", ["cloudfiles", "kafka", "delta"])
    def test_the_fallback_sits_between_the_action_and_the_default(
        self, sub_type: str
    ) -> None:
        generator = _REGISTERED_GENERATORS["load"][sub_type]
        nested = _load({"type": sub_type, "readMode": "batch"})
        both = _load({"type": sub_type, "readMode": "batch"}, "stream")
        neither = _load({"type": sub_type})

        assert generator.read_mode(nested) == "batch"
        assert generator.read_mode(both) == "stream"
        assert generator.read_mode(neither) == generator.default_read_mode

    def test_base_declares_no_read(self) -> None:
        assert BaseActionGenerator.default_read_mode is None
        assert BaseActionGenerator.fixed_read_mode is False


class TestRegistryReadMode:
    @pytest.mark.parametrize(
        ("action", "expected"),
        [
            (_load({"type": "delta", "table": "t"}), "batch"),
            (_load({"type": "delta", "table": "t"}, "stream"), "stream"),
            (_load({"type": "delta", "table": "t", "readMode": "stream"}), "stream"),
            (
                _load({"type": "delta", "table": "t", "readMode": "stream"}, "batch"),
                "batch",
            ),
            (_load({"type": "cloudfiles"}), "stream"),
            (_load({"type": "cloudfiles", "readMode": "batch"}), "batch"),
            (_load({"type": "kafka"}, "batch"), "batch"),
            (_load({"type": "custom_datasource"}), "stream"),
            (_load({"type": "python"}), None),
            (_load({"type": "sql", "sql": "SELECT 1"}), None),
            (_write({"type": "materialized_view"}, "stream"), "stream"),
        ],
        ids=[
            "delta-default",
            "delta-action",
            "delta-source",
            "delta-action-over-source",
            "cloudfiles-default",
            "cloudfiles-source-batch",
            "kafka-action-batch",
            "custom_datasource-default",
            "python-load-none",
            "sql-load-none",
            "materialized_view-asks-stream",
        ],
    )
    def test_read_mode_is_the_mode_asked_for(
        self, action: Action, expected: str | None
    ) -> None:
        assert ActionRegistry().read_mode(action) == expected

    def test_unrecognised_read_mode_is_returned_as_written(self) -> None:
        action = Action(
            name="t",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            source="v",
            target="v2",
            readMode="Stream",
        )
        assert ActionRegistry().read_mode(action) == "Stream"


class TestRegistryConsumerReadMode:
    @pytest.mark.parametrize(
        ("action", "expected"),
        [
            (_write({"type": "materialized_view"}), "batch"),
            (_write({"type": "materialized_view"}, "stream"), "batch"),
            (_write({"type": "sink", "sink_type": "delta"}), "stream"),
            (_write({"type": "sink", "sink_type": "kafka"}, "batch"), "stream"),
            (
                Action(
                    name="t", type=ActionType.TEST, test_type="uniqueness", source="v"
                ),
                "batch",
            ),
            (_load({"type": "cloudfiles"}, "batch"), "stream"),
            (_load({"type": "jdbc"}, "stream"), "batch"),
            (_load({"type": "delta", "table": "t"}), "batch"),
            (_load({"type": "python"}), None),
            (_load({"type": "python"}, "batch"), None),
            (_load({"type": "sql", "sql": "SELECT 1"}, "stream"), None),
            (
                Action(
                    name="t",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    sql="SELECT * FROM v",
                    target="v2",
                    readMode="batch",
                ),
                None,
            ),
        ],
        ids=[
            "materialized_view",
            "materialized_view-ignores-readMode",
            "sink",
            "sink-ignores-readMode",
            "test-action",
            "cloudfiles-always-stream",
            "jdbc-always-batch",
            "delta",
            "python-load-unknown",
            "python-load-ignores-readMode",
            "sql-load-ignores-readMode",
            "sql-transform-ignores-readMode",
        ],
    )
    def test_consumer_read(self, action: Action, expected: str | None) -> None:
        assert ActionRegistry().consumer_read_mode(action) == expected

    @pytest.mark.parametrize("transform_type", list(TransformType))
    def test_unrecognised_read_mode_is_not_certain(
        self, transform_type: TransformType
    ) -> None:
        action = Action(
            name="t",
            type=ActionType.TRANSFORM,
            transform_type=transform_type,
            source="v",
            target="v2",
            readMode="sometimes",
        )
        assert ActionRegistry().consumer_read_mode(action) is None

    def test_matches_the_generator_class_hook(self) -> None:
        registry = ActionRegistry()
        action = _write({"type": "streaming_table", "mode": "cdc"}, once=True)
        generator_class = type(
            registry.get_generator(ActionType.WRITE, "streaming_table")
        )

        assert registry.consumer_read_mode(action) is None
        assert generator_class.consumer_read_mode(action) is None
