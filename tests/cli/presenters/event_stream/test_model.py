"""Construction and immutability tests for the event-stream value objects."""

import dataclasses

import pytest

from lhp.cli.presenters.event_stream._model import (
    FailureLine,
    RenderOptions,
    RunHeader,
    RunOutcome,
    WarningLine,
)


def test_run_header_construct_and_frozen():
    header = RunHeader(command="generate", env="dev")
    assert header.command == "generate"
    assert header.env == "dev"
    with pytest.raises(dataclasses.FrozenInstanceError):
        header.command = "validate"  # type: ignore[misc]


def test_render_options_defaults_and_frozen():
    options = RenderOptions()
    assert options.show_details is False
    assert options.strict is False
    explicit = RenderOptions(show_details=True, strict=True)
    assert explicit.show_details is True
    assert explicit.strict is True
    with pytest.raises(dataclasses.FrozenInstanceError):
        options.strict = True  # type: ignore[misc]


def test_warning_line_construct_and_frozen():
    line = WarningLine(code="LHP-EVT-SOFT-CAP", message="near limit", file=None)
    assert line.code == "LHP-EVT-SOFT-CAP"
    assert line.message == "near limit"
    assert line.file is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        line.message = "other"  # type: ignore[misc]


def test_failure_line_construct_defaults_and_frozen():
    line = FailureLine(pipeline="bronze", code="LHP-VAL-021", message="boom")
    assert line.pipeline == "bronze"
    assert line.code == "LHP-VAL-021"
    assert line.message == "boom"
    assert line.flowgroup is None
    assert line.file is None
    enriched = FailureLine(
        pipeline="bronze",
        code="LHP-VAL-021",
        message="boom",
        flowgroup="ingest",
        file="ingest.yaml",
    )
    assert enriched.flowgroup == "ingest"
    assert enriched.file == "ingest.yaml"
    with pytest.raises(dataclasses.FrozenInstanceError):
        line.pipeline = "silver"  # type: ignore[misc]


def test_run_outcome_construct_and_frozen():
    warning = WarningLine(code="W", message="w", file=None)
    failure = FailureLine(pipeline="p", code="C", message="m")
    outcome = RunOutcome(
        response=object(),
        warnings=(warning,),
        failures=(failure,),
        errored=False,
    )
    assert outcome.warnings == (warning,)
    assert outcome.failures == (failure,)
    assert outcome.errored is False
    with pytest.raises(dataclasses.FrozenInstanceError):
        outcome.errored = True  # type: ignore[misc]
