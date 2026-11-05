"""Cross-cutting test utilities — keep helpers here so individual test files don't grow ad-hoc helpers that drift apart."""

from .contexts import process_unwrap, wrap_in_ctx
from .generation import read_generated_pipeline
from .quarantine import write_quarantine_project
from .replace_where import (
    OTHER_CATALOG_PIPELINE_FILE,
    RW_READER_MESSAGE,
    RW_READER_PIPELINE,
    TWO_PART_PIPELINE,
    inject_replace_where_stream_reader,
    inject_two_part_stream_readers,
)
from .telemetry import (
    assert_no_names_in_values,
    last_cli_command_line,
    parse_last_cli_command,
    project_names,
)
from .validation_warning import inject_worker_validation_warning, write_clean_project

__all__ = [
    "OTHER_CATALOG_PIPELINE_FILE",
    "RW_READER_MESSAGE",
    "RW_READER_PIPELINE",
    "TWO_PART_PIPELINE",
    "assert_no_names_in_values",
    "inject_replace_where_stream_reader",
    "inject_two_part_stream_readers",
    "inject_worker_validation_warning",
    "last_cli_command_line",
    "parse_last_cli_command",
    "process_unwrap",
    "project_names",
    "read_generated_pipeline",
    "wrap_in_ctx",
    "write_clean_project",
    "write_quarantine_project",
]
