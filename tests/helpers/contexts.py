from __future__ import annotations

from typing import Any

from lhp.models import FlowGroup, FlowGroupContext


def wrap_in_ctx(fg: Any, source_yaml: Any = None) -> FlowGroupContext:
    return FlowGroupContext(flowgroup=fg, source_yaml=source_yaml)


def process_unwrap(
    processor: Any, fg: FlowGroup, sub_mgr: Any, **kwargs: Any
) -> FlowGroup:
    return processor.process_flowgroup(wrap_in_ctx(fg), sub_mgr, **kwargs).flowgroup
