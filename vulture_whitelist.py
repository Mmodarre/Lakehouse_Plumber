"""Vulture allow-list for LHP.

Each entry suppresses a confirmed false positive. The comment above
each line states *why* vulture flags it and why the symbol must stay.

Run::

    vulture src/lhp/ vulture_whitelist.py --min-confidence 80

Expected outcome: 0 findings.
"""

# Signature parameter of the importlib.metadata.version() fallback stub —
# required to match the public signature even though the body returns "0.0.0".
package

# Used as a stringified forward-reference ("ClassVar[Set[str]]") on
# models.config.ProjectConfig._legacy_warned_paths; vulture does not parse
# string annotations.
ClassVar

# Pydantic's BaseModel.model_post_init() hook requires the second positional
# arg (named __context per Pydantic convention); the leading double-underscore
# signals intentional non-use, and the symbol is part of Pydantic's API contract.
__context

# TYPE_CHECKING-only re-import aliased for the public DTO module; referenced
# via string forward-ref ("_InternalDepResult") in _converters.py:483, which
# vulture does not trace.
_InternalDepResult
