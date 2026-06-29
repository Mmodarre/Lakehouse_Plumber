"""Developer-sandbox configuration models (team policy + personal profile).

``SandboxConfig`` is the team-level policy committed in ``lhp.yaml`` under the
``sandbox:`` block; ``SandboxProfile`` is the personal, gitignored
``.lhp/profile.yaml`` payload (nested under its ``sandbox:`` key). Both are
pure input-validation models (constitution §2.6): the loaders map their
``ValidationError``s to coded LHP errors — the models themselves never import
error codes.
"""

import re
import string
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

# The only placeholders a table_pattern may reference — and both must appear,
# otherwise distinct sandbox tables (or distinct source tables) would collide.
_PATTERN_PLACEHOLDERS = frozenset({"namespace", "table"})

# Literal text between placeholders must stay an identifier fragment so the
# formatted result is always a legal Unity Catalog table-name leaf.
_PATTERN_LITERAL_RE = re.compile(r"[A-Za-z0-9_]*")


class SandboxConfig(BaseModel):
    """Team-level sandbox policy from the ``sandbox:`` block in ``lhp.yaml``."""

    strategy: Literal["table"] = "table"  # widens to schema/catalog later
    table_pattern: str = "{namespace}_{table}"  # formats the table LEAF only
    allowed_envs: Optional[List[str]] = None  # None = unrestricted; [] is the
    # parser's problem (CFG_062) — the model deliberately permits it.

    @field_validator("table_pattern")
    @classmethod
    def _check_table_pattern(cls, value: str) -> str:
        """Structural validation behind CFG_063 (raised here as ``ValueError``).

        Parsed with ``string.Formatter().parse`` so the check matches exactly
        what ``str.format`` will do at rewrite time: placeholders must be a
        subset of ``{namespace, table}`` with both present, carry no
        conversion (``!r``) or format spec (``:>10``), and literal text is
        limited to ``[A-Za-z0-9_]``.
        """
        try:
            segments = list(string.Formatter().parse(value))
        except ValueError as e:
            raise ValueError(f"table_pattern is not a valid format string: {e}") from e

        seen: set[str] = set()
        for literal_text, field_name, format_spec, conversion in segments:
            if not _PATTERN_LITERAL_RE.fullmatch(literal_text):
                raise ValueError(
                    "table_pattern literal text may only contain letters, "
                    f"digits, and underscores; got {literal_text!r}"
                )
            if field_name is None:
                continue
            if field_name not in _PATTERN_PLACEHOLDERS:
                raise ValueError(
                    f"table_pattern placeholder {{{field_name}}} is not "
                    "recognized; only {namespace} and {table} are allowed"
                )
            if conversion is not None or format_spec:
                raise ValueError(
                    f"table_pattern placeholder {{{field_name}}} must be "
                    "plain — conversions (e.g. !r) and format specs "
                    "(e.g. :>10) are not allowed"
                )
            seen.add(field_name)

        missing = _PATTERN_PLACEHOLDERS - seen
        if missing:
            missing_list = ", ".join(f"{{{name}}}" for name in sorted(missing))
            raise ValueError(f"table_pattern must contain {missing_list}")
        return value


class SandboxProfile(BaseModel):
    """Personal sandbox profile from gitignored ``.lhp/profile.yaml``.

    Explicit opt-in only: the namespace and pipeline scope are always stated
    by the developer — never auto-detected.
    """

    # Lowercase-only, matching the canonicalization lowering applied to
    # generated table names; max 64 chars total.
    namespace: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    pipelines: List[str] = Field(min_length=1)  # names or fnmatchcase globs
