"""Registry of LHP error codes.

A code is synthesized in ``LHPError.__init__`` as
``f"LHP-{category.value}-{code_number}"``, so identity is the
``(ErrorCategory, code_number)`` pair, not the bare number — the same
number recurs across categories and ``code_number`` is a free-form ``str``
(e.g. ``"DUPFG"``), not necessarily numeric. The ``.code`` property
reproduces the exact synthesized string.

Naming scheme: ``<CATEGORY_VALUE>_<NUMBER>`` so each constant is greppable
from its rendered code — ``VAL_009`` maps to ``LHP-VAL-009``.

Internal module per constitution §1.7: no ``:stability:`` annotations and
not re-exported from ``lhp/api/``.
"""

from dataclasses import dataclass

from .categories import ErrorCategory


@dataclass(frozen=True)
class ErrorCode:
    """A single LHP error code, keyed on ``(category, number)``."""

    category: ErrorCategory
    number: str

    @property
    def code(self) -> str:
        return f"LHP-{self.category.value}-{self.number}"


VAL_001 = ErrorCode(ErrorCategory.VALIDATION, "001")
VAL_002 = ErrorCode(ErrorCategory.VALIDATION, "002")
VAL_003 = ErrorCode(ErrorCategory.VALIDATION, "003")
VAL_004 = ErrorCode(ErrorCategory.VALIDATION, "004")
VAL_005 = ErrorCode(ErrorCategory.VALIDATION, "005")
VAL_006 = ErrorCode(ErrorCategory.VALIDATION, "006")
VAL_007 = ErrorCode(ErrorCategory.VALIDATION, "007")
VAL_008 = ErrorCode(ErrorCategory.VALIDATION, "008")
VAL_009 = ErrorCode(ErrorCategory.VALIDATION, "009")
VAL_010 = ErrorCode(ErrorCategory.VALIDATION, "010")
VAL_011 = ErrorCode(ErrorCategory.VALIDATION, "011")
VAL_012 = ErrorCode(ErrorCategory.VALIDATION, "012")
VAL_013 = ErrorCode(ErrorCategory.VALIDATION, "013")
VAL_014 = ErrorCode(ErrorCategory.VALIDATION, "014")
VAL_015 = ErrorCode(ErrorCategory.VALIDATION, "015")
VAL_016 = ErrorCode(ErrorCategory.VALIDATION, "016")
VAL_017 = ErrorCode(ErrorCategory.VALIDATION, "017")
VAL_018 = ErrorCode(ErrorCategory.VALIDATION, "018")
VAL_019 = ErrorCode(ErrorCategory.VALIDATION, "019")
VAL_020 = ErrorCode(ErrorCategory.VALIDATION, "020")
VAL_021 = ErrorCode(ErrorCategory.VALIDATION, "021")
VAL_022 = ErrorCode(ErrorCategory.VALIDATION, "022")
VAL_023 = ErrorCode(ErrorCategory.VALIDATION, "023")
VAL_024 = ErrorCode(ErrorCategory.VALIDATION, "024")
VAL_025 = ErrorCode(ErrorCategory.VALIDATION, "025")
VAL_041 = ErrorCode(ErrorCategory.VALIDATION, "041")
VAL_042 = ErrorCode(ErrorCategory.VALIDATION, "042")
VAL_043 = ErrorCode(ErrorCategory.VALIDATION, "043")
VAL_044 = ErrorCode(ErrorCategory.VALIDATION, "044")
VAL_045 = ErrorCode(ErrorCategory.VALIDATION, "045")
VAL_046 = ErrorCode(ErrorCategory.VALIDATION, "046")
VAL_053 = ErrorCode(ErrorCategory.VALIDATION, "053")
VAL_055 = ErrorCode(ErrorCategory.VALIDATION, "055")
VAL_061 = ErrorCode(ErrorCategory.VALIDATION, "061")
VAL_062 = ErrorCode(ErrorCategory.VALIDATION, "062")
# VAL_063: an action's optional ``depends_on`` escape-hatch entry is malformed —
# raised by ConfigFieldValidator._validate_depends_on when an entry is not a
# non-empty string, has >3 dot-separated parts, or has a blank dotted part
# (a well-formed catalog.schema.table / schema.table / table reference is
# required).
VAL_063 = ErrorCode(ErrorCategory.VALIDATION, "063")
# VAL_064: a sandbox profile ``pipelines`` entry matches zero pipelines in the
# project — raised by sandbox scope resolution when a profile pattern selects
# nothing.
VAL_064 = ErrorCode(ErrorCategory.VALIDATION, "064")
# VAL_065: WARNING code — a sandbox-rewritten sink is also produced by an
# out-of-scope pipeline (mixed-producer sink; the sink is rewritten anyway);
# stamped by the sandbox engine on SandboxWarningRecord (rides WarningEmitted,
# category="sandbox"), never raised as an error.
VAL_065 = ErrorCode(ErrorCategory.VALIDATION, "065")
# VAL_066: WARNING code — an in-scope read could not be rewritten to its
# sandbox table name (indirect Python reference); stamped by the sandbox
# engine, never raised as an error.
VAL_066 = ErrorCode(ErrorCategory.VALIDATION, "066")
VAL_902 = ErrorCode(ErrorCategory.VALIDATION, "902")
VAL_DUPFG = ErrorCode(ErrorCategory.VALIDATION, "DUPFG")

IO_001 = ErrorCode(ErrorCategory.IO, "001")
IO_002 = ErrorCode(ErrorCategory.IO, "002")
IO_003 = ErrorCode(ErrorCategory.IO, "003")
IO_004 = ErrorCode(ErrorCategory.IO, "004")
IO_005 = ErrorCode(ErrorCategory.IO, "005")
IO_006 = ErrorCode(ErrorCategory.IO, "006")
IO_007 = ErrorCode(ErrorCategory.IO, "007")
IO_020 = ErrorCode(ErrorCategory.IO, "020")
IO_021 = ErrorCode(ErrorCategory.IO, "021")
IO_022 = ErrorCode(ErrorCategory.IO, "022")
IO_023 = ErrorCode(ErrorCategory.IO, "023")
IO_024 = ErrorCode(ErrorCategory.IO, "024")
# IO_025: sandbox mode requires a personal profile — raised by the sandbox
# profile loader when ``.lhp/profile.yaml`` is missing.
IO_025 = ErrorCode(ErrorCategory.IO, "025")
# IO_026: the optional webapp dependencies (fastapi / uvicorn) are not
# installed — raised by the ``lhp web`` command when ``importlib.util.find_spec``
# cannot locate one of them; suggests ``pip install lakehouse-plumber[webapp]``.
IO_026 = ErrorCode(ErrorCategory.IO, "026")

CFG_001 = ErrorCode(ErrorCategory.CONFIG, "001")
CFG_002 = ErrorCode(ErrorCategory.CONFIG, "002")
CFG_003 = ErrorCode(ErrorCategory.CONFIG, "003")
CFG_004 = ErrorCode(ErrorCategory.CONFIG, "004")
CFG_005 = ErrorCode(ErrorCategory.CONFIG, "005")
CFG_006 = ErrorCode(ErrorCategory.CONFIG, "006")
CFG_007 = ErrorCode(ErrorCategory.CONFIG, "007")
CFG_008 = ErrorCode(ErrorCategory.CONFIG, "008")
CFG_009 = ErrorCode(ErrorCategory.CONFIG, "009")
CFG_010 = ErrorCode(ErrorCategory.CONFIG, "010")
CFG_011 = ErrorCode(ErrorCategory.CONFIG, "011")
CFG_012 = ErrorCode(ErrorCategory.CONFIG, "012")
CFG_013 = ErrorCode(ErrorCategory.CONFIG, "013")
CFG_014 = ErrorCode(ErrorCategory.CONFIG, "014")
CFG_015 = ErrorCode(ErrorCategory.CONFIG, "015")
CFG_016 = ErrorCode(ErrorCategory.CONFIG, "016")
CFG_017 = ErrorCode(ErrorCategory.CONFIG, "017")
CFG_020 = ErrorCode(ErrorCategory.CONFIG, "020")
CFG_021 = ErrorCode(ErrorCategory.CONFIG, "021")
CFG_023 = ErrorCode(ErrorCategory.CONFIG, "023")
CFG_024 = ErrorCode(ErrorCategory.CONFIG, "024")
CFG_025 = ErrorCode(ErrorCategory.CONFIG, "025")
CFG_026 = ErrorCode(ErrorCategory.CONFIG, "026")
CFG_027 = ErrorCode(ErrorCategory.CONFIG, "027")
CFG_028 = ErrorCode(ErrorCategory.CONFIG, "028")
CFG_029 = ErrorCode(ErrorCategory.CONFIG, "029")
CFG_030 = ErrorCode(ErrorCategory.CONFIG, "030")
CFG_031 = ErrorCode(ErrorCategory.CONFIG, "031")
CFG_033 = ErrorCode(ErrorCategory.CONFIG, "033")
CFG_034 = ErrorCode(ErrorCategory.CONFIG, "034")
CFG_040 = ErrorCode(ErrorCategory.CONFIG, "040")
CFG_047 = ErrorCode(ErrorCategory.CONFIG, "047")
CFG_048 = ErrorCode(ErrorCategory.CONFIG, "048")
CFG_049 = ErrorCode(ErrorCategory.CONFIG, "049")
CFG_050 = ErrorCode(ErrorCategory.CONFIG, "050")
CFG_051 = ErrorCode(ErrorCategory.CONFIG, "051")
CFG_052 = ErrorCode(ErrorCategory.CONFIG, "052")
CFG_054 = ErrorCode(ErrorCategory.CONFIG, "054")
CFG_056 = ErrorCode(ErrorCategory.CONFIG, "056")
CFG_057 = ErrorCode(ErrorCategory.CONFIG, "057")
CFG_058 = ErrorCode(ErrorCategory.CONFIG, "058")
CFG_059 = ErrorCode(ErrorCategory.CONFIG, "059")
CFG_060 = ErrorCode(ErrorCategory.CONFIG, "060")
# CFG_061: wheel packaging requires a valid ``/Volumes/...`` artifact volume —
# raised by BundleManager._resolve_artifact_volume when wheel.artifact_volume is
# absent/empty or does not start with ``/Volumes/`` (serverless installs custom
# wheels only from a UC volume).
CFG_061 = ErrorCode(ErrorCategory.CONFIG, "061")
# CFG_062: the ``sandbox:`` block in lhp.yaml is invalid — raised by the
# sandbox config loader when the block is not a mapping, names an unknown
# strategy, or declares an empty ``allowed_envs``.
CFG_062 = ErrorCode(ErrorCategory.CONFIG, "062")
# CFG_063: the sandbox ``table_pattern`` is invalid — raised by the sandbox
# config loader when the pattern cannot produce a valid sandbox table name.
CFG_063 = ErrorCode(ErrorCategory.CONFIG, "063")
# CFG_064: the personal sandbox profile (``.lhp/profile.yaml``) is invalid —
# raised by the sandbox profile loader when ``namespace`` fails the identifier
# regex or ``pipelines`` is empty.
CFG_064 = ErrorCode(ErrorCategory.CONFIG, "064")
# CFG_065: the requested environment is not sandbox-enabled — raised when
# ``--sandbox`` targets an env absent from ``sandbox.allowed_envs``.
CFG_065 = ErrorCode(ErrorCategory.CONFIG, "065")
# CFG_066: a declared UC tag key or value is illegal under Unity Catalog's
# charset/length rules — raised at generation time when a tag key or value
# contains a prohibited character, has leading/trailing whitespace, or exceeds
# 256 characters. Raised via ``ErrorFactory.config_error`` with title
# "Illegal UC tag key/value" and a ``details`` template threading the context
# label (table FQN or ``fqn.column``), which part is offending ("key"/"value"),
# the offending text, and the specific reason, e.g.:
#   f"UC tag {part} {value!r} on {context} is illegal: {reason}."
CFG_066 = ErrorCode(ErrorCategory.CONFIG, "066")

DEP_001 = ErrorCode(ErrorCategory.DEPENDENCY, "001")
# DEP_002: dependency extraction found a recognized table-read in Python code
# whose argument could not be statically resolved (opaque read) — warning-only
# advisory carried on ``DependencyWarning`` records, never raised as an error.
DEP_002 = ErrorCode(ErrorCategory.DEPENDENCY, "002")
# DEP_003: a SQL source could not be parsed for table extraction — warning-only
# advisory carried on ``DependencyWarning`` records, never raised as an error.
DEP_003 = ErrorCode(ErrorCategory.DEPENDENCY, "003")
DEP_022 = ErrorCode(ErrorCategory.DEPENDENCY, "022")

ACT_001 = ErrorCode(ErrorCategory.ACTION, "001")
ACT_002 = ErrorCode(ErrorCategory.ACTION, "002")

GEN_001 = ErrorCode(ErrorCategory.GENERAL, "001")
GEN_901 = ErrorCode(ErrorCategory.GENERAL, "901")
GEN_902 = ErrorCode(ErrorCategory.GENERAL, "902")

# DEPRECATION (DEPR) — soft-deprecation warnings emitted by producer tasks.
# Frozen contract; message text is stamped at the call site via
# ErrorFactory.deprecation_error. (blueprint: legacy is a HARD error,
# LHP-VAL-061, NOT a soft deprecation — deliberately no slot here.)
#   DEPR_001: bare ``{token}`` substitution syntax is deprecated; use
#             ``${token}`` (the only valid non-``$`` braces form is
#             ``%{local_var}`` for local variables).
#   DEPR_002: the ``database`` field is deprecated.
#   DEPR_003: the schema-transform ``enforcement`` key is deprecated.
#   DEPR_004: ``database_suffix`` is deprecated.
DEPR_001 = ErrorCode(ErrorCategory.DEPRECATION, "001")
DEPR_002 = ErrorCode(ErrorCategory.DEPRECATION, "002")
DEPR_003 = ErrorCode(ErrorCategory.DEPRECATION, "003")
DEPR_004 = ErrorCode(ErrorCategory.DEPRECATION, "004")


ALL_CODES: tuple[ErrorCode, ...] = (
    VAL_001,
    VAL_002,
    VAL_003,
    VAL_004,
    VAL_005,
    VAL_006,
    VAL_007,
    VAL_008,
    VAL_009,
    VAL_010,
    VAL_011,
    VAL_012,
    VAL_013,
    VAL_014,
    VAL_015,
    VAL_016,
    VAL_017,
    VAL_018,
    VAL_019,
    VAL_020,
    VAL_021,
    VAL_022,
    VAL_023,
    VAL_024,
    VAL_025,
    VAL_041,
    VAL_042,
    VAL_043,
    VAL_044,
    VAL_045,
    VAL_046,
    VAL_053,
    VAL_055,
    VAL_061,
    VAL_062,
    VAL_063,
    VAL_064,
    VAL_065,
    VAL_066,
    VAL_902,
    VAL_DUPFG,
    IO_001,
    IO_002,
    IO_003,
    IO_004,
    IO_005,
    IO_006,
    IO_007,
    IO_020,
    IO_021,
    IO_022,
    IO_023,
    IO_024,
    IO_025,
    IO_026,
    CFG_001,
    CFG_002,
    CFG_003,
    CFG_004,
    CFG_005,
    CFG_006,
    CFG_007,
    CFG_008,
    CFG_009,
    CFG_010,
    CFG_011,
    CFG_012,
    CFG_013,
    CFG_014,
    CFG_015,
    CFG_016,
    CFG_017,
    CFG_020,
    CFG_021,
    CFG_023,
    CFG_024,
    CFG_025,
    CFG_026,
    CFG_027,
    CFG_028,
    CFG_029,
    CFG_030,
    CFG_031,
    CFG_033,
    CFG_034,
    CFG_040,
    CFG_047,
    CFG_048,
    CFG_049,
    CFG_050,
    CFG_051,
    CFG_052,
    CFG_054,
    CFG_056,
    CFG_057,
    CFG_058,
    CFG_059,
    CFG_060,
    CFG_061,
    CFG_062,
    CFG_063,
    CFG_064,
    CFG_065,
    CFG_066,
    DEP_001,
    DEP_002,
    DEP_003,
    DEP_022,
    ACT_001,
    ACT_002,
    GEN_001,
    GEN_901,
    GEN_902,
    DEPR_001,
    DEPR_002,
    DEPR_003,
    DEPR_004,
)


__all__ = [
    "ACT_001",
    "ACT_002",
    "ALL_CODES",
    "CFG_001",
    "CFG_002",
    "CFG_003",
    "CFG_004",
    "CFG_005",
    "CFG_006",
    "CFG_007",
    "CFG_008",
    "CFG_009",
    "CFG_010",
    "CFG_011",
    "CFG_012",
    "CFG_013",
    "CFG_014",
    "CFG_015",
    "CFG_016",
    "CFG_017",
    "CFG_020",
    "CFG_021",
    "CFG_023",
    "CFG_024",
    "CFG_025",
    "CFG_026",
    "CFG_027",
    "CFG_028",
    "CFG_029",
    "CFG_030",
    "CFG_031",
    "CFG_033",
    "CFG_034",
    "CFG_040",
    "CFG_047",
    "CFG_048",
    "CFG_049",
    "CFG_050",
    "CFG_051",
    "CFG_052",
    "CFG_054",
    "CFG_056",
    "CFG_057",
    "CFG_058",
    "CFG_059",
    "CFG_060",
    "CFG_061",
    "CFG_062",
    "CFG_063",
    "CFG_064",
    "CFG_065",
    "CFG_066",
    "DEPR_001",
    "DEPR_002",
    "DEPR_003",
    "DEPR_004",
    "DEP_001",
    "DEP_002",
    "DEP_003",
    "DEP_022",
    "GEN_001",
    "GEN_901",
    "GEN_902",
    "IO_001",
    "IO_002",
    "IO_003",
    "IO_004",
    "IO_005",
    "IO_006",
    "IO_007",
    "IO_020",
    "IO_021",
    "IO_022",
    "IO_023",
    "IO_024",
    "IO_025",
    "IO_026",
    "VAL_001",
    "VAL_002",
    "VAL_003",
    "VAL_004",
    "VAL_005",
    "VAL_006",
    "VAL_007",
    "VAL_008",
    "VAL_009",
    "VAL_010",
    "VAL_011",
    "VAL_012",
    "VAL_013",
    "VAL_014",
    "VAL_015",
    "VAL_016",
    "VAL_017",
    "VAL_018",
    "VAL_019",
    "VAL_020",
    "VAL_021",
    "VAL_022",
    "VAL_023",
    "VAL_024",
    "VAL_025",
    "VAL_041",
    "VAL_042",
    "VAL_043",
    "VAL_044",
    "VAL_045",
    "VAL_046",
    "VAL_053",
    "VAL_055",
    "VAL_061",
    "VAL_062",
    "VAL_063",
    "VAL_064",
    "VAL_065",
    "VAL_066",
    "VAL_902",
    "VAL_DUPFG",
    "ErrorCode",
]
