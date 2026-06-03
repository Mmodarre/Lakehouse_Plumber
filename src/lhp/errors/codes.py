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

DEP_001 = ErrorCode(ErrorCategory.DEPENDENCY, "001")
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
    DEP_001,
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
    "DEPR_001",
    "DEPR_002",
    "DEPR_003",
    "DEPR_004",
    "DEP_001",
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
    "VAL_902",
    "VAL_DUPFG",
    "ErrorCode",
]
