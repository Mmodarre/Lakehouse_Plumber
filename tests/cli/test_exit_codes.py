"""Canonical contract for the LHP CLI exit-code scheme.

The CLI uses a portable 4-value scheme (``lhp.cli.exit_codes.ExitCode``):

    0  SUCCESS        — command completed normally
    1  ERROR          — domain-level error (any LHPError)
    2  USAGE_ERROR    — caller passed wrong arguments or options
    3  INTERNAL_ERROR — unexpected bug

This test pins the integer values and guards against the re-introduction of
the old sysexits-style members (CONFIG_ERROR / DATA_ERROR / NO_INPUT /
SOFTWARE_ERROR / IO_ERROR / GENERAL_ERROR) that the collapse removed.
"""

from enum import IntEnum

from lhp.cli.exit_codes import ExitCode


def test_exit_code_is_int_enum():
    """ExitCode is an IntEnum so members compare equal to their ints."""
    assert issubclass(ExitCode, IntEnum)


def test_exit_code_values():
    """The four canonical members map to 0/1/2/3."""
    assert ExitCode.SUCCESS == 0
    assert ExitCode.ERROR == 1
    assert ExitCode.USAGE_ERROR == 2
    assert ExitCode.INTERNAL_ERROR == 3


def test_exit_code_members_are_exactly_the_four():
    """No extra members beyond the canonical four."""
    assert {member.name for member in ExitCode} == {
        "SUCCESS",
        "ERROR",
        "USAGE_ERROR",
        "INTERNAL_ERROR",
    }


def test_removed_sysexits_members_no_longer_exist():
    """The old category-specific exit codes were dropped in the collapse."""
    for removed in (
        "CONFIG_ERROR",
        "DATA_ERROR",
        "NO_INPUT",
        "SOFTWARE_ERROR",
        "IO_ERROR",
        "GENERAL_ERROR",
    ):
        assert not hasattr(ExitCode, removed), (
            f"ExitCode.{removed} should have been removed by the 4-value collapse"
        )


def test_removed_factory_helpers_no_longer_exist():
    """from_lhp_error / from_error_category were removed with the collapse."""
    assert not hasattr(ExitCode, "from_lhp_error")
    assert not hasattr(ExitCode, "from_error_category")
