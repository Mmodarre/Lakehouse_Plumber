"""Tests for :mod:`lhp.utils.file_pattern_matcher`.

Characterization of the determinism guarantee of
:func:`discover_files_with_patterns`: the function uses :meth:`Path.rglob`,
whose iteration order follows the underlying filesystem and is therefore not
guaranteed to be stable across machines or runs. Every other discovery site in
the codebase sorts its results; this one is the outlier the test pins down.

The test rebinds :meth:`pathlib.Path.rglob` to yield files in a deliberately
shuffled (reverse-sorted) order and asserts that
:func:`discover_files_with_patterns` returns them in stable, path-sorted order
regardless of the iteration order the filesystem hands back. It exercises both
return paths: the no-patterns backwards-compatibility branch and the
pattern-matched branch.
"""

from pathlib import Path

import pytest

from lhp.utils.file_pattern_matcher import discover_files_with_patterns


def _make_shuffled_rglob(base_dir: Path):
    """Return an rglob replacement that yields .yaml files in reverse order.

    Reverse order is a stand-in for any non-sorted filesystem iteration order:
    if the function did not sort, the returned list would come back reversed.
    """
    yaml_files = [
        base_dir / "zebra.yaml",
        base_dir / "sub" / "middle.yaml",
        base_dir / "alpha.yaml",
    ]

    def fake_rglob(self: Path, pattern: str):
        if pattern == "*.yaml":
            # Hand them back in a non-sorted (reverse-sorted) order.
            return iter(sorted(yaml_files, reverse=True))
        return iter([])  # no *.yml files

    return fake_rglob, yaml_files


@pytest.mark.unit
def test_discover_no_patterns_returns_sorted_regardless_of_iteration_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The no-patterns branch returns files in stable sorted order."""
    fake_rglob, yaml_files = _make_shuffled_rglob(tmp_path)
    monkeypatch.setattr(Path, "rglob", fake_rglob)

    result = discover_files_with_patterns(tmp_path, [])

    assert result == sorted(yaml_files)
    # Guard: assert it is genuinely sorted, not merely equal to insertion order.
    assert result == sorted(result)


@pytest.mark.unit
def test_discover_with_patterns_returns_sorted_regardless_of_iteration_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pattern-matched branch returns files in stable sorted order."""
    fake_rglob, yaml_files = _make_shuffled_rglob(tmp_path)
    monkeypatch.setattr(Path, "rglob", fake_rglob)

    # "**/*.yaml" matches every yaml file at any depth.
    result = discover_files_with_patterns(tmp_path, ["**/*.yaml"])

    assert result == sorted(yaml_files)
    assert result == sorted(result)
