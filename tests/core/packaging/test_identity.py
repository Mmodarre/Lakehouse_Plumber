"""Identity-math contract tests for ``lhp.core.packaging.identity``.

These pins are load-bearing: the content hash, distribution name, and wheel
filename are content-addressed identities that callers (the commit seam, the
deploy upload-skip in WHEEL_PACKAGING_SPEC §9) depend on. A change to any
asserted value here is a deliberate identity break, not a free refactor.
"""

from __future__ import annotations

import pytest

from lhp.core.packaging import (
    content_hash,
    distribution_name,
    import_package_name,
    wheel_filename,
)

# A fixed 2-file payload and its FROZEN content hash. Computed once and
# hardcoded; this asserts the digest is stable across machines/runs (D7, R3).
_FIXED_PAYLOAD = {
    "b_pkg/mod_b.py": b"import dlt\n\nVALUE = 2\n",
    "a_pkg/mod_a.py": b"import dlt\n\nVALUE = 1\n",
}
_GOLDEN_HASH = "2871f94cecf7"


@pytest.mark.unit
class TestContentHash:
    def test_frozen_golden_hash(self) -> None:
        assert content_hash(_FIXED_PAYLOAD) == _GOLDEN_HASH

    def test_hash_is_12_hex_chars(self) -> None:
        result = content_hash(_FIXED_PAYLOAD)
        assert len(result) == 12
        assert all(c in "0123456789abcdef" for c in result)

    def test_reorder_invariance_mapping(self) -> None:
        """Same members in a different insertion order hash identically."""
        reordered = dict(reversed(list(_FIXED_PAYLOAD.items())))
        assert list(reordered) != list(_FIXED_PAYLOAD)  # order really differs
        assert content_hash(reordered) == _GOLDEN_HASH

    def test_accepts_iterable_of_pairs(self) -> None:
        """A list of (relpath, bytes) pairs hashes the same as the mapping."""
        as_pairs = list(reversed(list(_FIXED_PAYLOAD.items())))
        assert content_hash(as_pairs) == _GOLDEN_HASH

    def test_distinct_content_distinct_hash(self) -> None:
        mutated = dict(_FIXED_PAYLOAD)
        mutated["a_pkg/mod_a.py"] = b"import dlt\n\nVALUE = 99\n"
        assert content_hash(mutated) != _GOLDEN_HASH

    def test_relpath_participates_in_hash(self) -> None:
        """Moving identical bytes to a different relpath changes the hash."""
        renamed = {
            "z_pkg/mod_b.py": _FIXED_PAYLOAD["b_pkg/mod_b.py"],
            "a_pkg/mod_a.py": _FIXED_PAYLOAD["a_pkg/mod_a.py"],
        }
        assert content_hash(renamed) != _GOLDEN_HASH

    def test_empty_payload_is_stable(self) -> None:
        assert content_hash({}) == content_hash([])
        assert len(content_hash({})) == 12


@pytest.mark.unit
class TestImportPackageName:
    def test_leading_digit_yields_valid_identifier(self) -> None:
        result = import_package_name("15_python_load")
        assert result.isidentifier()

    def test_leading_digit_prefixed(self) -> None:
        assert import_package_name("02_bronze") == "p_02_bronze"

    @pytest.mark.parametrize(
        "raw",
        ["15_python_load", "02_bronze", "pre_brz_bp_1524", "MyPipeline", "a-b.c d"],
    )
    def test_always_a_valid_identifier(self, raw: str) -> None:
        assert import_package_name(raw).isidentifier()

    def test_lowercased_and_sanitized(self) -> None:
        assert import_package_name("My-Pipe.Line") == "my_pipe_line"

    def test_never_equals_reserved_package(self) -> None:
        result = import_package_name("custom_python_functions")
        assert result != "custom_python_functions"
        assert result.isidentifier()

    def test_sanitizes_to_reserved_then_avoids_it(self) -> None:
        """A name that *sanitizes* to the reserved package still avoids it."""
        result = import_package_name("Custom-Python.Functions")
        assert result != "custom_python_functions"
        assert result.isidentifier()

    def test_empty_name_is_valid_identifier(self) -> None:
        assert import_package_name("").isidentifier()


@pytest.mark.unit
class TestDistributionName:
    def test_pep503_canonical_uses_dashes(self) -> None:
        result = distribution_name(
            pipeline="pre_brz_bp_1524", env="preprod", content_hash="a1b2c3d4e5f6"
        )
        assert result == "lhp-pre-brz-bp-1524-preprod-a1b2c3d4e5f6"

    def test_distinct_from_import_package_name(self) -> None:
        dist = distribution_name(
            pipeline="15_python_load", env="dev", content_hash="0123456789ab"
        )
        imp = import_package_name("15_python_load")
        assert dist != imp


@pytest.mark.unit
class TestWheelFilename:
    def test_shape_ends_with_compat_tag(self) -> None:
        dist = distribution_name(
            pipeline="pre_brz_bp_1524", env="preprod", content_hash="a1b2c3d4e5f6"
        )
        fname = wheel_filename(dist_name=dist, version="0.9.0")
        assert fname.endswith("-py3-none-any.whl")

    def test_pep427_collapses_dashes_and_dots_in_dist(self) -> None:
        dist = distribution_name(
            pipeline="pre_brz_bp_1524", env="preprod", content_hash="a1b2c3d4e5f6"
        )
        fname = wheel_filename(dist_name=dist, version="0.9.0")
        # the canonical (dashed) dist part collapses to underscores
        assert fname.startswith("lhp_pre_brz_bp_1524_preprod_a1b2c3d4e5f6-")
        # the dist component itself carries no '-' or '.'
        dist_component = fname.split("-", 1)[0]
        assert "-" not in dist_component
        assert "." not in dist_component

    def test_version_dots_preserved(self) -> None:
        fname = wheel_filename(dist_name="pkg", version="0.9.0")
        assert fname == "pkg-0.9.0-py3-none-any.whl"

    def test_version_local_segment_stripped(self) -> None:
        """PEP 440 local segments (``+...``) are stripped from the filename."""
        fname = wheel_filename(dist_name="pkg", version="0.9.1.dev5+g1")
        assert fname == "pkg-0.9.1.dev5-py3-none-any.whl"
