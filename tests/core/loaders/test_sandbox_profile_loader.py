"""Tests for ``load_sandbox_profile`` (personal ``.lhp/profile.yaml``).

A missing ``.lhp/profile.yaml`` raises ``LHPError`` with code ``LHP-IO-025``.
Malformed YAML, a non-mapping root, a missing top-level ``sandbox:`` key, or
a ``SandboxProfile`` validation failure (bad namespace, empty pipelines,
wrong types) raises ``LHP-CFG-064``. Glob entries in ``pipelines`` pass
through verbatim; unknown keys are ignored (pydantic default).
"""

from pathlib import Path

import pytest

from lhp.core.loaders import load_sandbox_profile
from lhp.errors import LHPError
from lhp.models import SandboxProfile


def _write_profile(project_root: Path, content: str) -> Path:
    """Write ``.lhp/profile.yaml`` under the given project root."""
    lhp_dir = project_root / ".lhp"
    lhp_dir.mkdir(parents=True, exist_ok=True)
    profile_path = lhp_dir / "profile.yaml"
    profile_path.write_text(content, encoding="utf-8")
    return profile_path


@pytest.mark.unit
class TestLoadSandboxProfileMissing:
    """A missing profile raises ``LHPError`` with code ``LHP-IO-025``."""

    def test_missing_file_raises_io_025(self, tmp_path):
        """No .lhp/ directory at all raises LHP-IO-025."""
        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-IO-025"

    def test_missing_file_in_existing_lhp_dir_raises_io_025(self, tmp_path):
        """An .lhp/ directory without profile.yaml raises LHP-IO-025."""
        (tmp_path / ".lhp").mkdir()

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-IO-025"

    def test_missing_file_error_names_expected_path_and_example(self, tmp_path):
        """The IO-025 error tells the developer what to create and where."""
        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        err = exc_info.value
        assert str(tmp_path / ".lhp" / "profile.yaml") in err.details
        assert err.example is not None
        assert "namespace" in err.example
        assert "pipelines" in err.example


@pytest.mark.unit
class TestLoadSandboxProfileValid:
    """Well-formed profiles load into a ``SandboxProfile``."""

    def test_valid_profile_returns_model_with_fields(self, tmp_path):
        """A valid profile round-trips namespace and pipelines onto the model."""
        _write_profile(
            tmp_path,
            "sandbox:\n"
            "  namespace: alice\n"
            "  pipelines:\n"
            "    - acmi_edw_bronze\n"
            "    - acmi_edw_silver\n",
        )

        profile = load_sandbox_profile(tmp_path)

        assert isinstance(profile, SandboxProfile)
        assert profile.namespace == "alice"
        assert profile.pipelines == ["acmi_edw_bronze", "acmi_edw_silver"]

    def test_glob_entries_pass_through_verbatim(self, tmp_path):
        """Glob patterns in pipelines are not expanded or altered by the loader."""
        _write_profile(
            tmp_path,
            "sandbox:\n"
            "  namespace: alice\n"
            "  pipelines:\n"
            "    - acmi_edw_bronze\n"
            '    - "acmi_edw_silv*"\n'
            '    - "*_gold"\n',
        )

        profile = load_sandbox_profile(tmp_path)

        assert profile.pipelines == ["acmi_edw_bronze", "acmi_edw_silv*", "*_gold"]

    def test_namespace_at_64_char_boundary_accepted(self, tmp_path):
        """A 64-character namespace (regex maximum) is accepted."""
        namespace = "a" * 64
        _write_profile(
            tmp_path,
            f"sandbox:\n  namespace: {namespace}\n  pipelines: [my_pipeline]\n",
        )

        profile = load_sandbox_profile(tmp_path)

        assert profile.namespace == namespace

    def test_extra_unknown_keys_are_ignored(self, tmp_path):
        """Unknown keys under sandbox are ignored (pydantic default, no extra='forbid')."""
        _write_profile(
            tmp_path,
            "sandbox:\n"
            "  namespace: alice\n"
            "  pipelines: [my_pipeline]\n"
            "  favourite_colour: green\n",
        )

        profile = load_sandbox_profile(tmp_path)

        assert profile.namespace == "alice"
        assert profile.pipelines == ["my_pipeline"]
        assert not hasattr(profile, "favourite_colour")

    def test_extra_top_level_keys_are_ignored(self, tmp_path):
        """Keys beside the top-level sandbox key are ignored by the loader."""
        _write_profile(
            tmp_path,
            "sandbox:\n"
            "  namespace: alice\n"
            "  pipelines: [my_pipeline]\n"
            "something_else: true\n",
        )

        profile = load_sandbox_profile(tmp_path)

        assert profile.namespace == "alice"


@pytest.mark.unit
class TestLoadSandboxProfileInvalid:
    """Structural and validation failures raise ``LHP-CFG-064``."""

    def test_malformed_yaml_raises_cfg_064(self, tmp_path):
        """Unparseable YAML raises LHP-CFG-064."""
        _write_profile(tmp_path, "sandbox: [unclosed\n  namespace: : :\n")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    @pytest.mark.parametrize(
        "content",
        [
            "- alice\n- bob\n",  # list root
            "just a string\n",  # scalar root
        ],
        ids=["list_root", "scalar_root"],
    )
    def test_non_mapping_root_raises_cfg_064(self, tmp_path, content):
        """A YAML root that is not a mapping raises LHP-CFG-064."""
        _write_profile(tmp_path, content)

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_missing_sandbox_key_raises_cfg_064(self, tmp_path):
        """Fields at the top level without a sandbox: key raise LHP-CFG-064."""
        _write_profile(tmp_path, "namespace: alice\npipelines: [my_pipeline]\n")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_empty_file_raises_cfg_064(self, tmp_path):
        """An empty profile.yaml raises LHP-CFG-064 (no sandbox key)."""
        _write_profile(tmp_path, "")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_non_mapping_sandbox_value_raises_cfg_064(self, tmp_path):
        """sandbox: with a scalar value raises LHP-CFG-064."""
        _write_profile(tmp_path, "sandbox: hello\n")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    @pytest.mark.parametrize(
        "namespace",
        [
            "Alice",  # uppercase
            "1alice",  # leading digit
            "a" * 65,  # exceeds 64-char maximum
            "ali-ce",  # hyphen not allowed
        ],
        ids=["uppercase", "leading_digit", "too_long", "hyphen"],
    )
    def test_bad_namespace_raises_cfg_064(self, tmp_path, namespace):
        """A namespace violating the identifier regex raises LHP-CFG-064."""
        _write_profile(
            tmp_path,
            f'sandbox:\n  namespace: "{namespace}"\n  pipelines: [my_pipeline]\n',
        )

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_empty_pipelines_list_raises_cfg_064(self, tmp_path):
        """An empty pipelines list raises LHP-CFG-064 (min_length=1)."""
        _write_profile(tmp_path, "sandbox:\n  namespace: alice\n  pipelines: []\n")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_pipelines_wrong_type_raises_cfg_064(self, tmp_path):
        """pipelines as a string instead of a list raises LHP-CFG-064."""
        _write_profile(
            tmp_path, "sandbox:\n  namespace: alice\n  pipelines: my_pipeline\n"
        )

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_missing_namespace_raises_cfg_064(self, tmp_path):
        """A sandbox block without namespace raises LHP-CFG-064."""
        _write_profile(tmp_path, "sandbox:\n  pipelines: [my_pipeline]\n")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"

    def test_missing_pipelines_raises_cfg_064(self, tmp_path):
        """A sandbox block without pipelines raises LHP-CFG-064."""
        _write_profile(tmp_path, "sandbox:\n  namespace: alice\n")

        with pytest.raises(LHPError) as exc_info:
            load_sandbox_profile(tmp_path)

        assert exc_info.value.code == "LHP-CFG-064"
