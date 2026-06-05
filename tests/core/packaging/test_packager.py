"""Contract tests for ``lhp.core.packaging.packager.PipelinePackager``.

These pins are load-bearing: ``PipelinePackager`` is the keystone seam that
turns a pipeline's in-memory outcomes into a content-addressed wheel. The
critical invariants are the arcname mapping (flowgroup modules under the
import package, ``custom_python_functions`` top-level — R6), DETERMINISM of the
wheel identity, and R4 byte-identity: every packaged ``.py`` is normalized
through the SAME ``normalize_content`` source mode writes to disk, so the wheel
matches a ``--no-format`` source build byte-for-byte (WHEEL_PACKAGING_SPEC §6).
"""

from __future__ import annotations

import dataclasses
import io
import zipfile
from pathlib import Path

import pytest

from lhp.core.packaging import PipelinePackager, WheelPackageResult
from lhp.models.processing import CopiedModuleRecord, FlowgroupOutcome
from lhp.utils.file_header import normalize_content

_PIPELINE = "02_bronze_ingest"
_ENV = "preprod"
_VERSION = "0.9.0"
# import_package_name lowercases, replaces non-identifier chars, and prefixes a
# leading digit with ``p_`` -> ``p_02_bronze_ingest``.
_IMPORT_PKG = "p_02_bronze_ingest"

_FLOWGROUP_CODE = (
    "import dlt\n\n\n@dlt.table\ndef customers():\n    return spark.range(1)\n"
)
_HELPER_CODE = "# LHP-SOURCE: pipelines/helpers/util.py\ndef helper():\n    return 1\n"
_INIT_CODE = "# LHP-SOURCE: pipelines/helpers/__init__.py\n"


def _outcome(output_dir: Path) -> FlowgroupOutcome:
    """A success outcome with one flowgroup module + a custom_python_functions
    closure (entry + a synthesized package ``__init__``)."""
    cpf_dir = output_dir / "custom_python_functions"
    return FlowgroupOutcome.ok(
        _PIPELINE,
        "customers",
        formatted_code=_FLOWGROUP_CODE,
        copy_records=[
            CopiedModuleRecord(
                source_path="pipelines/helpers/util.py",
                dest_path=cpf_dir / "util.py",
                content=_HELPER_CODE,
                module_path="pipelines/helpers/util.py",
                custom_functions_dir=cpf_dir,
            ),
            CopiedModuleRecord(
                source_path="pipelines/helpers/__init__.py",
                dest_path=cpf_dir / "__init__.py",
                content=_INIT_CODE,
                module_path="pipelines/helpers/util.py",
                custom_functions_dir=cpf_dir,
            ),
        ],
    )


def _wheel_members(wheel_bytes: bytes) -> dict[str, bytes]:
    """Read the built wheel back into an ``arcname -> bytes`` map."""
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as zf:
        return {name: zf.read(name) for name in zf.namelist()}


@pytest.mark.unit
class TestArcnameMapping:
    def test_arcnames_match_expected_layout(self, tmp_path: Path) -> None:
        """Flowgroup module under the import package; custom_python_functions
        TOP-LEVEL (incl. its own ``__init__``); a synth flowgroup ``__init__``."""
        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        members = _wheel_members(result.wheel_bytes)

        assert f"{_IMPORT_PKG}/customers.py" in members
        assert f"{_IMPORT_PKG}/__init__.py" in members  # synthesized
        assert "custom_python_functions/util.py" in members  # top-level (R6)
        assert "custom_python_functions/__init__.py" in members  # top-level (R6)

        # No flowgroup/helper member leaked into the wrong root.
        assert f"{_IMPORT_PKG}/util.py" not in members
        assert "custom_python_functions/customers.py" not in members

    def test_result_carries_runner_and_identity(self, tmp_path: Path) -> None:
        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        assert isinstance(result, WheelPackageResult)
        assert result.import_package == _IMPORT_PKG
        assert result.runner_filename == f"{_IMPORT_PKG}_runner.py"
        assert _IMPORT_PKG in result.runner_code
        assert len(result.content_hash) == 12
        assert result.wheel_filename.endswith("-py3-none-any.whl")
        assert result.content_hash in result.wheel_filename
        assert _ENV in result.wheel_filename
        # The wheel carries the ``lhp_`` brand prefix (escaped dist name).
        assert result.wheel_filename.startswith("lhp_")


@pytest.mark.unit
class TestDeterminism:
    def test_identical_inputs_identical_filename(self, tmp_path: Path) -> None:
        """Identical inputs -> identical wheel_filename (content-addressed)."""
        a = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        b = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        assert a.wheel_filename == b.wheel_filename
        assert a.content_hash == b.content_hash
        assert a.wheel_bytes == b.wheel_bytes


@pytest.mark.unit
class TestR4Normalization:
    def test_member_bytes_equal_normalized_input(self, tmp_path: Path) -> None:
        """R4: a flowgroup member's in-wheel bytes ==
        ``normalize_content(input).encode('utf-8')``."""
        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        members = _wheel_members(result.wheel_bytes)
        assert members[f"{_IMPORT_PKG}/customers.py"] == normalize_content(
            _FLOWGROUP_CODE
        ).encode("utf-8")

    def test_non_normalized_input_is_normalized_in_wheel(self, tmp_path: Path) -> None:
        """A non-normalized input (CRLF, trailing whitespace, no final newline)
        is normalized in the wheel — normalization is actually applied."""
        raw = (
            "import dlt   \r\ndef f():\r\n    return 1   "  # CRLF + trailing ws + no \n
        )
        assert raw != normalize_content(raw)  # guard: input really is non-normalized

        outcome = FlowgroupOutcome.ok(_PIPELINE, "raw_fg", formatted_code=raw)
        result = PipelinePackager().package(
            [outcome],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        members = _wheel_members(result.wheel_bytes)
        in_wheel = members[f"{_IMPORT_PKG}/raw_fg.py"]

        assert in_wheel == normalize_content(raw).encode("utf-8")
        assert b"\r" not in in_wheel  # CRLF stripped
        assert in_wheel.endswith(b"\n")  # final newline added
        assert b"   \n" not in in_wheel  # trailing whitespace trimmed


@pytest.mark.unit
class TestHashScope:
    def test_synth_init_excluded_from_hash(self, tmp_path: Path) -> None:
        """The synth ``<import_pkg>/__init__.py`` is excluded from the hash
        input but still present in the wheel (§6.4/§6.5)."""
        from lhp.core.packaging import content_hash

        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        members = _wheel_members(result.wheel_bytes)

        # Re-derive the hash over every NON-packaging member (drop the synth
        # __init__ and the whole .dist-info) and confirm it matches.
        synth = f"{_IMPORT_PKG}/__init__.py"
        hash_input = {
            arc: data
            for arc, data in members.items()
            if arc != synth and ".dist-info/" not in arc
        }
        assert content_hash(hash_input) == result.content_hash
        assert synth in members  # synth IS in the wheel


@pytest.mark.unit
class TestExtraMembers:
    """The two domain-agnostic buckets: ``extra_package_modules`` (under the
    flowgroup package) and ``extra_toplevel_files`` (wheel top-level). Both are
    byte-normalized like every other member and folded into the content hash.
    This is the seam the test-reporting hook (and any future generated sidecar)
    rides on — the packager itself stays domain-agnostic."""

    def test_extra_package_modules_under_flowgroup_package(
        self, tmp_path: Path
    ) -> None:
        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
            extra_package_modules={"_test_reporting_hook.py": "HOOK = 1\n"},
        )
        members = _wheel_members(result.wheel_bytes)
        assert f"{_IMPORT_PKG}/_test_reporting_hook.py" in members
        assert members[f"{_IMPORT_PKG}/_test_reporting_hook.py"] == b"HOOK = 1\n"
        # Did NOT leak to the top level.
        assert "_test_reporting_hook.py" not in members

    def test_extra_toplevel_files_at_wheel_root(self, tmp_path: Path) -> None:
        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
            extra_toplevel_files={
                "test_reporting_providers/__init__.py": "",
                "test_reporting_providers/p.py": "def publish():\n    return 1\n",
            },
        )
        members = _wheel_members(result.wheel_bytes)
        assert "test_reporting_providers/__init__.py" in members
        assert "test_reporting_providers/p.py" in members
        assert (
            members["test_reporting_providers/p.py"]
            == b"def publish():\n    return 1\n"
        )
        # Empty __init__ normalizes to empty bytes.
        assert members["test_reporting_providers/__init__.py"] == b""
        # Did NOT land under the flowgroup package.
        assert f"{_IMPORT_PKG}/test_reporting_providers/p.py" not in members

    def test_extra_members_are_byte_normalized(self, tmp_path: Path) -> None:
        """R4: both buckets are normalized through the SAME ``normalize_content``
        (CRLF + trailing whitespace + missing final newline all fixed)."""
        raw = "X = 1   \r\nY = 2"  # CRLF + trailing ws + no final newline
        assert raw != normalize_content(raw)  # guard: input really is non-normalized

        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
            extra_package_modules={"hook.py": raw},
            extra_toplevel_files={"providers/p.py": raw},
        )
        members = _wheel_members(result.wheel_bytes)
        expected = normalize_content(raw).encode("utf-8")
        assert members[f"{_IMPORT_PKG}/hook.py"] == expected
        assert members["providers/p.py"] == expected

    def test_extra_members_included_in_hash(self, tmp_path: Path) -> None:
        """Both buckets are real content: changing either changes the wheel
        identity (only the synth ``__init__`` + ``.dist-info`` are excluded)."""
        base = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        with_pkg = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
            extra_package_modules={"hook.py": "HOOK = 1\n"},
        )
        with_pkg_changed = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
            extra_package_modules={"hook.py": "HOOK = 2\n"},
        )
        with_top = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
            extra_toplevel_files={"providers/p.py": "P = 1\n"},
        )

        # Adding a member, and changing a member's content, both move the hash
        # (and therefore the content-addressed wheel_filename).
        assert with_pkg.content_hash != base.content_hash
        assert with_pkg.wheel_filename != base.wheel_filename
        assert with_pkg_changed.content_hash != with_pkg.content_hash
        assert with_pkg_changed.wheel_filename != with_pkg.wheel_filename
        assert with_top.content_hash != base.content_hash
        assert with_top.wheel_filename != base.wheel_filename


@pytest.mark.unit
class TestFrozen:
    def test_result_is_frozen(self, tmp_path: Path) -> None:
        result = PipelinePackager().package(
            [_outcome(tmp_path)],
            output_dir=tmp_path,
            pipeline=_PIPELINE,
            env=_ENV,
            version=_VERSION,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.content_hash = "deadbeef"  # type: ignore[misc]
