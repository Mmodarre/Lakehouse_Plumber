"""DTO contract tests for ``lhp.api.responses.SkillInstallResult``.

Per constitution §8.3 + §9.15. The result is the public, frozen return
shape (a one-shot result DTO, §1.3) for installing the LHP Claude Code
skill into a project. Like :class:`WheelExtractionResult` — and unlike
:class:`DependencyOutputsResult`, which carries ``success`` / ``error_*``
and reports failure via ``success=False`` — the install operation
*raises* on failure (``LHP-CFG-011`` / ``LHP-IO-020``) and only ever
returns this result on success, so there are deliberately no ``success``
/ ``error_message`` / ``error_code`` fields (they would be permanently
dead state). Every field must be JSON-serialisable, the type is frozen,
and the projection must survive both pickle and JSON round-trips so
external consumers (CLI, future WebUI, future VSCode extension) can
capture the install outcome across process and wire boundaries.

Five contracts are covered:

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises ``FrozenInstanceError``.
3. Pickle round-trip with ``==`` equality.
4. JSON-style round-trip via flat field values, then field-by-field
   equality after reconstruction.
5. Field-type contract — no bare ``Any``, no ``Dict``/``List``, no
   ``Exception``/``LHPError``, no Pydantic ``BaseModel``; and the
   deliberate absence of ``success`` / ``error_*`` state.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Mapping, get_type_hints

import pytest

from lhp.api.responses import SkillInstallResult


def _install_to_json_safe_dict(view: SkillInstallResult) -> dict[str, object]:
    return {
        # Path → str for JSON-shape.
        "install_dir": str(view.install_dir),
        "skill_version": view.skill_version,
        "previous_version": view.previous_version,
        "action": view.action,
        # Tuple[str, ...] → list.
        "installed_files": list(view.installed_files),
        "routing_block_status": view.routing_block_status,
    }


def _install_from_json_safe_dict(
    payload: Mapping[str, object],
) -> SkillInstallResult:
    files_payload = payload["installed_files"]
    assert isinstance(files_payload, list)
    return SkillInstallResult(
        install_dir=Path(payload["install_dir"]),  # type: ignore[arg-type]
        skill_version=payload["skill_version"],  # type: ignore[arg-type]
        previous_version=payload["previous_version"],  # type: ignore[arg-type]
        action=payload["action"],  # type: ignore[arg-type]
        installed_files=tuple(files_payload),
        routing_block_status=payload["routing_block_status"],  # type: ignore[arg-type]
    )


@pytest.fixture
def fresh_install_result() -> SkillInstallResult:
    """A fresh project install (no prior version, routing block created)."""
    return SkillInstallResult(
        install_dir=Path("/proj/.claude/skills/lhp"),
        skill_version="1.2.3",
        previous_version=None,
        action="installed",
        installed_files=("SKILL.md", "references/quickstart.md"),
        routing_block_status="created",
    )


@pytest.fixture
def updated_install_result() -> SkillInstallResult:
    """A forced refresh of an existing install."""
    return SkillInstallResult(
        install_dir=Path("/proj/.claude/skills/lhp"),
        skill_version="2.0.0",
        previous_version="1.2.3",
        action="updated",
        installed_files=("SKILL.md", "references/errors.md"),
        routing_block_status="unchanged",
    )


@pytest.mark.unit
class TestFrozenContract:
    def test_skill_install_result_is_frozen(self) -> None:
        params = getattr(SkillInstallResult, "__dataclass_params__", None)
        assert params is not None, "SkillInstallResult has no __dataclass_params__"
        assert params.frozen is True


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_install_dir_mutation_raises(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            updated_install_result.install_dir = Path("/tampered")  # type: ignore[misc]

    def test_skill_version_mutation_raises(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            updated_install_result.skill_version = "9.9.9"  # type: ignore[misc]

    def test_previous_version_mutation_raises(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            updated_install_result.previous_version = None  # type: ignore[misc]

    def test_action_mutation_raises(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            updated_install_result.action = "installed"  # type: ignore[misc]

    def test_installed_files_mutation_raises(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            updated_install_result.installed_files = ()  # type: ignore[misc]

    def test_routing_block_status_mutation_raises(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            updated_install_result.routing_block_status = "updated"  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_fresh_install_result_pickles(
        self, fresh_install_result: SkillInstallResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(fresh_install_result))
        assert restored == fresh_install_result

    def test_updated_install_result_pickles(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(updated_install_result))
        assert restored == updated_install_result


@pytest.mark.unit
class TestJSONRoundTrip:
    def test_updated_install_result_json_round_trip(
        self, updated_install_result: SkillInstallResult
    ) -> None:
        projection = _install_to_json_safe_dict(updated_install_result)
        wire = json.loads(json.dumps(projection))
        restored = _install_from_json_safe_dict(wire)
        assert restored == updated_install_result
        # Field-by-field for clearer drift signal.
        assert restored.install_dir == updated_install_result.install_dir
        assert restored.skill_version == updated_install_result.skill_version
        assert restored.previous_version == updated_install_result.previous_version
        assert restored.action == updated_install_result.action
        assert restored.installed_files == updated_install_result.installed_files
        assert (
            restored.routing_block_status == updated_install_result.routing_block_status
        )

    def test_fresh_install_result_json_round_trip(
        self, fresh_install_result: SkillInstallResult
    ) -> None:
        projection = _install_to_json_safe_dict(fresh_install_result)
        wire = json.loads(json.dumps(projection))
        restored = _install_from_json_safe_dict(wire)
        assert restored == fresh_install_result


_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "DTOs must not carry live Exception instances (§4.8).",
    "LHPError": "DTOs must not carry LHPError instances (§4.8).",
    "BaseModel": "Public DTOs must not embed Pydantic models (§9.12).",
}


def _annotation_strings(cls: type) -> dict[str, str]:
    return {
        f.name: f.type if isinstance(f.type, str) else str(f.type)
        for f in dataclasses.fields(cls)
    }


@pytest.mark.unit
class TestFieldTypeContract:
    def test_skill_install_result_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(SkillInstallResult).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"SkillInstallResult.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_skill_install_result_no_any(self) -> None:
        for name, annotation in _annotation_strings(SkillInstallResult).items():
            assert "Any" not in annotation, (
                f"SkillInstallResult.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        hints = get_type_hints(SkillInstallResult)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"SkillInstallResult.{name}: resolved type {text!r} carries an "
                f"Exception."
            )
            assert "LHPError" not in text, (
                f"SkillInstallResult.{name}: resolved type {text!r} carries an "
                f"LHPError."
            )

    def test_skill_install_result_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(SkillInstallResult)}
        required = {
            "install_dir",
            "skill_version",
            "previous_version",
            "action",
            "installed_files",
            "routing_block_status",
        }
        missing = required - field_names
        assert not missing, f"SkillInstallResult is missing required fields: {missing}."

    def test_skill_install_result_has_no_success_or_error_state(self) -> None:
        # One-shot result DTO (§1.3): the install raises on failure (unlike
        # DependencyOutputsResult's success=False path), so success/error
        # fields would be permanently True/None dead state.
        field_names = {f.name for f in dataclasses.fields(SkillInstallResult)}
        banned = {"success", "error_message", "error_code"}
        present = banned & field_names
        assert not present, (
            f"SkillInstallResult must not carry one-shot dead state "
            f"{present} — the install raises on failure (§1.3)."
        )


@pytest.mark.unit
class TestPublicExport:
    def test_skill_install_result_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "SkillInstallResult" in api.__all__
        assert api.SkillInstallResult is SkillInstallResult
