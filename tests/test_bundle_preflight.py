"""Unit tests for ``lhp.bundle.preflight``.

Assertions target the structured ``LHPConfigError.context["failures"]``
payload — not free-text in ``details`` — so the tests stay robust to
phrasing changes and exercise the public contract preflight provides to
machine consumers (CLI error display, future API surfaces).
"""

from pathlib import Path

import pytest
import yaml

from lhp.bundle.preflight import (
    require_pipeline_config_flag,
    validate_catalog_schema,
)
from lhp.utils.error_formatter import ErrorCategory, LHPConfigError


# ---------------------------------------------------------------------------
# Fixture helpers — same idiom as test_bundle_manager_simplified.py
# ---------------------------------------------------------------------------


def _write_pipeline_config(
    project_root: Path,
    *,
    project_defaults: dict | None = None,
    pipelines: dict[str, dict] | None = None,
) -> str:
    """Write a multi-doc pipeline_config.yaml; return its project-relative path."""
    docs: list[str] = []
    if project_defaults is not None:
        docs.append(yaml.safe_dump({"project_defaults": project_defaults}))
    for name, cfg in (pipelines or {}).items():
        docs.append(yaml.safe_dump({"pipeline": name, **cfg}))
    text = "\n---\n".join(docs) if docs else ""
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "pipeline_config.yaml"
    config_path.write_text(text, encoding="utf-8")
    return str(config_path.relative_to(project_root))


def _write_substitutions(
    project_root: Path, env: str, mappings: dict[str, str] | None = None
) -> None:
    subs_dir = project_root / "substitutions"
    subs_dir.mkdir(parents=True, exist_ok=True)
    body: dict = {env: mappings or {"placeholder": "value"}}
    (subs_dir / f"{env}.yaml").write_text(yaml.safe_dump(body), encoding="utf-8")


# ---------------------------------------------------------------------------
# B gate: require_pipeline_config_flag
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_require_pipeline_config_flag_raises_when_bundle_enabled_and_no_pc():
    with pytest.raises(LHPConfigError) as exc_info:
        require_pipeline_config_flag(
            bundle_enabled=True, pipeline_config_path=None
        )
    assert exc_info.value.code == "LHP-CFG-023"
    assert exc_info.value.code_number == "023"
    assert exc_info.value.category == ErrorCategory.CONFIG


@pytest.mark.unit
def test_require_pipeline_config_flag_passes_when_bundle_disabled():
    # No exception
    require_pipeline_config_flag(
        bundle_enabled=False, pipeline_config_path=None
    )


@pytest.mark.unit
def test_require_pipeline_config_flag_passes_when_pc_provided():
    # No exception
    require_pipeline_config_flag(
        bundle_enabled=True, pipeline_config_path="config/pipeline_config.yaml"
    )


# ---------------------------------------------------------------------------
# A+ validator: happy paths
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_passes_when_project_defaults_set(tmp_path: Path):
    """Only project_defaults sets catalog/schema; every pipeline inherits."""
    config = _write_pipeline_config(
        tmp_path,
        project_defaults={"catalog": "cat1", "schema": "sch1"},
        pipelines={"p1": {"serverless": True}, "p2": {"serverless": False}},
    )
    _write_substitutions(tmp_path, "dev")

    # No raise expected.
    validate_catalog_schema(
        project_root=tmp_path,
        pipeline_config_path=config,
        pipeline_names=["p1", "p2"],
        env="dev",
    )


@pytest.mark.unit
def test_validate_passes_when_per_pipeline_set(tmp_path: Path):
    """Per-pipeline catalog/schema, no project_defaults — should pass."""
    config = _write_pipeline_config(
        tmp_path,
        pipelines={
            "p1": {"catalog": "c1", "schema": "s1"},
            "p2": {"catalog": "c2", "schema": "s2"},
        },
    )
    _write_substitutions(tmp_path, "dev")

    validate_catalog_schema(
        project_root=tmp_path,
        pipeline_config_path=config,
        pipeline_names=["p1", "p2"],
        env="dev",
    )


@pytest.mark.unit
def test_validate_pipeline_overrides_project_defaults(tmp_path: Path):
    """Per-pipeline catalog/schema wins over project_defaults."""
    config = _write_pipeline_config(
        tmp_path,
        project_defaults={"catalog": "default_cat", "schema": "default_sch"},
        pipelines={"p1": {"catalog": "override_cat", "schema": "override_sch"}},
    )
    _write_substitutions(tmp_path, "dev")

    # Both pass; preflight does not inspect which won.
    validate_catalog_schema(
        project_root=tmp_path,
        pipeline_config_path=config,
        pipeline_names=["p1"],
        env="dev",
    )


# ---------------------------------------------------------------------------
# A+ validator: aggregation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_aggregates_both_missing(tmp_path: Path):
    """All three pipelines miss both catalog and schema."""
    config = _write_pipeline_config(
        tmp_path,
        # No project_defaults → DEFAULT_PIPELINE_CONFIG has no catalog/schema.
        pipelines={"p1": {}, "p2": {}, "p3": {}},
    )
    _write_substitutions(tmp_path, "dev")

    with pytest.raises(LHPConfigError) as exc_info:
        validate_catalog_schema(
            project_root=tmp_path,
            pipeline_config_path=config,
            pipeline_names=["p1", "p2", "p3"],
            env="dev",
        )

    assert exc_info.value.code == "LHP-CFG-026"
    failures = exc_info.value.context["failures"]
    assert failures["both_missing"] == ["p1", "p2", "p3"]
    assert failures["incomplete"] == []
    assert failures["empty_after_substitution"] == []
    assert exc_info.value.context["total_failures"] == 3


@pytest.mark.unit
def test_validate_aggregates_incomplete_pairing(tmp_path: Path):
    """catalog without schema → incomplete bucket."""
    config = _write_pipeline_config(
        tmp_path,
        pipelines={
            "p1": {"catalog": "only_cat"},
            "p2": {"schema": "only_sch"},
        },
    )
    _write_substitutions(tmp_path, "dev")

    with pytest.raises(LHPConfigError) as exc_info:
        validate_catalog_schema(
            project_root=tmp_path,
            pipeline_config_path=config,
            pipeline_names=["p1", "p2"],
            env="dev",
        )

    failures = exc_info.value.context["failures"]
    assert failures["both_missing"] == []
    assert {d["pipeline_name"] for d in failures["incomplete"]} == {"p1", "p2"}
    by_name = {d["pipeline_name"]: d for d in failures["incomplete"]}
    assert by_name["p1"]["catalog"] == "only_cat"
    assert by_name["p1"]["schema"] is None
    assert by_name["p2"]["catalog"] is None
    assert by_name["p2"]["schema"] == "only_sch"


@pytest.mark.unit
def test_validate_aggregates_empty_after_substitution(tmp_path: Path):
    """Whitespace-only values (truthy but ``.strip()`` empty) → empty_after_substitution.

    Note: a substitution token resolving to the empty string `""` is falsy, so
    it lands in ``both_missing`` / ``incomplete`` (matching the original
    ``BundleManager`` three-raise semantics). The ``empty_after_substitution``
    bucket exclusively catches whitespace-only values.
    """
    config = _write_pipeline_config(
        tmp_path,
        pipelines={"p1": {"catalog": "${ws_cat}", "schema": "${ws_sch}"}},
    )
    _write_substitutions(tmp_path, "dev", {"ws_cat": "   ", "ws_sch": "   "})

    with pytest.raises(LHPConfigError) as exc_info:
        validate_catalog_schema(
            project_root=tmp_path,
            pipeline_config_path=config,
            pipeline_names=["p1"],
            env="dev",
        )

    failures = exc_info.value.context["failures"]
    assert failures["both_missing"] == []
    assert failures["incomplete"] == []
    assert len(failures["empty_after_substitution"]) == 1
    diag = failures["empty_after_substitution"][0]
    assert diag["pipeline_name"] == "p1"


@pytest.mark.unit
def test_validate_groups_all_three_categories(tmp_path: Path):
    """A single error aggregates all three failure types."""
    config = _write_pipeline_config(
        tmp_path,
        pipelines={
            "missing_both": {},
            "missing_catalog": {"schema": "s"},
            # Whitespace token → truthy but stripped empty → empty_after_substitution.
            "empty_sub": {"catalog": "${ws}", "schema": "sch"},
        },
    )
    _write_substitutions(tmp_path, "dev", {"ws": "   "})

    with pytest.raises(LHPConfigError) as exc_info:
        validate_catalog_schema(
            project_root=tmp_path,
            pipeline_config_path=config,
            pipeline_names=["missing_both", "missing_catalog", "empty_sub"],
            env="dev",
        )

    failures = exc_info.value.context["failures"]
    assert failures["both_missing"] == ["missing_both"]
    assert [d["pipeline_name"] for d in failures["incomplete"]] == [
        "missing_catalog"
    ]
    assert [d["pipeline_name"] for d in failures["empty_after_substitution"]] == [
        "empty_sub"
    ]
    assert exc_info.value.context["total_failures"] == 3


# ---------------------------------------------------------------------------
# A+ validator: monitoring inclusion
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_includes_monitoring_pipeline_when_enabled(tmp_path: Path):
    """Misconfigured monitoring pipeline appears in failures."""
    config = _write_pipeline_config(
        tmp_path,
        project_defaults={"catalog": "cat", "schema": "sch"},
        # Override the monitoring pipeline to be missing catalog/schema.
        pipelines={"m1": {"catalog": None, "schema": None}, "p1": {}},
    )
    _write_substitutions(tmp_path, "dev")

    with pytest.raises(LHPConfigError) as exc_info:
        validate_catalog_schema(
            project_root=tmp_path,
            pipeline_config_path=config,
            pipeline_names=["p1"],
            env="dev",
            monitoring_pipeline_name="m1",
        )

    failures = exc_info.value.context["failures"]
    assert "m1" in failures["both_missing"]


@pytest.mark.unit
def test_validate_skips_monitoring_when_not_passed(tmp_path: Path):
    """No monitoring_pipeline_name → 'm1' not validated."""
    config = _write_pipeline_config(
        tmp_path,
        project_defaults={"catalog": "cat", "schema": "sch"},
        pipelines={"m1": {"catalog": None, "schema": None}, "p1": {}},
    )
    _write_substitutions(tmp_path, "dev")

    # No raise: m1 was not checked, p1 inherits valid project_defaults.
    validate_catalog_schema(
        project_root=tmp_path,
        pipeline_config_path=config,
        pipeline_names=["p1"],
        env="dev",
        monitoring_pipeline_name=None,
    )


@pytest.mark.unit
def test_validate_does_not_duplicate_monitoring_if_in_pipeline_list(tmp_path: Path):
    """If monitoring name is already in pipeline_names, it isn't checked twice."""
    config = _write_pipeline_config(
        tmp_path,
        pipelines={"m1": {}},  # both missing → 1 failure entry
    )
    _write_substitutions(tmp_path, "dev")

    with pytest.raises(LHPConfigError) as exc_info:
        validate_catalog_schema(
            project_root=tmp_path,
            pipeline_config_path=config,
            pipeline_names=["m1"],
            env="dev",
            monitoring_pipeline_name="m1",
        )

    failures = exc_info.value.context["failures"]
    assert failures["both_missing"] == ["m1"]
    assert exc_info.value.context["total_failures"] == 1


@pytest.mark.unit
def test_validate_runs_without_substitution_file(tmp_path: Path):
    """Missing substitution file is tolerated; catalog/schema literals still pass."""
    config = _write_pipeline_config(
        tmp_path,
        pipelines={"p1": {"catalog": "literal_cat", "schema": "literal_sch"}},
    )
    # Deliberately NOT writing substitutions/dev.yaml.

    validate_catalog_schema(
        project_root=tmp_path,
        pipeline_config_path=config,
        pipeline_names=["p1"],
        env="dev",
    )
