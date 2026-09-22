"""Tests for :mod:`lhp.telemetry._identity`.

The reader is tolerant by contract: every malformed input degrades to the
"no identity" answer with a DEBUG record, never an exception. Project files
are authored per test under ``tmp_path`` so no fixture project's real name or
id ever reaches an assertion.
"""

import hashlib
import logging
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Optional

import pytest

from lhp.telemetry._identity import (
    PROJECT_ID_SALT,
    ProjectIdentity,
    env_class,
    hash_project_id,
    read_project_identity,
)

PROJECT_UUID = "8d3c1f4e-6a2b-4c5d-9e7f-0a1b2c3d4e5f"
BUNDLE_UUID = "1f2e3d4c-5b6a-4798-8a9b-0c1d2e3f4a5b"


def _project(
    root: Path,
    *,
    lhp_yaml: Optional[str] = None,
    databricks_yml: Optional[str] = None,
    environments: int = 0,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    if lhp_yaml is not None:
        (root / "lhp.yaml").write_text(lhp_yaml, "utf-8")
    if databricks_yml is not None:
        (root / "databricks.yml").write_text(databricks_yml, "utf-8")
    if environments:
        (root / "substitutions").mkdir()
        for n in range(environments):
            suffix = "yaml" if n % 2 == 0 else "yml"
            (root / "substitutions" / f"env{n}.{suffix}").write_text("{}", "utf-8")
    return root


# hashing


@pytest.mark.unit
def test_hash_is_the_salted_sha256_prefix() -> None:
    expected = hashlib.sha256(f"{PROJECT_ID_SALT}{PROJECT_UUID}".encode()).hexdigest()
    assert hash_project_id(PROJECT_UUID) == expected[:32]


@pytest.mark.unit
def test_hash_is_32_lowercase_hex_characters() -> None:
    digest = hash_project_id("anything")
    assert len(digest) == 32
    assert digest == digest.lower()
    int(digest, 16)


@pytest.mark.unit
def test_hash_is_deterministic_and_salted() -> None:
    assert hash_project_id(PROJECT_UUID) == hash_project_id(PROJECT_UUID)
    unsalted = hashlib.sha256(PROJECT_UUID.encode()).hexdigest()[:32]
    assert hash_project_id(PROJECT_UUID) != unsalted
    assert PROJECT_ID_SALT == "lhp-project:"


@pytest.mark.unit
def test_hash_normalises_case_and_surrounding_whitespace() -> None:
    assert hash_project_id("  My-Project \n") == hash_project_id("my-project")
    assert hash_project_id("my-project") != hash_project_id("my_project")


# read_project_identity


@pytest.mark.unit
def test_no_project_root_means_no_identity() -> None:
    assert read_project_identity(None) == ProjectIdentity(None, "none", False, {}, 0)


@pytest.mark.unit
def test_missing_files_mean_no_identity(tmp_path: Path) -> None:
    assert read_project_identity(tmp_path) == ProjectIdentity(
        None, "none", False, {}, 0
    )


@pytest.mark.unit
def test_a_nonexistent_root_means_no_identity(tmp_path: Path) -> None:
    identity = read_project_identity(tmp_path / "missing")
    assert identity.project_id is None and identity.source == "none"


@pytest.mark.unit
def test_project_id_in_lhp_yaml_wins(tmp_path: Path) -> None:
    root = _project(
        tmp_path,
        lhp_yaml=f"name: demo\nproject_id: {PROJECT_UUID}\n",
        databricks_yml=f"bundle:\n  name: demo\n  uuid: {BUNDLE_UUID}\n",
    )
    identity = read_project_identity(root)
    assert identity.project_id == hash_project_id(PROJECT_UUID)
    assert identity.source == "lhp_yaml"
    assert identity.bundle_present is True


@pytest.mark.unit
def test_bundle_uuid_is_the_second_choice(tmp_path: Path) -> None:
    root = _project(
        tmp_path,
        lhp_yaml="name: demo\n",
        databricks_yml=f"bundle:\n  name: demo\n  uuid: {BUNDLE_UUID}\n",
    )
    identity = read_project_identity(root)
    assert identity.project_id == hash_project_id(BUNDLE_UUID)
    assert identity.source == "bundle_uuid"


@pytest.mark.unit
def test_project_name_is_the_last_resort(tmp_path: Path) -> None:
    root = _project(tmp_path, lhp_yaml="name: Demo Project\n")
    identity = read_project_identity(root)
    assert identity.project_id == hash_project_id("Demo Project")
    assert identity.source == "name_hash"
    assert identity.bundle_present is False


@pytest.mark.unit
@pytest.mark.parametrize(
    "lhp_yaml",
    [
        "name: ''\nproject_id: ''\n",
        "name: 42\nproject_id: 42\n",
        "version: '1.0'\n",
        "name:\nproject_id:\n",
    ],
)
def test_empty_or_non_string_identity_fields_are_ignored(
    tmp_path: Path, lhp_yaml: str
) -> None:
    root = _project(tmp_path, lhp_yaml=lhp_yaml)
    identity = read_project_identity(root)
    assert identity.project_id is None
    assert identity.source == "none"


@pytest.mark.unit
def test_a_databricks_yaml_spelling_is_not_a_bundle(tmp_path: Path) -> None:
    root = _project(tmp_path, lhp_yaml="name: demo\n")
    (root / "databricks.yaml").write_text(f"bundle:\n  uuid: {BUNDLE_UUID}\n", "utf-8")
    identity = read_project_identity(root)
    assert identity.bundle_present is False
    assert identity.source == "name_hash"


@pytest.mark.unit
@pytest.mark.parametrize(
    "content",
    ["- just\n- a list\n", "name: [unclosed\n", "just a string\n", ""],
)
def test_malformed_lhp_yaml_is_tolerated(
    tmp_path: Path, content: str, caplog: pytest.LogCaptureFixture
) -> None:
    root = _project(tmp_path, lhp_yaml=content)
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        identity = read_project_identity(root)
    assert identity == ProjectIdentity(None, "none", False, {}, 0)
    assert all(record.levelno == logging.DEBUG for record in caplog.records)


@pytest.mark.unit
def test_undecodable_lhp_yaml_is_tolerated(tmp_path: Path) -> None:
    root = _project(tmp_path)
    (root / "lhp.yaml").write_bytes(b"name: \xff\xfe\n")
    assert read_project_identity(root).source == "none"


@pytest.mark.unit
def test_malformed_databricks_yml_still_yields_the_name_hash(tmp_path: Path) -> None:
    root = _project(tmp_path, lhp_yaml="name: demo\n", databricks_yml="bundle: [oops\n")
    identity = read_project_identity(root)
    assert identity.source == "name_hash"
    assert identity.bundle_present is True
    assert identity.target_modes == {}


@pytest.mark.unit
def test_target_modes_are_read_from_the_bundle_targets(tmp_path: Path) -> None:
    root = _project(
        tmp_path,
        lhp_yaml="name: demo\n",
        databricks_yml=(
            "bundle:\n  name: demo\n"
            "targets:\n"
            "  dev:\n    mode: development\n"
            "  prod:\n    mode: production\n"
            "  staging:\n    default: true\n"
            "  weird: not-a-mapping\n"
        ),
    )
    identity = read_project_identity(root)
    assert identity.target_modes == {"dev": "development", "prod": "production"}


@pytest.mark.unit
def test_environment_count_counts_yaml_and_yml_substitution_files(
    tmp_path: Path,
) -> None:
    root = _project(tmp_path, lhp_yaml="name: demo\n", environments=3)
    (root / "substitutions" / "README.md").write_text("ignored", "utf-8")
    assert read_project_identity(root).environment_count == 3


@pytest.mark.unit
def test_identity_is_frozen() -> None:
    identity = ProjectIdentity(None, "none", False, {}, 0)
    with pytest.raises(FrozenInstanceError):
        identity.source = "lhp_yaml"  # type: ignore[misc]


# env_class


@pytest.mark.unit
@pytest.mark.parametrize(
    ("databricks_yml", "env", "expected"),
    [
        (None, "dev", "none"),
        (None, None, "none"),
        ("targets:\n  dev:\n    mode: development\n", "dev", "development"),
        ("targets:\n  prod:\n    mode: production\n", "prod", "production"),
        ("targets:\n  dev:\n    mode: development\n", "prod", "unspecified"),
        ("targets:\n  dev:\n    default: true\n", "dev", "unspecified"),
        ("targets:\n  dev:\n    mode: staging\n", "dev", "unspecified"),
        ("bundle:\n  name: demo\n", None, "unspecified"),
        ("targets: [oops\n", "dev", "unspecified"),
    ],
)
def test_env_class(
    tmp_path: Path, databricks_yml: Optional[str], env: Optional[str], expected: str
) -> None:
    root = _project(tmp_path, lhp_yaml="name: demo\n", databricks_yml=databricks_yml)
    assert env_class(root, env) == expected


@pytest.mark.unit
def test_env_class_without_a_project_root_is_none() -> None:
    assert env_class(None, "dev") == "none"
