"""Read-only saved-configuration inspection through the actual LHP resolvers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml


def _project_file(root: Path, path: str, *, required: bool = True) -> Path:
    candidate = (root / path).resolve()
    if not candidate.is_relative_to(root):
        raise PermissionError(
            "Configuration preview paths must stay inside the project."
        )
    if required and not candidate.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    return candidate


def preview_configuration(
    project_root: Path,
    *,
    path: str,
    kind: Literal["pipeline", "job"],
    env: str,
    target: str = "",
) -> dict[str, Any]:
    """Resolve saved source only; this operation never generates or writes files.

    Pipeline values use the same merge/event-log/substitution path as bundle
    generation. Job values use JobGenerator's merger; ordinary job generation
    intentionally leaves environment tokens for the bundle. Monitoring configs
    use their flat-document resolver, including LHP substitution.

    :stability: provisional
    """
    # This function is re-exported by lhp.api. Load its resolver/codegen stack
    # only when a preview is requested, keeping public API imports lightweight.
    from lhp.bundle.manager import BundleManager
    from lhp.core.jobs.job_generator import JobGenerator
    from lhp.core.loaders.project_config_loader import ProjectConfigLoader
    from lhp.core.processing.substitution import EnhancedSubstitutionManager

    root = Path(project_root).resolve()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", env) or env in {".", ".."}:
        raise ValueError("Choose an environment name without path separators.")
    config_path = _project_file(root, path)
    if config_path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError("Configuration preview requires a YAML file.")
    # Guard every indirect file read, including symlinks.
    _project_file(root, "lhp.yaml", required=False)
    substitution = _project_file(root, f"substitutions/{env}.yaml", required=False)
    relative = config_path.relative_to(root).as_posix()
    warnings: list[str] = [
        "Saved files only. Unsaved edits and sandbox transforms are not included.",
        "Generated resource names, paths, tasks and template-only defaults are not included.",
    ]
    tiers = ["Built-in defaults", f"{path}: project_defaults", "Target override"]

    if kind == "pipeline":
        project = ProjectConfigLoader(root).load_project_config()
        manager = BundleManager(
            root, pipeline_config_path=str(config_path), project_config=project
        )
        targets = sorted(manager.config_loader.pipeline_configs)
        selected = target or (targets[0] if targets else "")
        values = manager.resolve_pipeline_settings(selected, env)
        values["packaging"] = manager.config_loader.resolve_packaging_modes([selected])[
            selected
        ]
        tiers += ["Project event-log settings", f"substitutions/{env}.yaml"]
        warnings.append("Wheel artifact dependencies are added during generation.")
        if not substitution.is_file():
            warnings.append(
                "No substitution file exists for this environment; pipeline tokens remain unchanged."
            )
    elif kind == "job" and config_path.name.startswith("monitoring_job_config"):
        try:
            with config_path.open(encoding="utf-8") as source:
                raw = yaml.safe_load(source) or {}
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid monitoring job YAML: {exc}") from exc
        if not isinstance(raw, dict):
            raise ValueError(
                "A monitoring job configuration must contain one flat YAML mapping."
            )
        substitution_manager = EnhancedSubstitutionManager(
            substitution if substitution.is_file() else None, env
        )
        values = JobGenerator.resolve_monitoring_job_config(
            substitution_manager.substitute_yaml(raw)
        )
        targets = []
        selected = target
        tiers = ["Built-in job defaults", path, f"substitutions/{env}.yaml"]
    elif kind == "job":
        try:
            generator = JobGenerator(project_root=root, config_file_path=relative)
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid job YAML: {exc}") from exc
        targets = sorted(generator.job_specific_configs)
        selected = target or (targets[0] if targets else "")
        values = generator.get_job_config_for_job(selected)
        warnings.append(
            "Standard job configuration is merged without LHP environment substitution, matching job generation."
        )
        warnings.append(
            "Master-job controls apply from file defaults only; they do not change individual jobs."
        )
    else:
        raise ValueError("Configuration kind must be pipeline or job.")

    if selected and selected not in targets and targets:
        warnings.append(
            "This target has no explicit document; file and built-in defaults apply."
        )
    return {
        "path": path,
        "kind": kind,
        "env": env,
        "target": selected,
        "targets": targets,
        "values": values,
        "tiers": tiers,
        "warnings": warnings,
        "source": "saved",
    }
