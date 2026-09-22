"""Request-local saved-file consistency and containment for draft resolution."""

from __future__ import annotations

import hashlib
from os import stat_result
from pathlib import Path
from typing import Any

from lhp.core.loaders.external_file_loader import is_file_path
from lhp.core.validators.config_validator import ConfigValidator
from lhp.models import FlowGroup

from .template_authoring import project_path

MAX_DEPENDENCIES = 512
MAX_DEPENDENCY_BYTES = 8 * 1024 * 1024


class DependencySnapshot:
    """Remember content and file identity before reads; reject changes before return.

    Stat identity catches replacements and change/restore races as well as byte
    changes. No application cache is touched. All core read candidates are
    containment-checked before the corresponding loader/validator runs.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self._files: dict[str, tuple[Any, ...]] = {}
        self._preset_names: tuple[str, ...] | None = None

    def path(self, value: str) -> Path:
        candidate = Path(value)
        if candidate.is_absolute():
            try:
                value = candidate.relative_to(self.root).as_posix()
            except ValueError as exc:
                raise PermissionError(
                    "Preview dependencies must stay inside the project."
                ) from exc
        return project_path(self.root, value)

    def _state(self, value: str) -> tuple[Any, ...]:
        path = self.path(value)
        try:
            before = path.stat()
        except FileNotFoundError:
            return (None,)
        if not path.is_file():
            raise ValueError(f"Preview dependency is not a file: {value}")
        if before.st_size > MAX_DEPENDENCY_BYTES:
            raise ValueError(f"Preview dependency exceeds 8 MiB: {value}")
        with path.open("rb") as source:
            content = source.read(MAX_DEPENDENCY_BYTES + 1)
        after = path.stat()

        def identity(stat: stat_result) -> tuple[int, ...]:
            return (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )

        if identity(before) != identity(after) or len(content) > MAX_DEPENDENCY_BYTES:
            raise SourceChangedError(
                "A saved dependency changed while preview was reading it. Refresh preview."
            )
        return (hashlib.sha256(content).hexdigest(), *identity(after), str(path))

    def watch(self, value: str) -> Path:
        path = self.path(value)
        relative = Path(value)
        if relative.is_absolute():
            relative = relative.relative_to(self.root)
        key = relative.as_posix()
        if key not in self._files:
            if len(self._files) >= MAX_DEPENDENCIES:
                raise ValueError("Preview exceeds the limit of 512 saved dependencies.")
            self._files[key] = self._state(key)
        return path

    def preset_paths(self) -> list[Path]:
        directory = self.path("presets")
        names = tuple(
            sorted(path.name for path in directory.glob("*.yaml") if path.is_file())
        )
        self._preset_names = names
        return [self.watch("presets/" + name) for name in names]

    def assert_current(self) -> None:
        for path, before in self._files.items():
            if self._state(path) != before:
                raise SourceChangedError(
                    "Saved dependencies changed during preview. Refresh to use their current contents."
                )
        if self._preset_names is not None:
            names = tuple(
                sorted(
                    path.name
                    for path in self.path("presets").glob("*.yaml")
                    if path.is_file()
                )
            )
            if names != self._preset_names:
                raise SourceChangedError(
                    "The preset directory changed during preview. Refresh preview."
                )

    def views(self) -> list[dict[str, Any]]:
        result = [
            {"path": key, "fingerprint": state[0]}
            for key, state in sorted(self._files.items())
        ]
        if self._preset_names is not None:
            result.append(
                {
                    "path": "presets/",
                    "fingerprint": hashlib.sha256(
                        "\n".join(self._preset_names).encode()
                    ).hexdigest(),
                }
            )
        return result


class SourceChangedError(ValueError):
    """The response must not be labelled as representing current saved files."""


class PreviewConfigValidator(ConfigValidator):
    """Keep canonical validation while guarding/fingerprinting its file reads."""

    def __init__(self, snapshot: DependencySnapshot, project_config: Any) -> None:
        super().__init__(snapshot.root, project_config)
        self.snapshot = snapshot

    def validate_flowgroup(self, flowgroup: FlowGroup) -> list[Any]:
        for action in flowgroup.actions:
            raw = action.model_dump(mode="json", exclude_none=True)
            for key in ("schema_file", "expectations_file", "module_path", "sql_path"):
                value = raw.get(key)
                if isinstance(value, str) and value:
                    self.snapshot.watch(value)
            target = raw.get("write_target")
            if isinstance(target, dict):
                schema = target.get("table_schema")
                if isinstance(schema, str) and schema and is_file_path(schema):
                    self.snapshot.watch(schema)
        return super().validate_flowgroup(flowgroup)
