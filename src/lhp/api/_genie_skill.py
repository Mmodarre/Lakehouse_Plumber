"""Install package resources through the authenticated Workspace API."""

from __future__ import annotations

import io
from pathlib import Path

from lhp.api._skill_assets import skill_package
from lhp.api._skill_export import check_existing


def install_files(payload: dict[str, bytes], skill_name: str = "lhp") -> Path:
    """Use notebook authentication in-process; never infer the user from the OS.

    :stability: provisional
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.workspace import ImportFormat, ObjectType

    skill_package(skill_name)
    workspace = WorkspaceClient()
    username = workspace.current_user.me().user_name
    if not username or "/" in username or username in {".", ".."}:
        raise ValueError("Could not determine a valid authenticated workspace user")
    destination = f"/Users/{username}/.assistant/skills/{skill_name}"
    existing: dict[str, bytes] = {}
    try:
        entries = list(workspace.workspace.list(destination, recursive=True))
    except NotFound:
        entries = []
    for entry in entries:
        if entry.object_type == ObjectType.DIRECTORY:
            continue
        if not entry.path or not entry.path.startswith(destination + "/"):
            raise ValueError("Workspace returned a file outside the skill destination")
        if entry.object_type != ObjectType.FILE:
            raise ValueError(
                f"Skill destination contains a non-file object: {entry.path}"
            )
        with workspace.workspace.download(entry.path) as stream:
            existing[entry.path[len(destination) + 1 :]] = stream.read()
    check_existing(existing, payload)
    for name, data in payload.items():
        target = f"{destination}/{name}"
        if name not in existing:
            workspace.workspace.mkdirs(target.rsplit("/", 1)[0])
            workspace.workspace.upload(
                target, io.BytesIO(data), format=ImportFormat.AUTO, overwrite=False
            )
        with workspace.workspace.download(target) as stream:
            if stream.read() != data:
                raise OSError(f"Skill verification failed: {target}")
    return Path(destination)
