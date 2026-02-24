import logging
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ruamel.yaml import YAML

logger = logging.getLogger(__name__)


class FlowgroupFileFormat(str, Enum):
    """How flowgroups are organized within a YAML file.

    LHP supports three formats for defining flowgroups in YAML files.
    The editor must detect and handle all three to avoid data loss.
    """
    SINGLE = "single"                  # One flowgroup per file
    MULTI_DOCUMENT = "multi_document"  # Multiple --- separated documents
    ARRAY_SYNTAX = "array_syntax"      # Single doc with flowgroups: list


@dataclass
class FlowgroupUpdateResult:
    """Result of a flowgroup update or delete operation.

    Returned by update_flowgroup and delete_flowgroup so callers
    do not need to re-read the file to determine its type.
    """
    is_multi_flowgroup_file: bool
    documents_in_file: int
    file_format: FlowgroupFileFormat


class YAMLEditor:
    """Round-trip safe YAML file editing with multi-flowgroup support.

    Uses ruamel.yaml to preserve comments, formatting, and key ordering
    when modifying YAML files. This is critical for a good editing UX.

    Handles all three flowgroup file formats:
    - Single: one flowgroup per file
    - Multi-document: --- separated documents (each with flowgroup: key)
    - Array syntax: single document with flowgroups: list and shared fields
    """

    def __init__(self):
        self._yaml = YAML()
        self._yaml.preserve_quotes = True

    def read(self, path: Path) -> Any:
        """Read a YAML file, preserving round-trip information."""
        with open(path) as f:
            return self._yaml.load(f)

    def read_all_documents(self, path: Path) -> List[Any]:
        """Read all YAML documents from a multi-document file."""
        with open(path) as f:
            return list(self._yaml.load_all(f))

    def write(self, path: Path, data: Any) -> None:
        """Write data to a YAML file, preserving formatting."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            self._yaml.dump(data, f)

    def write_all_documents(self, path: Path, documents: List[Any]) -> None:
        """Write multiple YAML documents to a file (--- separated)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            self._yaml.dump_all(documents, f)

    def detect_file_format(self, path: Path) -> FlowgroupFileFormat:
        """Detect the multi-flowgroup format of a YAML file.

        Reads the file once and categorizes it into one of three formats.
        """
        docs = self.read_all_documents(path)
        return self._detect_format_from_docs(docs)

    def _detect_format_from_docs(self, docs: List[Any]) -> FlowgroupFileFormat:
        """Categorize format from already-loaded documents."""
        # Check first document for array syntax (flowgroups: list)
        if docs and isinstance(docs[0], dict) and "flowgroups" in docs[0]:
            return FlowgroupFileFormat.ARRAY_SYNTAX

        flowgroup_docs = [
            d for d in docs
            if d and isinstance(d, dict) and "flowgroup" in d
        ]
        if len(flowgroup_docs) > 1:
            return FlowgroupFileFormat.MULTI_DOCUMENT

        return FlowgroupFileFormat.SINGLE

    def _count_flowgroups(
        self, docs: List[Any], fmt: FlowgroupFileFormat
    ) -> int:
        """Count the number of flowgroups in the loaded documents."""
        if fmt == FlowgroupFileFormat.ARRAY_SYNTAX:
            return len(docs[0].get("flowgroups", [])) if docs else 0
        return len([
            d for d in docs
            if d and isinstance(d, dict) and "flowgroup" in d
        ])

    def find_flowgroup_in_file(
        self, path: Path, flowgroup_name: str
    ) -> Tuple[int, Optional[Any]]:
        """Find a specific flowgroup document within a multi-doc file.

        Returns:
            Tuple of (document_index, document_data).
            index is -1 if not found.
        """
        docs = self.read_all_documents(path)
        for i, doc in enumerate(docs):
            if doc and isinstance(doc, dict) and doc.get("flowgroup") == flowgroup_name:
                return i, doc
        return -1, None

    def create_flowgroup(
        self,
        project_root: Path,
        pipeline: str,
        flowgroup_name: str,
        config: Dict[str, Any],
    ) -> Path:
        """Create a new flowgroup YAML file.

        Always creates a new 1:1 file at: pipelines/{pipeline}/{flowgroup_name}.yaml
        (Multi-flowgroup files are an advanced feature -- new flowgroups get their own file.)
        """
        file_path = project_root / "pipelines" / pipeline / f"{flowgroup_name}.yaml"
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Ensure required fields
        config.setdefault("pipeline", pipeline)
        config.setdefault("flowgroup", flowgroup_name)

        self.write(file_path, config)
        logger.info(f"Created flowgroup: {file_path}")
        return file_path

    def update_flowgroup(
        self,
        project_root: Path,
        flowgroup_path: Path,
        flowgroup_name: str,
        config: Dict[str, Any],
    ) -> FlowgroupUpdateResult:
        """Update a flowgroup -- handles all three file formats.

        Reads the file once, determines its format, performs the update,
        and returns metadata about the operation. This avoids the triple
        file read that would occur with separate detect + read + post-check.

        For all formats: merges the update config into the existing document,
        preserving keys like flowgroup/pipeline that aren't in the update.
        """
        # Single read
        docs = self.read_all_documents(flowgroup_path)
        fmt = self._detect_format_from_docs(docs)

        if fmt == FlowgroupFileFormat.ARRAY_SYNTAX:
            self._update_array_syntax(flowgroup_path, docs[0], flowgroup_name, config)
        elif fmt == FlowgroupFileFormat.MULTI_DOCUMENT:
            self._update_multi_document(flowgroup_path, docs, flowgroup_name, config)
        else:
            # Single-flowgroup file: merge update into existing document
            # to preserve flowgroup/pipeline keys and other fields
            existing = docs[0] if docs else {}
            if isinstance(existing, dict):
                existing.update(config)
                self.write(flowgroup_path, existing)
            else:
                self.write(flowgroup_path, config)

        logger.info(f"Updated flowgroup '{flowgroup_name}' in {flowgroup_path}")
        return FlowgroupUpdateResult(
            is_multi_flowgroup_file=(fmt != FlowgroupFileFormat.SINGLE),
            documents_in_file=self._count_flowgroups(docs, fmt),
            file_format=fmt,
        )

    def _update_multi_document(
        self,
        path: Path,
        docs: List[Any],
        flowgroup_name: str,
        config: Dict[str, Any],
    ) -> None:
        """Update a specific flowgroup within a --- multi-document file."""
        updated = False
        for i, doc in enumerate(docs):
            if (
                doc
                and isinstance(doc, dict)
                and doc.get("flowgroup") == flowgroup_name
            ):
                # Merge: update existing doc with new config fields
                doc.update(config)
                updated = True
                break

        if not updated:
            raise ValueError(
                f"Flowgroup '{flowgroup_name}' not found "
                f"in multi-document file {path}"
            )
        self.write_all_documents(path, docs)

    def _update_array_syntax(
        self,
        path: Path,
        doc: Dict[str, Any],
        flowgroup_name: str,
        config: Dict[str, Any],
    ) -> None:
        """Update a specific flowgroup within a flowgroups: array.

        Merges config into the matching entry in the flowgroups: list,
        preserving shared/inherited fields at the top level.
        """
        fg_list = doc.get("flowgroups", [])
        updated = False
        for i, fg in enumerate(fg_list):
            if isinstance(fg, dict) and fg.get("flowgroup") == flowgroup_name:
                # Merge: update existing entry with new config fields
                fg.update(config)
                updated = True
                break

        if not updated:
            raise ValueError(
                f"Flowgroup '{flowgroup_name}' not found "
                f"in array-syntax file {path}"
            )
        doc["flowgroups"] = fg_list
        self.write(path, doc)

    def delete_flowgroup(
        self,
        flowgroup_path: Path,
        flowgroup_name: str,
        project_root: Optional[Path] = None,
    ) -> FlowgroupUpdateResult:
        """Delete a flowgroup -- handles all three file formats.

        Reads the file once, determines its format, performs the deletion,
        and returns metadata about the operation.

        For single-flowgroup files: deletes the entire file (after verifying
        the flowgroup name matches).
        For multi-document files: removes only the matching document.
        For array-syntax files: removes only the matching entry from
        the flowgroups: list, preserving shared fields and siblings.
        If the last flowgroup is removed, deletes the file.

        Args:
            flowgroup_path: Path to the YAML file containing the flowgroup.
            flowgroup_name: Name of the flowgroup to delete.
            project_root: Stop directory for empty parent cleanup. If None,
                cleanup stops at the filesystem root.
        """
        # Single read
        docs = self.read_all_documents(flowgroup_path)
        fmt = self._detect_format_from_docs(docs)
        count = self._count_flowgroups(docs, fmt)

        if fmt == FlowgroupFileFormat.ARRAY_SYNTAX:
            self._delete_array_syntax(
                flowgroup_path, docs[0], flowgroup_name, project_root
            )
        elif fmt == FlowgroupFileFormat.MULTI_DOCUMENT:
            self._delete_multi_document(
                flowgroup_path, docs, flowgroup_name, project_root
            )
        else:
            # Single-flowgroup file: verify name matches before deleting
            doc = docs[0] if docs else {}
            if isinstance(doc, dict) and doc.get("flowgroup") != flowgroup_name:
                raise ValueError(
                    f"Flowgroup '{flowgroup_name}' not found in {flowgroup_path}"
                )
            self._delete_file_and_cleanup(flowgroup_path, stop_at=project_root)

        return FlowgroupUpdateResult(
            is_multi_flowgroup_file=(fmt != FlowgroupFileFormat.SINGLE),
            documents_in_file=count,
            file_format=fmt,
        )

    def _delete_multi_document(
        self,
        path: Path,
        docs: List[Any],
        flowgroup_name: str,
        project_root: Optional[Path],
    ) -> None:
        """Remove a flowgroup from a --- multi-document file."""
        remaining = [
            d for d in docs
            if not (d and isinstance(d, dict) and d.get("flowgroup") == flowgroup_name)
        ]

        if len(remaining) == len(docs):
            raise ValueError(
                f"Flowgroup '{flowgroup_name}' not found "
                f"in multi-document file {path}"
            )

        if not remaining or all(d is None for d in remaining):
            # Last flowgroup removed -- delete the file
            self._delete_file_and_cleanup(path, stop_at=project_root)
        else:
            self.write_all_documents(path, remaining)
            logger.info(
                f"Removed flowgroup '{flowgroup_name}' from multi-doc file: {path}"
            )

    def _delete_array_syntax(
        self,
        path: Path,
        doc: Dict[str, Any],
        flowgroup_name: str,
        project_root: Optional[Path],
    ) -> None:
        """Remove a flowgroup from a flowgroups: array-syntax file.

        Removes the matching entry from the flowgroups: list. If the
        list becomes empty, deletes the entire file.
        """
        fg_list = doc.get("flowgroups", [])
        remaining = [
            fg for fg in fg_list
            if not (isinstance(fg, dict) and fg.get("flowgroup") == flowgroup_name)
        ]

        if len(remaining) == len(fg_list):
            raise ValueError(
                f"Flowgroup '{flowgroup_name}' not found "
                f"in array-syntax file {path}"
            )

        if not remaining:
            # Last flowgroup removed -- delete the file
            self._delete_file_and_cleanup(path, stop_at=project_root)
        else:
            doc["flowgroups"] = remaining
            self.write(path, doc)
            logger.info(
                f"Removed flowgroup '{flowgroup_name}' from array-syntax file: {path}"
            )

    def _delete_file_and_cleanup(
        self, path: Path, stop_at: Optional[Path] = None
    ) -> None:
        """Delete a file and clean up empty parent directories.

        Stops climbing at `stop_at` (typically the project root).
        Never deletes `stop_at` itself. If stop_at is None, stops
        at the filesystem root.
        """
        if path.exists():
            path.unlink()
            logger.info(f"Deleted: {path}")

            stop_resolved = stop_at.resolve() if stop_at else None
            parent = path.parent
            while (
                parent != parent.parent  # filesystem root guard
                and (stop_resolved is None or parent.resolve() != stop_resolved)
                and not any(parent.iterdir())
            ):
                parent.rmdir()
                logger.debug(f"Removed empty directory: {parent}")
                parent = parent.parent

    def create_preset(
        self, project_root: Path, name: str, config: Dict[str, Any]
    ) -> Path:
        """Create a new preset YAML file at presets/{name}.yaml."""
        file_path = project_root / "presets" / f"{name}.yaml"
        self.write(file_path, config)
        logger.info(f"Created preset: {file_path}")
        return file_path

    def create_template(
        self, project_root: Path, name: str, config: Dict[str, Any]
    ) -> Path:
        """Create a new template YAML file at templates/{name}.yaml.

        Ensures the ``name`` field is present in the written content, since
        the Template Pydantic model requires it for validation.
        """
        # Inject 'name' so the template passes model validation on read-back
        if "name" not in config:
            config = {"name": name, **config}
        file_path = project_root / "templates" / f"{name}.yaml"
        self.write(file_path, config)
        logger.info(f"Created template: {file_path}")
        return file_path

    def create_environment(
        self, project_root: Path, env_name: str, tokens: Dict[str, Any]
    ) -> Path:
        """Create a new environment substitution file at substitutions/{env}.yaml."""
        file_path = project_root / "substitutions" / f"{env_name}.yaml"
        self.write(file_path, tokens)
        logger.info(f"Created environment: {file_path}")
        return file_path
