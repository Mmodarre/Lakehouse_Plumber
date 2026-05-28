"""Job-config loader for LHP orchestration-job generation.

Loads the user's custom job-config YAML (single- or multi-document) and
returns the parsed ``(project_defaults, job_specific_configs)`` tuple
that ``JobGenerator`` merges against its ``DEFAULT_JOB_CONFIG``.

Supports two file shapes:

1. Single-document (legacy / backward-compatible): the whole document is
   treated as ``project_defaults``.
2. Multi-document: one document with a ``project_defaults`` key becomes
   the global defaults; subsequent documents with a ``job_name`` key
   become per-job overrides (a ``job_name`` may be a string or list).
"""

import logging
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from ...errors import ErrorCategory, LHPFileError

logger = logging.getLogger(__name__)


class JobConfigLoader:
    """Loads job-config YAML for ``JobGenerator``.

    Pure loader — merging against ``DEFAULT_JOB_CONFIG`` is the caller's
    responsibility.
    """

    def load(
        self,
        project_root: Optional[Path],
        config_file_path: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """Load user's custom job config with multi-document support.

        Args:
            project_root: Root directory of the project. If ``None``, an
                empty ``({}, {})`` tuple is returned.
            config_file_path: Custom config file path relative to
                ``project_root``. If omitted, defaults to
                ``templates/bundle/job_config.yaml``.

        Returns:
            Tuple of ``(project_defaults, job_specific_configs_dict)``.

        Raises:
            LHPFileError: If an explicit ``config_file_path`` is provided
                but the file does not exist.
            LHPError: If a multi-document file has an empty ``job_name``
                list or duplicate ``job_name`` entries.
            yaml.YAMLError: If the config file has invalid YAML syntax.
        """
        if project_root is None:
            return {}, {}

        if config_file_path:
            full_config_path = project_root / config_file_path
            if not full_config_path.exists():
                raise LHPFileError(
                    category=ErrorCategory.IO,
                    code_number="001",
                    title="Job config file not found",
                    details=f"Job config file not found: {config_file_path} (looking in {project_root})",
                    suggestions=[
                        f"Ensure the file exists at: {full_config_path}",
                        "Check the file path for typos",
                        "Create the job config file if it doesn't exist",
                    ],
                    context={
                        "File Path": str(config_file_path),
                        "Project Root": str(project_root),
                    },
                )
        else:
            full_config_path = project_root / "templates" / "bundle" / "job_config.yaml"
            if not full_config_path.exists():
                logger.debug(
                    f"No custom job config found at {full_config_path}, using defaults"
                )
                return {}, {}

        try:
            with open(full_config_path, "r", encoding="utf-8") as f:
                documents = list(yaml.safe_load_all(f))

            documents = [doc for doc in documents if doc is not None]

            if not documents:
                logger.debug(
                    f"Empty config file at {full_config_path}, using defaults"
                )
                return {}, {}

            if len(documents) == 1:
                doc = documents[0]
                if "project_defaults" in doc:
                    logger.info(f"Loaded project_defaults from {full_config_path}")
                    return doc["project_defaults"], {}
                else:
                    # Legacy single-doc format — treat entire doc as project_defaults
                    logger.info(
                        f"Loaded single-document job config from {full_config_path}"
                    )
                    return doc, {}

            project_defaults: Dict[str, Any] = {}
            job_specific_configs: Dict[str, Dict[str, Any]] = {}
            seen_job_names: set = set()
            first_seen: Dict[str, int] = {}  # Track which document first defined each job_name

            for idx, doc in enumerate(documents):
                if "project_defaults" in doc:
                    project_defaults = doc["project_defaults"]
                    logger.info(f"Loaded project_defaults from document {idx+1}")

                elif "job_name" in doc:
                    job_names_raw = doc["job_name"]

                    # Normalize to list (support both string and list)
                    if isinstance(job_names_raw, str):
                        job_names = [job_names_raw]
                    elif isinstance(job_names_raw, list):
                        job_names = job_names_raw
                    else:
                        logger.warning(
                            f"Document {idx+1} has invalid job_name type: {type(job_names_raw)}. "
                            f"Expected string or list. Skipping."
                        )
                        continue

                    if not job_names:
                        from ...errors import LHPError

                        raise LHPError(
                            category=ErrorCategory.VALIDATION,
                            code_number="003",
                            title="Empty job_name list",
                            details=(
                                f"Document {idx+1} in {full_config_path} has an empty job_name list. "
                                f"At least one job name is required."
                            ),
                            suggestions=[
                                "Add at least one job name to the list",
                                "Use 'job_name: my_job' for a single job",
                                "Use 'job_name: [job1, job2]' for multiple jobs",
                            ],
                        )

                    job_config = {k: v for k, v in doc.items() if k != "job_name"}

                    for job_name in job_names:
                        if job_name in seen_job_names:
                            from ...errors import LHPError

                            raise LHPError(
                                category=ErrorCategory.VALIDATION,
                                code_number="004",
                                title="Duplicate job_name",
                                details=(
                                    f"job_name '{job_name}' in document {idx+1} was already defined "
                                    f"in document {first_seen[job_name]}. Each job_name must be unique "
                                    f"across all documents in {full_config_path}."
                                ),
                                suggestions=[
                                    f"Remove the duplicate '{job_name}' from one of the documents",
                                    "Ensure each job_name appears only once in the entire config file",
                                    "If you want to override a config, use the same job_name with different values",
                                ],
                                context={
                                    "duplicate_job_name": job_name,
                                    "first_defined_in_document": first_seen[job_name],
                                    "duplicate_in_document": idx + 1,
                                },
                            )

                        seen_job_names.add(job_name)
                        first_seen[job_name] = idx + 1

                        # Deep copy config for each job to ensure independence
                        job_specific_configs[job_name] = deepcopy(job_config)
                        logger.info(
                            f"Loaded job-specific config for '{job_name}' from document {idx+1}"
                        )

                else:
                    logger.warning(
                        f"Document {idx+1} in {full_config_path} has neither 'project_defaults' "
                        f"nor 'job_name' key - skipping"
                    )

            return project_defaults, job_specific_configs

        except yaml.YAMLError as e:
            logger.exception(
                f"Invalid YAML in job config file {full_config_path}: {e}"
            )
            raise
