"""Generator for test reporting event hook files.

Produces a single _test_reporting_hook.py per pipeline that uses
@dp.on_event_hook to accumulate DQ expectation results and publish
them at terminal state via a user-supplied provider module.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ...models.config import ActionType, FlowGroup, ProjectConfig
from ...utils.error_formatter import ErrorCategory, LHPError
from ...utils.formatter import CodeFormatter
from ...utils.smart_file_writer import SmartFileWriter, build_lhp_source_header

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"

HOOK_FILENAME = "_test_reporting_hook.py"


class TestReportingHookGenerator:
    """Generates test reporting event hook files for pipelines.

    The hook file:
    1. Imports a user-supplied provider function
    2. Accumulates DQ expectation results from flow_progress events
    3. Publishes them at pipeline terminal state via the provider
    """

    __test__ = False  # Tell pytest this is not a test class

    def __init__(self, project_config: ProjectConfig, project_root: Path) -> None:
        self.project_config = project_config
        self.project_root = project_root
        self._jinja_env = Environment(  # nosec B701 — generates Python, not HTML
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            keep_trailing_newline=True,
        )
        self._formatter = CodeFormatter()

    @property
    def test_reporting_config(self):
        return self.project_config.test_reporting

    def generate(
        self,
        processed_flowgroups: List[FlowGroup],
        pipeline_name: str,
        output_dir: Path,
        smart_writer: SmartFileWriter,
    ) -> Optional[str]:
        """Generate the test reporting hook file for a pipeline.

        Args:
            processed_flowgroups: Flowgroups already processed for this pipeline
            pipeline_name: Pipeline name (used in hook header and context)
            output_dir: Pipeline output directory
            smart_writer: SmartFileWriter instance

        Returns:
            Rendered hook content, or None if hook should not be generated
        """
        if self.test_reporting_config is None:
            return None

        test_id_map = self._build_test_id_map(processed_flowgroups)

        if not test_id_map:
            logger.debug(
                f"Pipeline '{pipeline_name}': no test actions with test_id — "
                f"skipping hook generation"
            )
            return None

        provider_config = self._load_provider_config()
        self._copy_provider_module(output_dir, smart_writer)

        config = self.test_reporting_config
        provider_stem = Path(config.module_path).stem
        template = self._jinja_env.get_template("test_reporting/hook.py.j2")
        content = template.render(
            pipeline_name=pipeline_name,
            test_id_map_repr=repr(test_id_map),
            provider_config_repr=repr(provider_config),
            provider_stem=provider_stem,
            function_name=config.function_name,
        )

        try:
            content = self._formatter.format_code(content)
        except Exception as e:
            logger.warning(f"Black formatting failed for hook file: {e}")

        hook_path = output_dir / HOOK_FILENAME
        smart_writer.write_if_changed(hook_path, content)
        logger.info(f"Generated test reporting hook: {hook_path}")

        return content

    def validate(
        self,
        processed_flowgroups: Optional[List[FlowGroup]] = None,
        include_tests: bool = False,
    ) -> List[str]:
        """Validate test reporting configuration.

        Args:
            processed_flowgroups: Optional flowgroups (for --include-tests validation)
            include_tests: Whether to perform extended test-level validation

        Returns:
            List of validation error messages (empty = valid)
        """
        errors: List[str] = []

        if self.test_reporting_config is None:
            return errors

        config = self.test_reporting_config

        module_file = self.project_root / config.module_path
        if not module_file.exists():
            errors.append(
                f"test_reporting.module_path: file not found: {config.module_path}"
            )

        if config.config_file:
            config_path = self.project_root / config.config_file
            if not config_path.exists():
                errors.append(
                    f"test_reporting.config_file: file not found: {config.config_file}"
                )

        if include_tests and processed_flowgroups:
            test_id_map = self._build_test_id_map(processed_flowgroups)
            if not test_id_map:
                errors.append(
                    "test_reporting is configured but no test actions have test_id set"
                )

        return errors

    def _build_test_id_map(
        self, processed_flowgroups: List[FlowGroup]
    ) -> Dict[str, str]:
        """Build mapping from unqualified table name to external test_id.

        Only includes test actions that have test_id set.
        Uses Action.resolved_test_target for the canonical default target name.

        Raises:
            LHPError: If two test actions with test_id map to the same table name
        """
        test_id_map: Dict[str, str] = {}

        for fg in processed_flowgroups:
            for action in fg.actions:
                if action.type == ActionType.TEST and action.test_id:
                    table_name = action.resolved_test_target
                    if table_name in test_id_map:
                        raise LHPError(
                            category=ErrorCategory.CONFIG,
                            code_number="009",
                            title="Duplicate test_id table mapping",
                            details=(
                                f"Test actions '{table_name}' maps to both "
                                f"test_id '{test_id_map[table_name]}' and "
                                f"'{action.test_id}'"
                            ),
                            suggestions=[
                                "Each test action with test_id must have a unique target",
                                "Set an explicit 'target' on conflicting test actions",
                            ],
                        )
                    test_id_map[table_name] = action.test_id

        return test_id_map

    def _load_provider_config(self) -> Dict[str, Any]:
        """Load provider config from YAML file if specified."""
        config = self.test_reporting_config
        if not config or not config.config_file:
            return {}

        config_path = self.project_root / config.config_file
        if not config_path.exists():
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="009",
                title="Test reporting config file not found",
                details=f"Config file not found: {config_path}",
                suggestions=[
                    f"Create the config file at: {config.config_file}",
                    "Or remove config_file from test_reporting in lhp.yaml",
                ],
            )

        from ...utils.yaml_loader import load_yaml_file

        data = load_yaml_file(
            config_path,
            allow_empty=True,
            error_context="test reporting config file",
        )
        return data if isinstance(data, dict) else {}

    def _copy_provider_module(
        self, output_dir: Path, smart_writer: SmartFileWriter
    ) -> None:
        """Copy provider Python module to test_reporting_providers/ directory."""
        config = self.test_reporting_config
        source_file = self.project_root / config.module_path

        if not source_file.exists():
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="009",
                title="Test reporting provider module not found",
                details=f"Provider module not found: {source_file}",
                suggestions=[
                    f"Create the provider module at: {config.module_path}",
                    f"The module must define: {config.function_name}"
                    "(results, config, context, spark)",
                ],
            )

        providers_dir = output_dir / "test_reporting_providers"
        providers_dir.mkdir(parents=True, exist_ok=True)

        module_stem = Path(config.module_path).stem
        dest_file = providers_dir / f"{module_stem}.py"

        original_content = source_file.read_text()
        full_content = build_lhp_source_header(config.module_path) + original_content
        smart_writer.write_if_changed(dest_file, full_content)

        # SmartFileWriter handles no-op if content matches
        init_file = providers_dir / "__init__.py"
        smart_writer.write_if_changed(init_file, "")

        logger.debug(f"Copied provider module: {config.module_path} → {dest_file}")
