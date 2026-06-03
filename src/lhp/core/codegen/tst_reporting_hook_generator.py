"""Generator for test reporting event hook files.

Produces a single _test_reporting_hook.py per pipeline that uses
@dp.on_event_hook to accumulate DQ expectation results and publish
them at terminal state via a user-supplied provider module.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment

from lhp.models import ActionType, FlowGroup, ProjectConfig

from ...errors import ErrorFactory, codes
from ...utils.file_header import build_lhp_source_header, write_normalized
from ..processing.substitution import EnhancedSubstitutionManager
from .template_renderer import get_lhp_template_loader

logger = logging.getLogger(__name__)

HOOK_FILENAME = "_test_reporting_hook.py"


class TestReportingHookGenerator:
    """Generates ``_test_reporting_hook.py`` per pipeline.

    The hook imports a user-supplied provider function, accumulates DQ
    expectation results from flow_progress events, and publishes them at
    pipeline terminal state via the provider.
    """

    __test__ = False  # Tell pytest this is not a test class

    def __init__(self, project_config: ProjectConfig, project_root: Path) -> None:
        self.project_config = project_config
        self.project_root = project_root
        self._jinja_env = Environment(  # nosec B701 — generates Python, not HTML
            loader=get_lhp_template_loader(),
            keep_trailing_newline=True,
        )

    @property
    def test_reporting_config(self):
        return self.project_config.test_reporting

    def generate(
        self,
        processed_flowgroups: List[FlowGroup],
        pipeline_name: str,
        output_dir: Path,
        substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    ) -> Optional[str]:
        """Return rendered hook content, or None if no test_id was found."""
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
        self._copy_provider_module(output_dir, substitution_mgr)

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

        # NOT formatted here: the coordinator's single terminal ``ruff
        # format`` pass (formatter.format_generated_tree) formats the whole
        # output tree — including this hook — once, after commit. Writing the
        # rendered (unformatted) source verbatim keeps formatting in one place.
        hook_path = output_dir / HOOK_FILENAME
        write_normalized(hook_path, content)
        logger.info(f"Generated test reporting hook: {hook_path}")

        return content

    def validate(
        self,
        processed_flowgroups: Optional[List[FlowGroup]] = None,
        include_tests: bool = False,
    ) -> List[str]:
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

        Uses ``Action.resolved_test_target`` for the canonical default target name.

        Raises:
            LHPError: If two test actions with test_id map to the same table name.
        """
        test_id_map: Dict[str, str] = {}

        for fg in processed_flowgroups:
            for action in fg.actions:
                if action.type == ActionType.TEST and action.test_id:
                    table_name = action.resolved_test_target
                    if table_name in test_id_map:
                        raise ErrorFactory.config_error(
                            codes.CFG_009,
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
        config = self.test_reporting_config
        if not config or not config.config_file:
            return {}

        config_path = self.project_root / config.config_file
        if not config_path.exists():
            raise ErrorFactory.config_error(
                codes.CFG_009,
                title="Test reporting config file not found",
                details=f"Config file not found: {config_path}",
                suggestions=[
                    f"Create the config file at: {config.config_file}",
                    "Or remove config_file from test_reporting in lhp.yaml",
                ],
            )

        from ...parsers.yaml_loader import load_yaml_file

        data = load_yaml_file(
            config_path,
            allow_empty=True,
            error_context="test reporting config file",
        )
        return data if isinstance(data, dict) else {}

    def _copy_provider_module(
        self,
        output_dir: Path,
        substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    ) -> None:
        config = self.test_reporting_config
        source_file = self.project_root / config.module_path

        if not source_file.exists():
            raise ErrorFactory.config_error(
                codes.CFG_009,
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

        if substitution_mgr:
            original_content = substitution_mgr._process_string(original_content)

        full_content = build_lhp_source_header(config.module_path) + original_content
        write_normalized(dest_file, full_content)

        init_file = providers_dir / "__init__.py"
        write_normalized(init_file, "")

        logger.debug(f"Copied provider module: {config.module_path} → {dest_file}")
