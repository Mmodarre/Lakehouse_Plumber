"""Data quality transformation generator."""

from typing import List, Dict, Any
from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.dqe import DQEParser
import yaml


class DataQualityTransformGenerator(BaseActionGenerator):
    """Generate data quality transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
        self.dqe_parser = DQEParser()

    def generate(self, action: Action, flowgroup_config: Dict[str, Any]) -> str:
        """Generate data quality transform code."""
        # Data quality transforms require stream mode
        readMode = action.readMode or "stream"
        if readMode != "stream":
            raise ValueError(
                f"Data quality action '{action.name}' requires readMode='stream', got '{readMode}'"
            )

        # Read expectations from file
        expectations_file = action.expectations_file
        if not expectations_file:
            raise ValueError(
                f"Data quality transform '{action.name}' requires expectations_file"
            )

        expectations = self._load_expectations(action, flowgroup_config.get("spec_dir"))

        # Parse expectations based on format
        if expectations and isinstance(expectations, list):
            # Old format: list of dicts with constraint/type fields
            expect_all, expect_all_or_drop, expect_all_or_fail = (
                self.dqe_parser.parse_expectations(expectations)
            )
            fail_expectations = expect_all_or_fail
            drop_expectations = expect_all_or_drop
            warn_expectations = expect_all
        else:
            # New format: dict where key is constraint, value has action/name
            fail_expectations = {}
            drop_expectations = {}
            warn_expectations = {}

            for constraint, exp_config in (expectations or {}).items():
                action_type = exp_config.get("action", "warn").lower()
                name = exp_config.get("name", constraint)

                if action_type == "fail":
                    fail_expectations[name] = constraint
                elif action_type == "drop":
                    drop_expectations[name] = constraint
                else:  # warn or default
                    warn_expectations[name] = constraint

        # Extract source view
        source_view = self._extract_source_view(action.source)

        template_context = {
            "target_view": action.target,
            "source_view": source_view,
            "readMode": readMode,
            "fail_expectations": fail_expectations,
            "drop_expectations": drop_expectations,
            "warn_expectations": warn_expectations,
            "description": action.description
            or f"Data quality checks for {action.source}",
        }

        return self.render_template("transform/data_quality.py.j2", template_context)

    def _extract_source_view(self, source) -> str:
        """Extract source view name from source configuration."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            return source.get("view", source.get("source", ""))
        else:
            return ""

    def _load_expectations(self, action: Action, spec_dir: Path = None) -> List:
        """Load expectations from action configuration."""
        # Check if action has expectations as an attribute (from test)
        if hasattr(action, "expectations") and action.expectations:
            return action.expectations

        # Check if source is a dict with expectations
        if isinstance(action.source, dict) and "expectations" in action.source:
            return action.source["expectations"]

        # Check for expectations_file
        expectations_file = None
        if hasattr(action, "expectations_file"):
            expectations_file = action.expectations_file
        elif isinstance(action.source, dict) and "expectations_file" in action.source:
            expectations_file = action.source["expectations_file"]

        if expectations_file:
            expectations_file = Path(expectations_file)
            if not expectations_file.is_absolute() and spec_dir:
                expectations_file = spec_dir / expectations_file

            # Try to load the file
            if expectations_file.exists():
                with open(expectations_file, "r") as f:
                    data = yaml.safe_load(f)

                # Handle different formats
                if isinstance(data, dict):
                    # Check if it has 'expectations' key (old format)
                    if "expectations" in data:
                        return data["expectations"]
                    else:
                        # New format: direct dictionary of constraints
                        return data
                elif isinstance(data, list):
                    # Direct list of expectations
                    return data
            else:
                raise FileNotFoundError(
                    f"Expectations file not found: {expectations_file}"
                )

        # Return empty dict/list if no expectations found
        return {}
