"""LHP MCP server — exposes LHP validation, preset, and template tools.

This server is spawned by OpenCode as a child process (stdio transport).
It gives the AI assistant access to LHP-specific operations that go beyond
simple file read/write — schema validation, resolved config expansion,
preset listing, etc.

Usage (invoked by OpenCode, not directly by users):
    python -m lhp.mcp.server
"""

import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _get_project_root() -> Path:
    """Resolve the LHP project root from environment or cwd."""
    root = Path(os.environ.get("LHP_PROJECT_ROOT", ".")).resolve()
    if not (root / "lhp.yaml").exists():
        # Fall back to cwd
        root = Path.cwd()
    return root


def create_server():
    """Create and configure the MCP server with LHP tools."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        print(
            "Error: MCP SDK not installed. "
            "Install with: pip install 'lakehouse-plumber[ai]'",
            file=sys.stderr,
        )
        sys.exit(1)

    mcp = FastMCP("lhp", instructions="Lakehouse Plumber YAML pipeline tools")

    @mcp.tool()
    def lhp_validate_yaml(env: str = "dev") -> str:
        """Validate all LHP YAML configurations for the given environment.

        Returns validation results including any errors or warnings.
        Use this after editing flowgroup YAML files to check correctness.
        """
        try:
            from lhp.core.orchestrator import ActionOrchestrator

            root = _get_project_root()
            orchestrator = ActionOrchestrator(root, enforce_version=False)
            results = orchestrator.validate_configuration(env)

            if not results:
                return "Validation passed: all configurations are valid."

            output_lines = []
            for result in results:
                status = "ERROR" if getattr(result, "is_error", True) else "WARNING"
                msg = getattr(result, "message", str(result))
                output_lines.append(f"[{status}] {msg}")

            return "\n".join(output_lines)
        except Exception as e:
            return f"Validation failed: {e}"

    @mcp.tool()
    def lhp_show_resolved(flowgroup_name: str, env: str = "dev") -> str:
        """Show a flowgroup's resolved configuration after all expansions.

        Applies template rendering, preset merging, and substitution
        processing to show the final effective configuration. Useful for
        understanding what a flowgroup actually does after all layers
        of configuration are applied.
        """
        try:
            from lhp.core.orchestrator import ActionOrchestrator

            root = _get_project_root()
            orchestrator = ActionOrchestrator(root, enforce_version=False)
            flowgroups = orchestrator.discover_all_flowgroups(env)

            for fg in flowgroups:
                fg_name = getattr(fg, "flowgroup", getattr(fg, "name", None))
                if fg_name == flowgroup_name:
                    # Convert to dict for readable output
                    if hasattr(fg, "model_dump"):
                        data = fg.model_dump(exclude_none=True)
                    elif hasattr(fg, "dict"):
                        data = fg.dict(exclude_none=True)
                    else:
                        data = str(fg)
                    return json.dumps(data, indent=2, default=str)

            available = []
            for fg in flowgroups:
                name = getattr(fg, "flowgroup", getattr(fg, "name", "?"))
                available.append(name)

            return (
                f"Flowgroup '{flowgroup_name}' not found.\n"
                f"Available flowgroups: {', '.join(available)}"
            )
        except Exception as e:
            return f"Error resolving flowgroup: {e}"

    @mcp.tool()
    def lhp_list_presets() -> str:
        """List all available LHP presets with their descriptions.

        Presets provide reusable default configurations that are
        automatically merged by action type. Understanding available
        presets helps avoid redundant explicit configuration.
        """
        try:
            from lhp.presets.preset_manager import PresetManager

            root = _get_project_root()
            manager = PresetManager(root / "presets")
            presets = manager.list_presets()

            if not presets:
                return "No presets found in the presets/ directory."

            lines = ["Available presets:", ""]
            for preset in presets:
                name = getattr(preset, "name", str(preset))
                version = getattr(preset, "version", "")
                extends = getattr(preset, "extends", None)
                line = f"  - {name}"
                if version:
                    line += f" (v{version})"
                if extends:
                    line += f" [extends: {extends}]"
                lines.append(line)

            return "\n".join(lines)
        except Exception as e:
            return f"Error listing presets: {e}"

    @mcp.tool()
    def lhp_list_templates() -> str:
        """List all available LHP templates with their parameters.

        Templates are parameterized action macros. Knowing available
        templates and their parameters helps create flowgroups faster.
        """
        try:
            from lhp.core.template_engine import TemplateEngine

            root = _get_project_root()
            engine = TemplateEngine(root / "templates")
            templates = engine.list_templates()

            if not templates:
                return "No templates found in the templates/ directory."

            lines = ["Available templates:", ""]
            for tmpl in templates:
                name = getattr(tmpl, "name", str(tmpl))
                lines.append(f"  - {name}")
                info = engine.get_template_info(name)
                if info and hasattr(info, "parameters"):
                    for param_name, param_info in info.parameters.items():
                        required = getattr(param_info, "required", False)
                        default = getattr(param_info, "default", None)
                        desc = f"    {param_name}"
                        if required:
                            desc += " (required)"
                        elif default is not None:
                            desc += f" (default: {default})"
                        lines.append(desc)

            return "\n".join(lines)
        except Exception as e:
            return f"Error listing templates: {e}"

    @mcp.tool()
    def lhp_get_substitutions(env: str = "dev") -> str:
        """Show all substitution tokens available for the given environment.

        Substitution tokens use {token} or ${token} syntax and are
        defined in substitutions/<env>.yaml. Understanding available
        tokens prevents hardcoding values that should be environment-specific.
        """
        try:
            import yaml

            root = _get_project_root()
            sub_file = root / "substitutions" / f"{env}.yaml"

            if not sub_file.exists():
                available = [
                    f.stem
                    for f in (root / "substitutions").glob("*.yaml")
                    if f.stem != "__pycache__"
                ]
                return (
                    f"No substitution file found for env '{env}'.\n"
                    f"Available environments: {', '.join(available) or 'none'}"
                )

            with open(sub_file) as f:
                subs = yaml.safe_load(f) or {}

            lines = [f"Substitutions for '{env}' environment:", ""]
            for key, value in sorted(subs.items()):
                display_val = value if not _looks_like_secret(str(value)) else "***"
                lines.append(f"  ${{{key}}} = {display_val}")

            return "\n".join(lines)
        except Exception as e:
            return f"Error reading substitutions: {e}"

    return mcp


def _looks_like_secret(value: str) -> bool:
    """Heuristic to avoid exposing secrets in tool output."""
    secret_indicators = ["password", "secret", "token", "key", "credential"]
    lower = value.lower()
    return any(ind in lower for ind in secret_indicators)


def main() -> None:
    """Entry point for ``python -m lhp.mcp.server``."""
    server = create_server()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
