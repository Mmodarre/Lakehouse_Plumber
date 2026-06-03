"""Template loader for project initialization."""

import logging
from importlib.resources import files
from pathlib import Path
from typing import List

from jinja2 import Environment, PackageLoader

from .init_template_context import InitTemplateContext


class InitTemplateLoader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # PackageLoader uses importlib.util.find_spec (PEP 451) internally,
        # so it works identically for editable installs, wheels, and zipapps.
        self.jinja_env = Environment(  # nosec B701 — generates text, not HTML
            loader=PackageLoader("lhp", "templates/init"),
        )

    def load_template(self, template_path: str):
        try:
            return self.jinja_env.get_template(template_path)
        except Exception:
            self.logger.exception(f"Failed to load template {template_path}")
            raise

    def render_template(self, template_path: str, context: InitTemplateContext) -> str:
        try:
            template = self.load_template(template_path)

            context_dict = {
                "project_name": context.project_name,
                "current_date": context.current_date,
                "author": context.author,
                "bundle_enabled": context.bundle_enabled,
                "bundle_uuid": context.bundle_uuid,
            }

            return template.render(**context_dict)

        except Exception:
            self.logger.exception(f"Failed to render template {template_path}")
            raise

    def get_template_files(self, bundle_enabled: bool = False) -> List[str]:
        try:
            package_files = files("lhp.templates.init")
            template_files = []

            # Directories to exclude from initialization
            excluded_dirs = {"__pycache__"}

            def collect_files(current_path, relative_path=""):
                for item in current_path.iterdir():
                    if item.is_dir():
                        if item.name in excluded_dirs:
                            continue

                        if not bundle_enabled and item.name == "bundle":
                            continue

                        subdir_path = (
                            f"{relative_path}/{item.name}"
                            if relative_path
                            else item.name
                        )
                        collect_files(item, subdir_path)
                    elif item.is_file():
                        file_path = (
                            f"{relative_path}/{item.name}"
                            if relative_path
                            else item.name
                        )
                        template_files.append(file_path)

            collect_files(package_files)

            # Sort for consistent ordering
            template_files.sort()

            self.logger.debug(f"Discovered {len(template_files)} template files")
            return template_files

        except Exception:
            self.logger.exception("Failed to discover template files")
            # Fallback to basic files if discovery fails
            basic_files = [
                "lhp.yaml.j2",
                "substitutions/dev.yaml.j2",
                "presets/bronze_layer.yaml.j2",
                "templates/standard_ingestion.yaml.j2",
                "README.md.j2",
                ".gitignore.j2",
            ]
            if bundle_enabled:
                basic_files.extend(
                    ["bundle/databricks.yml.j2", "bundle/resources/.gitkeep"]
                )
            return basic_files

    def _copy_latest_schemas(self, project_path: Path):
        try:
            schemas_dir = project_path / ".vscode" / "schemas"
            schemas_dir.mkdir(parents=True, exist_ok=True)

            schema_files = {
                "flowgroup.schema.json",
                "template.schema.json",
                "substitution.schema.json",
                "project.schema.json",
                "preset.schema.json",
            }

            package_files = files("lhp.schemas")
            copied_count = 0

            for schema_file in schema_files:
                try:
                    source_file = package_files / schema_file
                    target_file = schemas_dir / schema_file

                    content = source_file.read_text(encoding="utf-8")
                    target_file.write_text(content, encoding="utf-8")
                    copied_count += 1

                except Exception as e:
                    self.logger.warning(f"Failed to copy schema {schema_file}: {e}")

            self.logger.debug(f"Copied {copied_count} schema files to {schemas_dir}")

        except Exception:
            self.logger.exception("Failed to copy schemas")
            # Don't fail the entire init process for schema copy errors

    def create_project_files(self, project_path: Path, context: InitTemplateContext):
        self.logger.info(f"Creating project files at {project_path}")

        template_files = self.get_template_files(context.bundle_enabled)

        for template_file in template_files:
            try:
                is_jinja_template = template_file.endswith(".j2")

                if template_file.endswith(".gitkeep"):
                    target_path = project_path / template_file.replace("bundle/", "")
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    target_path.write_text(
                        "# This file ensures the resources directory is tracked in git\n"
                    )
                    continue

                if is_jinja_template:
                    rendered_content = self.render_template(template_file, context)

                    target_file = template_file.replace(".j2", "")
                    if target_file.startswith("bundle/"):
                        target_file = target_file.replace("bundle/", "")

                    target_path = project_path / target_file

                    target_path.parent.mkdir(parents=True, exist_ok=True)

                    target_path.write_text(rendered_content, encoding="utf-8")
                else:
                    package_files = files("lhp.templates.init")
                    source_file = package_files / template_file

                    target_file = template_file
                    if target_file.startswith("bundle/"):
                        target_file = target_file.replace("bundle/", "")

                    target_path = project_path / target_file

                    target_path.parent.mkdir(parents=True, exist_ok=True)

                    content = source_file.read_text(encoding="utf-8")
                    target_path.write_text(content, encoding="utf-8")

                self.logger.debug(
                    f"Created file: {target_path.relative_to(project_path)}"
                )

            except Exception:
                self.logger.exception(
                    f"Failed to create file from template {template_file}"
                )
                raise

        self._copy_latest_schemas(project_path)

        self.logger.info(
            f"Successfully created project structure with {len(template_files)} files"
        )
