"""Saved previews use production merge/substitution rules without disk writes."""

import tempfile
import unittest
from pathlib import Path

from lhp.api import preview_configuration
from lhp.bundle.manager import BundleManager
from lhp.core.jobs.job_generator import JobGenerator


class ConfigurationPreviewTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "config").mkdir()
        (self.root / "substitutions").mkdir()
        (self.root / "lhp.yaml").write_text("name: preview_project\n")

    def write(self, name, content):
        path = self.root / "config" / name
        path.write_text(content)
        return path.relative_to(self.root).as_posix()

    def test_pipeline_uses_generator_resolution_and_preserves_disk(self):
        path = self.write(
            "pipeline_config.yaml",
            """project_defaults:
  catalog: ${catalog}
  schema: bronze
  configuration:
    keep: "yes"
    replace: defaults
---
pipeline: alpha
configuration:
  replace: explicit
""",
        )
        (self.root / "substitutions" / "dev.yaml").write_text(
            "dev:\n  catalog: dev_catalog\n"
        )
        before = {
            p.relative_to(self.root).as_posix(): p.read_bytes()
            for p in self.root.rglob("*")
            if p.is_file()
        }
        result = preview_configuration(
            self.root, path=path, kind="pipeline", env="dev", target="alpha"
        )
        manager = BundleManager(self.root, pipeline_config_path=path)
        expected = manager.resolve_pipeline_settings("alpha", "dev")
        expected["packaging"] = "source"
        self.assertEqual(result["values"], expected)
        self.assertEqual(result["values"]["catalog"], "dev_catalog")
        self.assertEqual(result["values"]["configuration"]["replace"], "explicit")
        self.assertEqual(result["targets"], ["alpha"])
        after = {
            p.relative_to(self.root).as_posix(): p.read_bytes()
            for p in self.root.rglob("*")
            if p.is_file()
        }
        self.assertEqual(before, after)

    def test_job_uses_production_merge_and_preserves_tokens(self):
        path = self.write(
            "job_config.yaml",
            """project_defaults:
  max_concurrent_runs: 2
  tags:
    env: ${env}
    team: platform
---
job_name: alpha
max_concurrent_runs: 4
tags:
  team: data
""",
        )
        result = preview_configuration(
            self.root, path=path, kind="job", env="dev", target="alpha"
        )
        expected = JobGenerator(
            project_root=self.root, config_file_path=path
        ).get_job_config_for_job("alpha")
        self.assertEqual(result["values"], expected)
        self.assertEqual(result["values"]["tags"]["env"], "${env}")
        self.assertEqual(result["values"]["max_concurrent_runs"], 4)

    def test_monitoring_requires_one_flat_document(self):
        path = self.write("monitoring_job_config.yaml", "queue:\n  enabled: false\n")
        result = preview_configuration(self.root, path=path, kind="job", env="dev")
        self.assertFalse(result["values"]["queue"]["enabled"])
        self.write("monitoring_job_config.yaml", "queue: {}\n---\ntags: {}\n")
        with self.assertRaises(ValueError):
            preview_configuration(self.root, path=path, kind="job", env="dev")

    def test_escapes_symlinks_and_invalid_environment_are_rejected(self):
        with self.assertRaises(PermissionError):
            preview_configuration(
                self.root, path="../outside.yaml", kind="pipeline", env="dev"
            )
        path = self.write("pipeline_config.yaml", "pipeline: alpha\n")
        with self.assertRaises(ValueError):
            preview_configuration(self.root, path=path, kind="pipeline", env="../prod")
        outside = self.root.parent / f"{self.root.name}-outside.yaml"
        outside.write_text("name: external\n")
        self.addCleanup(outside.unlink)
        (self.root / "lhp.yaml").unlink()
        (self.root / "lhp.yaml").symlink_to(outside)
        with self.assertRaises(PermissionError):
            preview_configuration(self.root, path=path, kind="pipeline", env="dev")

    def test_unknown_target_uses_defaults(self):
        path = self.write(
            "pipeline_config.yaml",
            "project_defaults:\n  channel: PREVIEW\n---\npipeline: alpha\nchannel: CURRENT\n",
        )
        result = preview_configuration(
            self.root, path=path, kind="pipeline", env="dev", target="beta"
        )
        self.assertEqual(result["values"]["channel"], "PREVIEW")
        self.assertTrue(
            any("no explicit document" in warning for warning in result["warnings"])
        )


if __name__ == "__main__":
    unittest.main()
