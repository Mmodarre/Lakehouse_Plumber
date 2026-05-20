"""Tests for Action Orchestrator - Step 4.5.8."""

import tempfile
from pathlib import Path

import pytest
import yaml

from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import Action, ActionType, FlowGroup, TransformType
from tests.helpers import read_generated_pipeline


class TestActionOrchestrator:
    """Test action orchestrator functionality."""

    def create_test_project(self, tmpdir):
        """Create a test project structure with sample files."""
        project_root = Path(tmpdir)

        # Create directories
        (project_root / "pipelines" / "test_pipeline").mkdir(parents=True)
        (project_root / "presets").mkdir()
        (project_root / "templates").mkdir()
        (project_root / "substitutions").mkdir()

        # Create substitution file
        substitutions = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze",
                "landing_path": "/mnt/dev/landing",
            },
            "secrets": {
                "default_scope": "dev_secrets",
                "scopes": {"db": "dev_db_secrets"},
            },
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(substitutions, f)

        # Create preset file
        preset = {
            "name": "bronze_layer",
            "version": "1.0",
            "defaults": {
                "load_actions": {
                    "cloudfiles": {
                        "schema_evolution_mode": "addNewColumns",
                        "rescue_data_column": "_rescued_data",
                    }
                }
            },
        }
        with open(project_root / "presets" / "bronze_layer.yaml", "w") as f:
            yaml.dump(preset, f)

        # Create a simple flowgroup
        flowgroup = {
            "pipeline": "test_pipeline",
            "flowgroup": "test_flowgroup",
            "presets": ["bronze_layer"],
            "actions": [
                {
                    "name": "load_customers",
                    "type": "load",
                    "target": "v_customers_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/customers",
                        "format": "json",
                    },
                },
                {
                    "name": "clean_customers",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": "v_customers_raw",
                    "target": "v_customers_clean",
                    "sql": "SELECT * FROM v_customers_raw WHERE is_valid = true",
                },
                {
                    "name": "write_customers",
                    "type": "write",
                    "source": "v_customers_clean",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        "table": "customers",
                        "create_table": True,
                    },
                },
            ],
        }
        with open(
            project_root / "pipelines" / "test_pipeline" / "test_flowgroup.yaml", "w"
        ) as f:
            yaml.dump(flowgroup, f)

        return project_root

    def test_orchestrator_initialization(self):
        """Test orchestrator initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            orchestrator = ActionOrchestrator(project_root)

            assert orchestrator.project_root == project_root
            assert orchestrator.yaml_parser is not None
            assert orchestrator.preset_manager is not None
            assert orchestrator.template_engine is not None
            assert orchestrator.action_registry is not None

    def test_discover_flowgroups(self):
        """Test flowgroup discovery."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            orchestrator = ActionOrchestrator(project_root)

            pipeline_dir = project_root / "pipelines" / "test_pipeline"
            flowgroups = orchestrator.discover_flowgroups(pipeline_dir)

            assert len(flowgroups) == 1
            assert flowgroups[0].flowgroup == "test_flowgroup"
            assert len(flowgroups[0].actions) == 3

    def test_generate_pipeline(self):
        """Test complete pipeline generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)
            orchestrator = ActionOrchestrator(project_root)

            # Generate pipeline
            output_dir = project_root / "generated"
            generated_files = read_generated_pipeline(
                orchestrator,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=output_dir,
            )

            # Verify files were generated
            assert len(generated_files) == 1
            assert "test_flowgroup.py" in generated_files

            # Verify generated code content
            code = generated_files["test_flowgroup.py"]

            # Check header
            assert "# Generated by LakehousePlumber" in code
            assert "# Pipeline: test_pipeline" in code
            assert "# FlowGroup: test_flowgroup" in code

            # Check imports
            assert "from pyspark import pipelines as dp" in code

            # Check generated functions
            assert "@dp.temporary_view()" in code
            assert "def v_customers_raw():" in code
            assert "def v_customers_clean():" in code

            # Check substitutions were applied
            assert "/mnt/dev/landing/customers" in code  # ${landing_path} substituted
            assert (
                'name="dev_catalog.bronze.customers"' in code
            )  # ${catalog}.${bronze_schema} substituted in table name

            # Check preset defaults were applied
            assert "addNewColumns" in code
            assert "_rescued_data" in code

    def test_flowgroup_with_secret_substitution(self):
        """Flowgroup with secret references generates runtime-correct Python.

        The post-pass (`SecretCodeGenerator`) decides between two emission
        modes based on string-literal context:

        * **Entire-value secret** — when a Python string literal's content
          is exactly the secret placeholder (e.g. ``"user"`` field), the
          literal is replaced with a *bare* ``dbutils.secrets.get(...)``
          call. JDBC/Kafka authentication paths require this form;
          a string literal containing the call text would be passed
          verbatim and runtime auth would fail.
        * **Embedded secret** — when the placeholder appears inside a
          larger string literal (e.g. URL with host placeholder), the
          literal is rewritten as an f-string so the dbutils call
          evaluates at runtime.

        This test asserts both forms appear correctly *and* that the
        regressed "wrapped string literal" form is absent.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)

            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "secret_flowgroup",
                "actions": [
                    {
                        "name": "load_from_db",
                        "type": "load",
                        "target": "v_db_data",
                        "source": {
                            "type": "jdbc",
                            "url": "jdbc:postgresql://${secret:db/host}:5432/mydb",
                            "user": "${secret:db/username}",
                            "password": "${secret:db/password}",
                            "driver": "org.postgresql.Driver",
                            "table": "customers",
                        },
                    },
                    {
                        "name": "write_data",
                        "type": "write",
                        "source": "v_db_data",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "silver",
                            "table": "customers",
                            "create_table": True,
                        },
                    },
                ],
            }
            with open(
                project_root / "pipelines" / "test_pipeline" / "secret_flowgroup.yaml",
                "w",
            ) as f:
                yaml.dump(flowgroup, f)

            orchestrator = ActionOrchestrator(project_root)
            generated_files = read_generated_pipeline(
                orchestrator,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            code = generated_files["secret_flowgroup.py"]

            # Entire-value fields must use the bare-call form. Black
            # normalizes top-level string quotes to double, so the
            # dbutils call uses double-quoted scope/key here.
            assert (
                '.option("user", dbutils.secrets.get(scope="dev_db_secrets", key="username"))'
                in code
            ), (
                "Expected bare dbutils call for entire-value 'user' field; "
                "generated code:\n" + code
            )
            assert (
                '.option("password", dbutils.secrets.get(scope="dev_db_secrets", key="password"))'
                in code
            ), (
                "Expected bare dbutils call for entire-value 'password' field; "
                "generated code:\n" + code
            )

            # The wrapped-string regression form must not appear: a string
            # literal whose content is the call text instead of the call
            # itself. Any of these substrings indicates the bug.
            for bad in (
                '.option("user", "dbutils.secrets.get',
                '.option("password", "dbutils.secrets.get',
                '.option("url", "dbutils.secrets.get',
            ):
                assert bad not in code, (
                    f"Found wrapped-string form ({bad!r}); secret post-pass "
                    "regressed. Generated code:\n" + code
                )

            # Embedded secret in the URL must become an f-string. Inside an
            # f-string interpolation the dbutils call uses single quotes so
            # it doesn't collide with the outer double-quoted f-string.
            assert (
                "f\"jdbc:postgresql://{dbutils.secrets.get(scope='dev_db_secrets', key='host')}:5432/mydb\""
                in code
            ), (
                "Expected f-string with embedded dbutils call for URL; "
                "generated code:\n" + code
            )

            # Compile check: the surviving syntactic contract.
            compile(code, "<string>", "exec")

    def test_template_expansion(self):
        """Test template expansion in flowgroup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)

            # Create a template
            template = {
                "name": "standard_ingestion",
                "version": "1.0",
                "parameters": [
                    {"name": "source_table", "type": "string", "required": True},
                    {"name": "target_catalog", "type": "string", "required": True},
                    {"name": "target_schema", "type": "string", "required": True},
                ],
                "actions": [
                    {
                        "name": "load_{{ source_table }}",
                        "type": "load",
                        "target": "v_{{ source_table }}_raw",
                        "source": {
                            "type": "delta",
                            "catalog": "source_cat",
                            "schema": "source",
                            "table": "{{ source_table }}",
                        },
                    },
                    {
                        "name": "write_{{ source_table }}",
                        "type": "write",
                        "source": "v_{{ source_table }}_raw",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "{{ target_catalog }}",
                            "schema": "{{ target_schema }}",
                            "table": "{{ source_table }}",
                            "create_table": True,
                        },
                    },
                ],
            }
            with open(project_root / "templates" / "standard_ingestion.yaml", "w") as f:
                yaml.dump(template, f)

            # Create flowgroup using template
            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "template_flowgroup",
                "use_template": "standard_ingestion",
                "template_parameters": {
                    "source_table": "orders",
                    "target_catalog": "silver_cat",
                    "target_schema": "silver",
                },
            }
            with open(
                project_root
                / "pipelines"
                / "test_pipeline"
                / "template_flowgroup.yaml",
                "w",
            ) as f:
                yaml.dump(flowgroup, f)

            orchestrator = ActionOrchestrator(project_root)
            generated_files = read_generated_pipeline(
                orchestrator,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            # Verify template was expanded
            code = generated_files["template_flowgroup.py"]
            assert "def v_orders_raw():" in code
            assert (
                'spark.read.table("source_cat.source.orders")' in code
            )  # Delta table reference
            assert (
                'name="silver_cat.silver.orders"' in code
            )  # Full table name in streaming table

    def test_validation_errors(self):
        """Test validation error handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)

            # Create invalid flowgroup (missing required fields)
            invalid_flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "invalid_flowgroup",
                "actions": [
                    {
                        "name": "invalid_action",
                        "type": "load",
                        # Missing target and source
                    }
                ],
            }
            with open(
                project_root / "pipelines" / "test_pipeline" / "invalid_flowgroup.yaml",
                "w",
            ) as f:
                yaml.dump(invalid_flowgroup, f)

            orchestrator = ActionOrchestrator(project_root)

            # Should raise validation error
            with pytest.raises(ValueError, match="validation failed"):
                orchestrator.generate_pipeline_by_field(
                    pipeline_field="test_pipeline", env="dev"
                )

    def test_dependency_resolution(self):
        """Test that actions are generated in dependency order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self.create_test_project(tmpdir)

            # Create flowgroup with complex dependencies
            flowgroup = {
                "pipeline": "test_pipeline",
                "flowgroup": "dependency_flowgroup",
                "actions": [
                    {
                        "name": "join_ab",
                        "type": "transform",
                        "transform_type": "sql",
                        "source": ["v_a", "v_b"],
                        "target": "v_ab",
                        "sql": "SELECT * FROM v_a JOIN v_b ON v_a.id = v_b.id",
                    },
                    {
                        "name": "load_b",
                        "type": "load",
                        "target": "v_b",
                        "source": {
                            "type": "delta",
                            "catalog": "test_cat",
                            "schema": "test_schema",
                            "table": "table_b",
                        },
                    },
                    {
                        "name": "load_a",
                        "type": "load",
                        "target": "v_a",
                        "source": {
                            "type": "delta",
                            "catalog": "test_cat",
                            "schema": "test_schema",
                            "table": "table_a",
                        },
                    },
                    {
                        "name": "write_result",
                        "type": "write",
                        "source": "v_ab",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "gold",
                            "table": "result",
                            "create_table": True,
                        },
                    },
                ],
            }
            with open(
                project_root
                / "pipelines"
                / "test_pipeline"
                / "dependency_flowgroup.yaml",
                "w",
            ) as f:
                yaml.dump(flowgroup, f)

            orchestrator = ActionOrchestrator(project_root)
            generated_files = read_generated_pipeline(
                orchestrator,
                pipeline_field="test_pipeline",
                env="dev",
                output_dir=project_root / "generated",
            )

            code = generated_files["dependency_flowgroup.py"]

            # Find positions of function definitions
            pos_a = code.find("def v_a():")
            pos_b = code.find("def v_b():")
            pos_ab = code.find("def v_ab():")

            # Verify dependency order: loads before join
            assert pos_a < pos_ab
            assert pos_b < pos_ab


class TestOrchestratorDependencyInjection:
    """Test orchestrator dependency injection functionality."""

    def test_orchestrator_with_default_dependencies(self):
        """Test orchestrator initialization with default dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            (project_root / "substitutions").mkdir()

            # Should work with default dependencies
            orchestrator = ActionOrchestrator(project_root)

            # Verify dependencies are set
            assert orchestrator.dependencies is not None
            assert hasattr(orchestrator.dependencies, "substitution_factory")

    def test_orchestrator_with_custom_dependencies(self):
        """Test orchestrator initialization with custom dependencies."""
        from unittest.mock import Mock

        from lhp.core.factories import (
            DefaultSubstitutionFactory,
            OrchestrationDependencies,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            (project_root / "substitutions").mkdir()

            # Create custom dependencies
            mock_substitution_factory = Mock()
            custom_deps = OrchestrationDependencies(
                substitution_factory=mock_substitution_factory,
            )

            # Initialize with custom dependencies
            orchestrator = ActionOrchestrator(project_root, dependencies=custom_deps)

            # Verify custom dependencies are used
            assert (
                orchestrator.dependencies.substitution_factory
                == mock_substitution_factory
            )

    def test_dependency_factories_work(self):
        """Test that dependency factories can create instances."""
        from lhp.core.factories import (
            DefaultSubstitutionFactory,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            substitution_file = Path(tmpdir) / "test.yaml"
            substitution_file.write_text("test: value")

            # Test substitution factory
            sub_factory = DefaultSubstitutionFactory()
            sub_manager = sub_factory.create(substitution_file, "test")
            assert sub_manager is not None


class TestOrchestratorWithPipelineConfig:
    """Test ActionOrchestrator accepts and uses pipeline config."""

    def test_orchestrator_init_without_pipeline_config(self):
        """Orchestrator works without config (backward compatible)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Create minimal project structure
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()

            # Initialize without pipeline config
            orchestrator = ActionOrchestrator(project_root, enforce_version=False)

            # Should initialize successfully
            assert orchestrator.project_root == project_root
            # pipeline_config_path should be None by default
            assert hasattr(orchestrator, "pipeline_config_path")
            assert orchestrator.pipeline_config_path is None

    def test_orchestrator_init_with_pipeline_config(self):
        """Orchestrator accepts pipeline_config_path parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Create minimal project structure
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()

            config_path = "templates/bundle/pipeline_config.yaml"

            # Initialize with pipeline config
            orchestrator = ActionOrchestrator(
                project_root, enforce_version=False, pipeline_config_path=config_path
            )

            # Config path should be stored
            assert orchestrator.pipeline_config_path == config_path

    def test_orchestrator_stores_config_path(self):
        """Orchestrator stores config_path as instance variable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Create minimal project structure
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()

            config_path = "custom/path/config.yaml"
            orchestrator = ActionOrchestrator(
                project_root, enforce_version=False, pipeline_config_path=config_path
            )

            # Should be accessible as instance attribute
            assert hasattr(orchestrator, "pipeline_config_path")
            assert orchestrator.pipeline_config_path == config_path

    def test_sync_bundle_resources_uses_config(self):
        """_sync_bundle_resources passes config to BundleManager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Create full project structure for bundle support
            (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'")
            (project_root / "pipelines").mkdir()
            (project_root / "databricks.yml").write_text(
                "workspace:\n  host: https://test"
            )

            output_dir = project_root / "generated"
            output_dir.mkdir()

            config_path = "test_config.yaml"
            orchestrator = ActionOrchestrator(
                project_root, enforce_version=False, pipeline_config_path=config_path
            )

            # Mock the BundleManager to verify config is passed
            from unittest.mock import MagicMock, patch

            with patch("lhp.bundle.manager.BundleManager") as mock_bundle_manager_class:
                mock_manager_instance = MagicMock()
                mock_bundle_manager_class.return_value = mock_manager_instance

                # Call sync method
                orchestrator._sync_bundle_resources(output_dir, "dev")

                # Verify BundleManager was created with config path and project_config
                mock_bundle_manager_class.assert_called_once_with(
                    project_root,
                    config_path,
                    project_config=orchestrator.project_config,
                )


class TestGeneratePipelinesByFields:
    """Tests for the flat-pool plural method ``generate_pipelines_by_fields``.

    Runs Phase A across all pipelines in one flat ProcessPoolExecutor
    (under a ``spawn`` mp context) and Phase B (validation + file write
    + state save) per pipeline on the main thread. The shim
    ``generate_pipeline_by_field`` delegates to this method.
    """

    def _build_project(self, tmpdir, pipeline_names):
        """Build a multi-pipeline project with one flowgroup per pipeline.

        Each flowgroup performs the same load + transform + write pattern so
        the comparison ``plural-output == repeated-single-output`` is a
        meaningful byte-identical test.
        """
        project_root = Path(tmpdir)
        (project_root / "presets").mkdir()
        (project_root / "templates").mkdir()
        (project_root / "substitutions").mkdir()
        for name in pipeline_names:
            (project_root / "pipelines" / name).mkdir(parents=True)

        substitutions = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze",
                "landing_path": "/mnt/dev/landing",
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(substitutions, f)

        for name in pipeline_names:
            flowgroup = {
                "pipeline": name,
                "flowgroup": f"{name}_fg",
                "actions": [
                    {
                        "name": f"load_{name}",
                        "type": "load",
                        "target": f"v_{name}_raw",
                        "source": {
                            "type": "cloudfiles",
                            "path": "${landing_path}/" + name,
                            "format": "json",
                        },
                    },
                    {
                        "name": f"clean_{name}",
                        "type": "transform",
                        "transform_type": "sql",
                        "source": f"v_{name}_raw",
                        "target": f"v_{name}_clean",
                        "sql": f"SELECT * FROM v_{name}_raw",
                    },
                    {
                        "name": f"write_{name}",
                        "type": "write",
                        "source": f"v_{name}_clean",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": name,
                            "create_table": True,
                        },
                    },
                ],
            }
            with open(project_root / "pipelines" / name / f"{name}_fg.yaml", "w") as f:
                yaml.dump(flowgroup, f)

        return project_root

    def test_plural_matches_repeated_single_call(self):
        """Plural call output is byte-identical to repeated single-pipeline calls."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["p_alpha", "p_beta", "p_gamma"])

            # Reference: repeated single-pipeline calls via the shim
            ref_orch = ActionOrchestrator(project_root, max_workers=1)
            ref_out_dir = project_root / "ref"
            single = {
                name: ref_orch.generate_pipeline_by_field(
                    pipeline_field=name,
                    env="dev",
                    output_dir=ref_out_dir,
                )
                for name in ["p_alpha", "p_beta", "p_gamma"]
            }

            # Plural: one call across all 3
            plural_orch = ActionOrchestrator(project_root, max_workers=4)
            plural_out_dir = project_root / "plural"
            plural = plural_orch.generate_pipelines_by_fields(
                pipeline_fields=["p_alpha", "p_beta", "p_gamma"],
                env="dev",
                output_dir=plural_out_dir,
            )

            # Same set of pipelines, same files, same contents.
            # Filenames travel back via the return; content is read from disk.
            assert set(plural.keys()) == set(single.keys())
            for name in single:
                assert set(plural[name]) == set(single[name])
                for filename in single[name]:
                    single_code = (ref_out_dir / name / filename).read_text()
                    plural_code = (plural_out_dir / name / filename).read_text()
                    assert (
                        plural_code == single_code
                    ), f"Content mismatch for {name}/{filename}"

    def test_max_workers_1_matches_max_workers_8(self):
        """``max_workers=1`` (sequential) and ``max_workers=8`` produce
        byte-identical content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["p1", "p2", "p3", "p4"])

            orch1 = ActionOrchestrator(project_root, max_workers=1)
            out1 = orch1.generate_pipelines_by_fields(
                pipeline_fields=["p1", "p2", "p3", "p4"],
                env="dev",
                output_dir=project_root / "w1",
                max_workers=1,
            )

            orch8 = ActionOrchestrator(project_root, max_workers=8)
            out8 = orch8.generate_pipelines_by_fields(
                pipeline_fields=["p1", "p2", "p3", "p4"],
                env="dev",
                output_dir=project_root / "w8",
                max_workers=8,
            )

            assert out1.keys() == out8.keys()
            for name in out1:
                assert out1[name] == out8[name], f"Divergence for {name}"

    def test_determinism_across_runs(self):
        """10 runs of the same workload produce identical content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["d1", "d2", "d3", "d4", "d5"])

            baseline: dict = None
            for run_idx in range(10):
                orch = ActionOrchestrator(project_root, max_workers=4)
                run_out = orch.generate_pipelines_by_fields(
                    pipeline_fields=["d1", "d2", "d3", "d4", "d5"],
                    env="dev",
                    output_dir=project_root / f"r{run_idx}",
                    max_workers=4,
                )
                if baseline is None:
                    baseline = run_out
                else:
                    assert run_out == baseline, f"Run {run_idx} diverged from baseline"

    def test_empty_pipeline_returns_empty_dict(self):
        """A pipeline name with no flowgroups returns ``{}`` and does not crash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["e_real"])
            # Add an empty pipeline directory with no flowgroup yamls
            (project_root / "pipelines" / "e_empty").mkdir()

            orch = ActionOrchestrator(project_root, max_workers=2)
            out = orch.generate_pipelines_by_fields(
                pipeline_fields=["e_real", "e_empty"],
                env="dev",
                output_dir=project_root / "out",
            )

            # Both pipelines appear in successful outcomes; empty one has
            # an empty mapping (mirrors the single-pipeline shim's early
            # return of ``{}``).
            assert "e_real" in out
            assert "e_empty" in out
            assert out["e_empty"] == ()
            assert "e_real_fg.py" in out["e_real"]

    def test_on_pipeline_complete_callback_fires_per_pipeline(self):
        """Callback fires once per pipeline, on the main thread, with the
        :class:`PipelineDelta` instance produced by the worker."""
        from lhp.models.processing import PipelineDelta

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["c1", "c2", "c3"])

            seen: list = []

            def cb(delta: PipelineDelta) -> None:
                seen.append(delta.pipeline_name)

            orch = ActionOrchestrator(project_root, max_workers=3)
            orch.generate_pipelines_by_fields(
                pipeline_fields=["c1", "c2", "c3"],
                env="dev",
                output_dir=project_root / "out",
                on_pipeline_complete=cb,
            )

            # Each pipeline fires exactly once; order may vary by completion
            # time but the set must match.
            assert sorted(seen) == ["c1", "c2", "c3"]


class TestValidatePipelinesByFields:
    """Tests for the flat-pool plural method ``validate_pipelines_by_fields``.

    Parallelizes validation across pipelines using the same flat pool as
    generate but with no Phase B state save / file writes — just a
    cross-flowgroup CDC fan-in compatibility check per pipeline.
    """

    def _build_project(self, tmpdir, pipeline_names):
        """Build a multi-pipeline project (same shape as TestGenerate's helper)."""
        project_root = Path(tmpdir)
        (project_root / "presets").mkdir()
        (project_root / "templates").mkdir()
        (project_root / "substitutions").mkdir()
        for name in pipeline_names:
            (project_root / "pipelines" / name).mkdir(parents=True)

        substitutions = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze",
                "landing_path": "/mnt/dev/landing",
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(substitutions, f)

        for name in pipeline_names:
            flowgroup = {
                "pipeline": name,
                "flowgroup": f"{name}_fg",
                "actions": [
                    {
                        "name": f"load_{name}",
                        "type": "load",
                        "target": f"v_{name}_raw",
                        "source": {
                            "type": "cloudfiles",
                            "path": "${landing_path}/" + name,
                            "format": "json",
                        },
                    },
                    {
                        "name": f"clean_{name}",
                        "type": "transform",
                        "transform_type": "sql",
                        "source": f"v_{name}_raw",
                        "target": f"v_{name}_clean",
                        "sql": f"SELECT * FROM v_{name}_raw",
                    },
                    {
                        "name": f"write_{name}",
                        "type": "write",
                        "source": f"v_{name}_clean",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": name,
                            "create_table": True,
                        },
                    },
                ],
            }
            with open(project_root / "pipelines" / name / f"{name}_fg.yaml", "w") as f:
                yaml.dump(flowgroup, f)

        return project_root

    def test_multi_pipeline_happy_path(self):
        """All pipelines validate cleanly: each outcome has success=True
        and zero errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["v1", "v2", "v3"])
            orch = ActionOrchestrator(project_root, max_workers=4)

            outcomes = orch.validate_pipelines_by_fields(
                pipeline_fields=["v1", "v2", "v3"],
                env="dev",
                include_tests=True,
            )

            assert len(outcomes) == 3
            for outcome in outcomes:
                assert outcome.success, (
                    f"Pipeline {outcome.pipeline} failed unexpectedly: "
                    f"{outcome.errors}"
                )
                assert outcome.errors == ()
                assert outcome.warnings == ()

    def test_outcomes_returned_in_input_order(self):
        """Outcomes are returned in the order of pipeline_fields input, not
        completion order — important for stable display in the CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["z_last", "a_first", "m_mid"])
            orch = ActionOrchestrator(project_root, max_workers=4)

            outcomes = orch.validate_pipelines_by_fields(
                pipeline_fields=["z_last", "a_first", "m_mid"],
                env="dev",
                include_tests=True,
            )

            assert [o.pipeline for o in outcomes] == [
                "z_last",
                "a_first",
                "m_mid",
            ]

    def test_empty_pipeline_reports_error(self):
        """A pipeline with no flowgroups reports the legacy 'No flowgroups
        found' error rather than silently succeeding (matches the shim's
        legacy behavior)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["real_one"])
            orch = ActionOrchestrator(project_root)

            outcomes = orch.validate_pipelines_by_fields(
                pipeline_fields=["real_one", "missing_pipeline"],
                env="dev",
                include_tests=True,
            )

            by_name = {o.pipeline: o for o in outcomes}
            assert by_name["real_one"].success is True
            assert by_name["missing_pipeline"].success is False
            assert any(
                "No flowgroups found" in e for e in by_name["missing_pipeline"].errors
            )

    def test_shim_returns_first_outcome_errors_and_warnings(self):
        """The single-pipeline shim ``validate_pipeline_by_field`` returns
        the legacy ``(errors, warnings)`` tuple unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["shim_pipe"])
            orch = ActionOrchestrator(project_root)

            errors, warnings = orch.validate_pipeline_by_field(
                "shim_pipe", "dev", include_tests=True
            )

            assert isinstance(errors, list)
            assert isinstance(warnings, list)
            # Happy-path project — no errors expected
            assert errors == []

    def test_max_workers_1_matches_max_workers_8(self):
        """Sequential and parallel validation produce the same outcome set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = self._build_project(tmpdir, ["w1", "w2", "w3", "w4"])

            orch = ActionOrchestrator(project_root)
            seq = orch.validate_pipelines_by_fields(
                pipeline_fields=["w1", "w2", "w3", "w4"],
                env="dev",
                include_tests=True,
                max_workers=1,
            )

            par = orch.validate_pipelines_by_fields(
                pipeline_fields=["w1", "w2", "w3", "w4"],
                env="dev",
                include_tests=True,
                max_workers=8,
            )

            # Compare as tuples of (pipeline, success, errors)
            def _to_compare(outcomes):
                return [(o.pipeline, o.success, o.errors) for o in outcomes]

            assert _to_compare(seq) == _to_compare(par)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
