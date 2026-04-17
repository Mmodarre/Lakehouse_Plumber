"""Tests for dependency analyzer service."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

import networkx as nx
import pytest

from lhp.core.services.dependency_analyzer import DependencyAnalyzer
from lhp.models.config import Action, ActionType, FlowGroup
from lhp.models.dependencies import DependencyAnalysisResult, DependencyGraphs
from lhp.utils.error_formatter import ErrorCategory, LHPError


class TestDependencyAnalyzer:
    """Test DependencyAnalyzer functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.mock_config_loader = Mock()
        self.analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_flowgroup(self, flowgroup_name: str, pipeline: str, actions: list):
        """Create a mock FlowGroup for testing."""
        mock_actions = []
        for action_data in actions:
            action = Mock(spec=Action)
            action.name = action_data.get("name", "test_action")
            action.type = action_data.get("type", ActionType.LOAD)
            action.target = action_data.get("target", None)
            action.source = action_data.get("source", None)
            action.sql = action_data.get("sql", None)
            action.sql_path = action_data.get("sql_path", None)
            action.module_path = action_data.get("module_path", None)
            action.write_target = action_data.get("write_target", None)
            mock_actions.append(action)

        flowgroup = Mock(spec=FlowGroup)
        flowgroup.flowgroup = flowgroup_name
        flowgroup.pipeline = pipeline
        flowgroup.actions = mock_actions
        return flowgroup

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_build_dependency_graphs_empty(self, mockget_flowgroups):
        """Test building dependency graphs with no flowgroups."""
        mockget_flowgroups.return_value = []

        result = self.analyzer.build_dependency_graphs()

        assert isinstance(result, DependencyGraphs)
        assert len(result.action_graph.nodes) == 0
        assert len(result.flowgroup_graph.nodes) == 0
        assert len(result.pipeline_graph.nodes) == 0

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_build_action_graph_basic(self, mockget_flowgroups):
        """Test basic action graph building."""
        # Create test flowgroups
        actions1 = [
            {
                "name": "load_customers",
                "type": ActionType.LOAD,
                "target": "bronze.customers",
                "source": "raw.customers",
            },
            {
                "name": "transform_customers",
                "type": ActionType.TRANSFORM,
                "source": "bronze.customers",
                "target": "silver.customers",
            },
        ]
        actions2 = [
            {
                "name": "load_orders",
                "type": ActionType.LOAD,
                "target": "bronze.orders",
                "source": "raw.orders",
            }
        ]

        flowgroup1 = self.create_mock_flowgroup(
            "customer_processing", "customer_pipeline", actions1
        )
        flowgroup2 = self.create_mock_flowgroup(
            "order_processing", "order_pipeline", actions2
        )
        mockget_flowgroups.return_value = [flowgroup1, flowgroup2]

        graphs = self.analyzer.build_dependency_graphs()

        # Check action graph
        action_graph = graphs.action_graph
        expected_actions = [
            "customer_processing.load_customers",
            "customer_processing.transform_customers",
            "order_processing.load_orders",
        ]

        assert len(action_graph.nodes) == 3
        for action in expected_actions:
            assert action in action_graph.nodes

        # Check dependency between actions within the same flowgroup
        assert action_graph.has_edge(
            "customer_processing.load_customers",
            "customer_processing.transform_customers",
        )
        # Edge type should be "internal" (same flowgroup)
        edge_data = action_graph.edges[
            "customer_processing.load_customers",
            "customer_processing.transform_customers",
        ]
        assert edge_data["type"] == "internal"

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_build_flowgroup_graph(self, mockget_flowgroups):
        """Test flowgroup graph building."""
        # Create flowgroups with cross-flowgroup dependencies
        actions1 = [
            {
                "name": "load_customers",
                "type": ActionType.LOAD,
                "target": "bronze.customers",
            }
        ]
        actions2 = [
            {
                "name": "process_orders",
                "type": ActionType.TRANSFORM,
                "source": "bronze.customers",
            }
        ]

        flowgroup1 = self.create_mock_flowgroup("customers", "pipeline1", actions1)
        flowgroup2 = self.create_mock_flowgroup("orders", "pipeline2", actions2)
        mockget_flowgroups.return_value = [flowgroup1, flowgroup2]

        graphs = self.analyzer.build_dependency_graphs()

        # Check flowgroup graph
        flowgroup_graph = graphs.flowgroup_graph
        assert len(flowgroup_graph.nodes) == 2
        assert "customers" in flowgroup_graph.nodes
        assert "orders" in flowgroup_graph.nodes

        # Check dependency between flowgroups
        assert flowgroup_graph.has_edge("customers", "orders")
        # Edge type should be "cross_pipeline" (pipeline1 → pipeline2)
        edge_data = flowgroup_graph.edges["customers", "orders"]
        assert edge_data["type"] == "cross_pipeline"

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_build_pipeline_graph(self, mockget_flowgroups):
        """Test pipeline graph building."""
        # Create flowgroups in different pipelines with dependencies
        actions1 = [
            {"name": "load_raw", "type": ActionType.LOAD, "target": "bronze.data"}
        ]
        actions2 = [
            {
                "name": "transform",
                "type": ActionType.TRANSFORM,
                "source": "bronze.data",
                "target": "silver.data",
            }
        ]
        actions3 = [
            {
                "name": "aggregate",
                "type": ActionType.TRANSFORM,
                "source": "silver.data",
                "target": "gold.summary",
            }
        ]

        flowgroup1 = self.create_mock_flowgroup(
            "bronze_flow", "bronze_pipeline", actions1
        )
        flowgroup2 = self.create_mock_flowgroup(
            "silver_flow", "silver_pipeline", actions2
        )
        flowgroup3 = self.create_mock_flowgroup("gold_flow", "gold_pipeline", actions3)
        mockget_flowgroups.return_value = [flowgroup1, flowgroup2, flowgroup3]

        graphs = self.analyzer.build_dependency_graphs()

        # Check pipeline graph
        pipeline_graph = graphs.pipeline_graph
        assert len(pipeline_graph.nodes) == 3
        assert "bronze_pipeline" in pipeline_graph.nodes
        assert "silver_pipeline" in pipeline_graph.nodes
        assert "gold_pipeline" in pipeline_graph.nodes

        # Check pipeline dependencies
        assert pipeline_graph.has_edge("bronze_pipeline", "silver_pipeline")
        assert pipeline_graph.has_edge("silver_pipeline", "gold_pipeline")
        # All pipeline edges are cross_pipeline by definition
        assert pipeline_graph.edges["bronze_pipeline", "silver_pipeline"]["type"] == "cross_pipeline"
        assert pipeline_graph.edges["silver_pipeline", "gold_pipeline"]["type"] == "cross_pipeline"

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_action_edge_type_cross_flowgroup(self, mockget_flowgroups):
        """Action edge between different flowgroups in the same pipeline → cross_flowgroup."""
        actions1 = [
            {"name": "load", "type": ActionType.LOAD, "target": "bronze.data"},
        ]
        actions2 = [
            {"name": "transform", "type": ActionType.TRANSFORM, "source": "bronze.data"},
        ]
        fg1 = self.create_mock_flowgroup("fg_producer", "shared_pipeline", actions1)
        fg2 = self.create_mock_flowgroup("fg_consumer", "shared_pipeline", actions2)
        mockget_flowgroups.return_value = [fg1, fg2]

        graphs = self.analyzer.build_dependency_graphs()

        assert graphs.action_graph.has_edge("fg_producer.load", "fg_consumer.transform")
        edge_data = graphs.action_graph.edges["fg_producer.load", "fg_consumer.transform"]
        assert edge_data["type"] == "cross_flowgroup"

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_action_edge_type_cross_pipeline(self, mockget_flowgroups):
        """Action edge between different pipelines → cross_pipeline."""
        actions1 = [
            {"name": "write", "type": ActionType.LOAD, "target": "bronze.data"},
        ]
        actions2 = [
            {"name": "load_cdc", "type": ActionType.TRANSFORM, "source": "bronze.data"},
        ]
        fg1 = self.create_mock_flowgroup("fg_bronze", "bronze_pipeline", actions1)
        fg2 = self.create_mock_flowgroup("fg_silver", "silver_pipeline", actions2)
        mockget_flowgroups.return_value = [fg1, fg2]

        graphs = self.analyzer.build_dependency_graphs()

        assert graphs.action_graph.has_edge("fg_bronze.write", "fg_silver.load_cdc")
        edge_data = graphs.action_graph.edges["fg_bronze.write", "fg_silver.load_cdc"]
        assert edge_data["type"] == "cross_pipeline"

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_flowgroup_edge_type_cross_flowgroup(self, mockget_flowgroups):
        """Flowgroup edge within the same pipeline → cross_flowgroup."""
        actions1 = [
            {"name": "load", "type": ActionType.LOAD, "target": "staging.raw"},
        ]
        actions2 = [
            {"name": "clean", "type": ActionType.TRANSFORM, "source": "staging.raw"},
        ]
        fg1 = self.create_mock_flowgroup("ingest", "shared_pipeline", actions1)
        fg2 = self.create_mock_flowgroup("cleanse", "shared_pipeline", actions2)
        mockget_flowgroups.return_value = [fg1, fg2]

        graphs = self.analyzer.build_dependency_graphs()

        assert graphs.flowgroup_graph.has_edge("ingest", "cleanse")
        edge_data = graphs.flowgroup_graph.edges["ingest", "cleanse"]
        assert edge_data["type"] == "cross_flowgroup"

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_analyze_dependencies_complete(self, mockget_flowgroups):
        """Test complete dependency analysis."""
        # Create a simple dependency chain
        actions1 = [{"name": "load", "type": ActionType.LOAD, "target": "bronze.data"}]
        actions2 = [
            {"name": "transform", "type": ActionType.TRANSFORM, "source": "bronze.data"}
        ]

        flowgroup1 = self.create_mock_flowgroup("loader", "pipeline1", actions1)
        flowgroup2 = self.create_mock_flowgroup("transformer", "pipeline2", actions2)
        mockget_flowgroups.return_value = [flowgroup1, flowgroup2]

        result = self.analyzer.analyze_dependencies()

        assert isinstance(result, DependencyAnalysisResult)
        assert len(result.pipeline_dependencies) == 2
        assert len(result.execution_stages) == 2  # Two stages in the execution order
        assert len(result.circular_dependencies) == 0

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_detect_circular_dependencies(self, mockget_flowgroups):
        """Test circular dependency detection."""
        # Create circular dependency: A -> B -> C -> A
        actions_a = [
            {
                "name": "action_a",
                "type": ActionType.TRANSFORM,
                "source": "table_c",
                "target": "table_a",
            }
        ]
        actions_b = [
            {
                "name": "action_b",
                "type": ActionType.TRANSFORM,
                "source": "table_a",
                "target": "table_b",
            }
        ]
        actions_c = [
            {
                "name": "action_c",
                "type": ActionType.TRANSFORM,
                "source": "table_b",
                "target": "table_c",
            }
        ]

        flowgroup_a = self.create_mock_flowgroup("fg_a", "pipeline_a", actions_a)
        flowgroup_b = self.create_mock_flowgroup("fg_b", "pipeline_b", actions_b)
        flowgroup_c = self.create_mock_flowgroup("fg_c", "pipeline_c", actions_c)
        mockget_flowgroups.return_value = [flowgroup_a, flowgroup_b, flowgroup_c]

        result = self.analyzer.analyze_dependencies()

        assert len(result.circular_dependencies) > 0
        assert (
            len(result.execution_stages) == 0
        )  # No execution order possible due to cycles

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_execution_order_parallel_stages(self, mockget_flowgroups):
        """Test execution order with parallel stages."""
        # Create graph: A -> B, A -> C, B -> D, C -> D
        actions_a = [
            {"name": "load_base", "type": ActionType.LOAD, "target": "base.data"}
        ]
        actions_b = [
            {
                "name": "process_b",
                "type": ActionType.TRANSFORM,
                "source": "base.data",
                "target": "branch_b.data",
            }
        ]
        actions_c = [
            {
                "name": "process_c",
                "type": ActionType.TRANSFORM,
                "source": "base.data",
                "target": "branch_c.data",
            }
        ]
        actions_d = [
            {
                "name": "merge",
                "type": ActionType.TRANSFORM,
                "source": ["branch_b.data", "branch_c.data"],
                "target": "final.data",
            }
        ]

        flowgroup_a = self.create_mock_flowgroup("base", "pipeline_a", actions_a)
        flowgroup_b = self.create_mock_flowgroup("branch_b", "pipeline_b", actions_b)
        flowgroup_c = self.create_mock_flowgroup("branch_c", "pipeline_c", actions_c)
        flowgroup_d = self.create_mock_flowgroup("final", "pipeline_d", actions_d)
        mockget_flowgroups.return_value = [
            flowgroup_a,
            flowgroup_b,
            flowgroup_c,
            flowgroup_d,
        ]

        result = self.analyzer.analyze_dependencies()

        # Should have 3 stages: A, [B,C], D
        assert len(result.execution_stages) == 3
        assert result.execution_stages[0] == ["pipeline_a"]  # A runs first
        assert sorted(result.execution_stages[1]) == [
            "pipeline_b",
            "pipeline_c",
        ]  # B and C run in parallel
        assert result.execution_stages[2] == ["pipeline_d"]  # D runs last

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    @patch("lhp.utils.sql_parser.extract_tables_from_sql")
    def test_sql_source_extraction(self, mock_extract_sql, mockget_flowgroups):
        """Test SQL source extraction from actions."""
        mock_extract_sql.return_value = ["bronze.customers", "bronze.orders"]

        # Action with inline SQL
        actions = [
            {
                "name": "sql_action",
                "type": ActionType.TRANSFORM,
                "sql": "SELECT * FROM bronze.customers JOIN bronze.orders",
            }
        ]
        flowgroup = self.create_mock_flowgroup("test_fg", "test_pipeline", actions)
        mockget_flowgroups.return_value = [flowgroup]

        graphs = self.analyzer.build_dependency_graphs()

        # Verify SQL parser was called
        mock_extract_sql.assert_called()

        # Check that external sources were identified
        action_id = "test_fg.sql_action"
        assert action_id in graphs.action_graph.nodes
        external_sources = graphs.action_graph.nodes[action_id].get(
            "external_sources", []
        )
        assert "bronze.customers" in external_sources
        assert "bronze.orders" in external_sources

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    @patch("lhp.utils.python_parser.extract_tables_from_python")
    def test_python_source_extraction(self, mock_extract_python, mockget_flowgroups):
        """Test Python source extraction from actions."""
        mock_extract_python.return_value = ["silver.processed_data"]

        # Action with Python module
        actions = [
            {
                "name": "python_action",
                "type": ActionType.TRANSFORM,
                "module_path": "transforms/process_data.py",
            }
        ]
        flowgroup = self.create_mock_flowgroup("test_fg", "test_pipeline", actions)
        mockget_flowgroups.return_value = [flowgroup]

        # Mock file existence
        with (
            patch("pathlib.Path.exists", return_value=True),
            patch(
                "pathlib.Path.read_text",
                return_value='spark.sql("SELECT * FROM silver.processed_data")',
            ),
        ):
            graphs = self.analyzer.build_dependency_graphs()

        # Verify Python parser was called
        mock_extract_python.assert_called()

        # Check that external sources were identified
        action_id = "test_fg.python_action"
        assert action_id in graphs.action_graph.nodes
        external_sources = graphs.action_graph.nodes[action_id].get(
            "external_sources", []
        )
        assert "silver.processed_data" in external_sources

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_sql_file_path_resolution(self, mockget_flowgroups):
        """Test SQL file path resolution with flowgroup file paths."""
        actions = [
            {
                "name": "sql_file_action",
                "type": ActionType.TRANSFORM,
                "sql_path": "queries/transform.sql",
            }
        ]
        flowgroup = self.create_mock_flowgroup("test_fg", "test_pipeline", actions)
        mockget_flowgroups.return_value = [flowgroup]

        # Set up flowgroup file path mapping
        yaml_path = self.temp_dir / "pipelines" / "test.yaml"
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        self.analyzer._flowgroup_file_paths["test_fg"] = yaml_path

        # Create SQL file
        sql_file = yaml_path.parent / "queries" / "transform.sql"
        sql_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text("SELECT * FROM bronze.test_table")

        with patch(
            "lhp.utils.sql_parser.extract_tables_from_sql",
            return_value=["bronze.test_table"],
        ):
            graphs = self.analyzer.build_dependency_graphs()

        # Verify the SQL file was processed
        action_id = "test_fg.sql_file_action"
        assert action_id in graphs.action_graph.nodes
        external_sources = graphs.action_graph.nodes[action_id].get(
            "external_sources", []
        )
        assert "bronze.test_table" in external_sources

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_sql_file_not_found_error(self, mockget_flowgroups):
        """Test error handling when SQL file is not found."""
        actions = [
            {
                "name": "sql_file_action",
                "type": ActionType.TRANSFORM,
                "sql_path": "nonexistent.sql",
            }
        ]
        flowgroup = self.create_mock_flowgroup("test_fg", "test_pipeline", actions)
        mockget_flowgroups.return_value = [flowgroup]

        # Set up flowgroup file path mapping
        yaml_path = self.temp_dir / "test.yaml"
        self.analyzer._flowgroup_file_paths["test_fg"] = yaml_path

        # Should raise LHPError when file doesn't exist
        with pytest.raises(LHPError) as exc_info:
            self.analyzer.build_dependency_graphs()

        assert "LHP-IO-" in exc_info.value.code
        assert "SQL file not found" in exc_info.value.title

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_python_file_not_found_error(self, mockget_flowgroups):
        """Test error handling when Python file is not found."""
        actions = [
            {
                "name": "python_action",
                "type": ActionType.TRANSFORM,
                "module_path": "nonexistent.py",
            }
        ]
        flowgroup = self.create_mock_flowgroup("test_fg", "test_pipeline", actions)
        mockget_flowgroups.return_value = [flowgroup]

        # Should raise LHPError when file doesn't exist
        with pytest.raises(LHPError) as exc_info:
            self.analyzer.build_dependency_graphs()

        assert "LHP-IO-" in exc_info.value.code
        assert "Python file not found" in exc_info.value.title

    def test_export_to_dot_format(self):
        """Test DOT format export."""
        # Create a simple graph
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        # Add some pipeline nodes and edges
        graphs.pipeline_graph.add_node("pipeline_a", flowgroup_count=2)
        graphs.pipeline_graph.add_node("pipeline_b", flowgroup_count=1)
        graphs.pipeline_graph.add_edge("pipeline_a", "pipeline_b")

        dot_output = self.analyzer.export_to_dot(graphs, "pipeline")

        assert "digraph pipeline_dependencies" in dot_output
        assert "pipeline_a" in dot_output
        assert "pipeline_b" in dot_output
        assert 'pipeline_a" -> "pipeline_b"' in dot_output

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_export_to_json_format(self, mockget_flowgroups):
        """Test JSON format export."""
        # Create simple test data
        actions = [
            {"name": "test_action", "type": ActionType.LOAD, "target": "test.table"}
        ]
        flowgroup = self.create_mock_flowgroup("test_fg", "test_pipeline", actions)
        mockget_flowgroups.return_value = [flowgroup]

        result = self.analyzer.analyze_dependencies()
        json_output = self.analyzer.export_to_json(result)

        assert "metadata" in json_output
        assert "pipelines" in json_output
        assert "execution_stages" in json_output
        assert "external_sources" in json_output
        assert "circular_dependencies" in json_output
        assert json_output["metadata"]["total_pipelines"] == 1

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_pipeline_filtering(self, mockget_flowgroups):
        """Test pipeline filtering functionality."""
        # Create flowgroups in different pipelines
        actions1 = [{"name": "action1", "type": ActionType.LOAD, "target": "table1"}]

        flowgroup1 = self.create_mock_flowgroup("fg1", "pipeline_a", actions1)

        # Mock get_flowgroups to return only the filtered flowgroup when filtering
        def mockget_flowgroups_filter(pipeline_filter=None):
            if pipeline_filter == "pipeline_a":
                return [flowgroup1]
            return []

        mockget_flowgroups.side_effect = mockget_flowgroups_filter

        # Analyze only pipeline_a
        result = self.analyzer.analyze_dependencies(pipeline_filter="pipeline_a")

        # Should only have one pipeline
        assert len(result.pipeline_dependencies) == 1
        assert "pipeline_a" in result.pipeline_dependencies

    def test_external_source_collection(self):
        """Test external source collection across all graphs."""
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        # Add action nodes with external sources
        graphs.action_graph.add_node(
            "action1", external_sources=["external.table1", "external.table2"]
        )
        graphs.action_graph.add_node("action2", external_sources=["external.table3"])

        external_sources = self.analyzer._collect_external_sources(graphs)

        assert sorted(external_sources) == [
            "external.table1",
            "external.table2",
            "external.table3",
        ]

    def test_write_target_handling(self):
        """Test write target handling for dependency tracking."""
        # Mock a write action with write_target
        write_action = Mock(spec=Action)
        write_action.name = "write_action"
        write_action.type = ActionType.WRITE
        write_action.source = None
        write_action.target = None
        write_action.write_target = {"catalog": "test_cat", "schema": "bronze", "table": "output_table"}
        write_action.sql = None
        write_action.sql_path = None
        write_action.module_path = None

        # Mock a load action that depends on the write output
        load_action = Mock(spec=Action)
        load_action.name = "load_action"
        load_action.type = ActionType.LOAD
        load_action.source = "test_cat.bronze.output_table"
        load_action.target = "silver.processed"
        load_action.sql = None
        load_action.sql_path = None
        load_action.module_path = None
        load_action.write_target = None

        flowgroup1 = self.create_mock_flowgroup("writer", "pipeline1", [])
        flowgroup1.actions = [write_action]

        flowgroup2 = self.create_mock_flowgroup("reader", "pipeline2", [])
        flowgroup2.actions = [load_action]

        with patch.object(
            self.analyzer, "get_flowgroups", return_value=[flowgroup1, flowgroup2]
        ):
            graphs = self.analyzer.build_dependency_graphs()

        # Check that dependency was established
        assert graphs.action_graph.has_edge("writer.write_action", "reader.load_action")

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_write_target_sql_path_dependency_extraction(self, mockget_flowgroups):
        """Test that sql_path inside write_target is used for dependency extraction."""
        actions = [
            {
                "name": "write_claim_summary_mv",
                "type": ActionType.WRITE,
                "write_target": {
                    "type": "materialized_view",
                    "sql_path": "sql/gold/claim_summary.sql",
                    "database": "catalog.gold",
                    "table": "claim_summary",
                },
            }
        ]
        flowgroup = self.create_mock_flowgroup(
            "gold_claim_summary", "gold_pipeline", actions
        )
        mockget_flowgroups.return_value = [flowgroup]

        # Set up flowgroup file path mapping
        yaml_path = self.temp_dir / "pipelines" / "gold" / "test.yaml"
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        self.analyzer._flowgroup_file_paths["gold_claim_summary"] = yaml_path

        # Create SQL file relative to YAML
        sql_file = yaml_path.parent / "sql" / "gold" / "claim_summary.sql"
        sql_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text(
            "SELECT * FROM catalog.silver.claim "
            "JOIN catalog.silver.claim_line ON 1=1"
        )

        with patch(
            "lhp.utils.sql_parser.extract_tables_from_sql",
            return_value=["catalog.silver.claim", "catalog.silver.claim_line"],
        ):
            graphs = self.analyzer.build_dependency_graphs()

        # Verify that the SQL sources were extracted from write_target.sql_path
        action_id = "gold_claim_summary.write_claim_summary_mv"
        assert action_id in graphs.action_graph.nodes
        external_sources = graphs.action_graph.nodes[action_id].get(
            "external_sources", []
        )
        assert "catalog.silver.claim" in external_sources
        assert "catalog.silver.claim_line" in external_sources

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_write_target_inline_sql_dependency_extraction(self, mockget_flowgroups):
        """Test that inline sql inside write_target is used for dependency extraction."""
        actions = [
            {
                "name": "write_summary_mv",
                "type": ActionType.WRITE,
                "write_target": {
                    "type": "materialized_view",
                    "sql": "SELECT * FROM catalog.silver.orders JOIN catalog.silver.customers ON 1=1",
                    "database": "catalog.gold",
                    "table": "order_summary",
                },
            }
        ]
        flowgroup = self.create_mock_flowgroup(
            "gold_order_summary", "gold_pipeline", actions
        )
        mockget_flowgroups.return_value = [flowgroup]

        with patch(
            "lhp.utils.sql_parser.extract_tables_from_sql",
            return_value=["catalog.silver.orders", "catalog.silver.customers"],
        ):
            graphs = self.analyzer.build_dependency_graphs()

        # Verify that the SQL sources were extracted from write_target.sql
        action_id = "gold_order_summary.write_summary_mv"
        assert action_id in graphs.action_graph.nodes
        external_sources = graphs.action_graph.nodes[action_id].get(
            "external_sources", []
        )
        assert "catalog.silver.orders" in external_sources
        assert "catalog.silver.customers" in external_sources

    @patch("lhp.core.services.dependency_analyzer.DependencyAnalyzer.get_flowgroups")
    def test_empty_graphs_metadata(self, mockget_flowgroups):
        """Test metadata generation for empty graphs."""
        mockget_flowgroups.return_value = []

        graphs = self.analyzer.build_dependency_graphs()

        # Empty graphs have empty metadata
        assert graphs.metadata == {}
        assert len(graphs.action_graph.nodes) == 0
        assert len(graphs.flowgroup_graph.nodes) == 0
        assert len(graphs.pipeline_graph.nodes) == 0

    def test_invalid_graph_level_error(self):
        """Test error handling for invalid graph level in DOT export."""
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        with pytest.raises(ValueError) as exc_info:
            self.analyzer.export_to_dot(graphs, "invalid_level")

        error_msg = str(exc_info.value)
        assert "invalid_level" in error_msg
        assert "Unknown dependency graph level" in error_msg

    def test_not_implemented_methods(self):
        """Test placeholder methods that are not yet implemented."""
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        with pytest.raises(NotImplementedError):
            self.analyzer.get_critical_path(graphs)

        with pytest.raises(NotImplementedError):
            self.analyzer.get_parallelization_opportunities(graphs)

        with pytest.raises(NotImplementedError):
            self.analyzer.get_centrality_metrics(graphs)


class TestCycleDetection:
    """Tests for the _detect_circular_dependencies method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.mock_config_loader = Mock()
        self.analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _make_graphs(
        self,
        action_edges=None,
        flowgroup_edges=None,
        pipeline_edges=None,
    ):
        """Helper to build DependencyGraphs from edge lists."""
        action_graph = nx.DiGraph()
        flowgroup_graph = nx.DiGraph()
        pipeline_graph = nx.DiGraph()

        for src, dst in action_edges or []:
            action_graph.add_edge(src, dst)
        for src, dst in flowgroup_edges or []:
            flowgroup_graph.add_edge(src, dst)
        for src, dst in pipeline_edges or []:
            pipeline_graph.add_edge(src, dst)

        return DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=flowgroup_graph,
            pipeline_graph=pipeline_graph,
            metadata={},
        )

    # ------------------------------------------------------------------ #
    # 1. Multiple independent cycles -> all reported
    # ------------------------------------------------------------------ #
    def test_multiple_independent_cycles_all_reported(self):
        """Two separate cycles (A->B->A and C->D->C) should both be detected."""
        graphs = self._make_graphs(
            action_edges=[("a", "b"), ("b", "a"), ("c", "d"), ("d", "c")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        # Flatten all cycle descriptions into one string for easy checking
        all_descriptions = " | ".join(desc for cycle in result for desc in cycle)

        # Both cycles must appear
        assert len(result) >= 2, f"Expected at least 2 cycles, got {len(result)}"
        # Verify both pairs are represented somewhere in the output
        has_ab = any("a" in d and "b" in d for cycle in result for d in cycle)
        has_cd = any("c" in d and "d" in d for cycle in result for d in cycle)
        assert has_ab, f"Cycle A<->B not found in: {all_descriptions}"
        assert has_cd, f"Cycle C<->D not found in: {all_descriptions}"

    def test_multiple_independent_cycles_across_levels(self):
        """Independent cycles at action and pipeline levels should both be detected."""
        graphs = self._make_graphs(
            action_edges=[("x", "y"), ("y", "x")],
            pipeline_edges=[("p1", "p2"), ("p2", "p1")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert len(result) >= 2
        levels_found = set()
        for cycle in result:
            for desc in cycle:
                if desc.startswith("action level:"):
                    levels_found.add("action")
                elif desc.startswith("pipeline level:"):
                    levels_found.add("pipeline")

        assert "action" in levels_found, "Expected an action-level cycle"
        assert "pipeline" in levels_found, "Expected a pipeline-level cycle"

    # ------------------------------------------------------------------ #
    # 2. Cycles at each individual level
    # ------------------------------------------------------------------ #
    def test_action_level_cycle_only(self):
        """A cycle only in the action graph should be detected at action level."""
        graphs = self._make_graphs(
            action_edges=[("a1", "a2"), ("a2", "a3"), ("a3", "a1")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert len(result) >= 1
        descriptions = [desc for cycle in result for desc in cycle]
        assert all(
            d.startswith("action level:") for d in descriptions
        ), f"Expected only action-level cycles, got: {descriptions}"

    def test_flowgroup_level_cycle_only(self):
        """A cycle only in the flowgroup graph should be detected at flowgroup level."""
        graphs = self._make_graphs(
            flowgroup_edges=[("fg_a", "fg_b"), ("fg_b", "fg_a")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert len(result) >= 1
        descriptions = [desc for cycle in result for desc in cycle]
        assert all(
            d.startswith("flowgroup level:") for d in descriptions
        ), f"Expected only flowgroup-level cycles, got: {descriptions}"

    def test_pipeline_level_cycle_only(self):
        """A cycle only in the pipeline graph should be detected at pipeline level."""
        graphs = self._make_graphs(
            pipeline_edges=[("p_x", "p_y"), ("p_y", "p_z"), ("p_z", "p_x")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert len(result) >= 1
        descriptions = [desc for cycle in result for desc in cycle]
        assert all(
            d.startswith("pipeline level:") for d in descriptions
        ), f"Expected only pipeline-level cycles, got: {descriptions}"

    def test_cycles_at_all_three_levels(self):
        """Cycles at action, flowgroup, and pipeline levels should all be reported."""
        graphs = self._make_graphs(
            action_edges=[("a1", "a2"), ("a2", "a1")],
            flowgroup_edges=[("fg1", "fg2"), ("fg2", "fg1")],
            pipeline_edges=[("p1", "p2"), ("p2", "p1")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert len(result) >= 3
        levels_found = set()
        for cycle in result:
            for desc in cycle:
                if desc.startswith("action level:"):
                    levels_found.add("action")
                elif desc.startswith("flowgroup level:"):
                    levels_found.add("flowgroup")
                elif desc.startswith("pipeline level:"):
                    levels_found.add("pipeline")

        assert levels_found == {
            "action",
            "flowgroup",
            "pipeline",
        }, f"Expected all three levels, got: {levels_found}"

    # ------------------------------------------------------------------ #
    # 3. Cycle cap at 20
    # ------------------------------------------------------------------ #
    def test_cycle_cap_at_twenty(self):
        """When more than 20 cycles exist, only 20 should be reported."""
        # Create 25 independent 2-node cycles in the action graph.
        # Each pair (node_i_a, node_i_b) forms its own cycle.
        action_edges = []
        for i in range(25):
            node_a = f"node_{i}_a"
            node_b = f"node_{i}_b"
            action_edges.append((node_a, node_b))
            action_edges.append((node_b, node_a))

        graphs = self._make_graphs(action_edges=action_edges)

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert (
            len(result) == 20
        ), f"Expected exactly 20 cycles (the cap), got {len(result)}"

    def test_cycle_cap_across_levels(self):
        """The 20-cycle cap applies globally across all graph levels."""
        # Put 12 independent cycles in action graph, 12 in flowgroup graph.
        # Total > 20, so the cap should kick in.
        action_edges = []
        for i in range(12):
            a, b = f"act_{i}_a", f"act_{i}_b"
            action_edges.append((a, b))
            action_edges.append((b, a))

        flowgroup_edges = []
        for i in range(12):
            a, b = f"fg_{i}_a", f"fg_{i}_b"
            flowgroup_edges.append((a, b))
            flowgroup_edges.append((b, a))

        graphs = self._make_graphs(
            action_edges=action_edges,
            flowgroup_edges=flowgroup_edges,
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert (
            len(result) == 20
        ), f"Expected exactly 20 cycles (global cap), got {len(result)}"

    # ------------------------------------------------------------------ #
    # 4. No cycles -> empty list
    # ------------------------------------------------------------------ #
    def test_no_cycles_dag(self):
        """A DAG with no cycles should return an empty list."""
        graphs = self._make_graphs(
            action_edges=[("a", "b"), ("b", "c"), ("a", "c")],
            flowgroup_edges=[("fg_a", "fg_b"), ("fg_b", "fg_c")],
            pipeline_edges=[("p1", "p2"), ("p2", "p3")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert result == [], f"Expected no cycles for a DAG, got: {result}"

    def test_no_cycles_empty_graphs(self):
        """Completely empty graphs should return an empty list."""
        graphs = self._make_graphs()

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert result == []

    def test_no_cycles_single_nodes(self):
        """Graphs with isolated nodes (no edges) should return an empty list."""
        action_graph = nx.DiGraph()
        action_graph.add_node("lone_node_1")
        action_graph.add_node("lone_node_2")

        graphs = DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert result == []

    # ------------------------------------------------------------------ #
    # Additional edge-case: cycle description format
    # ------------------------------------------------------------------ #
    def test_cycle_description_format(self):
        """Verify the cycle description format includes level and arrow notation."""
        graphs = self._make_graphs(
            action_edges=[("alpha", "beta"), ("beta", "alpha")],
        )

        result = self.analyzer._detect_circular_dependencies(graphs)

        assert len(result) >= 1
        first_desc = result[0][0]
        assert first_desc.startswith("action level: ")
        assert " -> " in first_desc
        # The cycle should close back to the start node
        parts = first_desc.split(": ", 1)[1].split(" -> ")
        assert (
            parts[0] == parts[-1]
        ), f"Cycle should close back to start node: {first_desc}"
