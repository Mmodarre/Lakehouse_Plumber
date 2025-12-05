"""Tests for ParallelFlowgroupProcessor functionality."""

import os
import time
from pathlib import Path
import pytest
from dataclasses import dataclass

from lhp.core.parallel_processor import ParallelFlowgroupProcessor, FlowgroupResult


@dataclass
class MockFlowGroup:
    """Mock FlowGroup for testing."""
    flowgroup: str
    pipeline: str


class TestParallelFlowgroupProcessor:
    """Tests for ParallelFlowgroupProcessor class."""
    
    def test_initialization(self):
        """Test that processor initializes correctly."""
        processor = ParallelFlowgroupProcessor()
        assert processor.max_workers > 0
        assert processor.max_workers <= 8
    
    def test_custom_max_workers(self):
        """Test custom max_workers setting."""
        processor = ParallelFlowgroupProcessor(max_workers=4)
        assert processor.max_workers == 4
    
    def test_empty_flowgroups_list(self):
        """Test that empty list returns empty results."""
        processor = ParallelFlowgroupProcessor()
        
        def process_func(fg):
            return FlowgroupResult(
                flowgroup_name=fg.flowgroup,
                pipeline=fg.pipeline,
                code="",
                formatted_code="",
                source_yaml=None,
                success=True
            )
        
        results = processor.process_flowgroups_parallel([], process_func)
        assert len(results) == 0
    
    def test_sequential_fallback_for_small_batches(self):
        """Test that small batches use sequential processing."""
        processor = ParallelFlowgroupProcessor()
        
        flowgroups = [
            MockFlowGroup("fg1", "pipeline1"),
            MockFlowGroup("fg2", "pipeline1"),
            MockFlowGroup("fg3", "pipeline1")
        ]
        
        call_count = []
        
        def process_func(fg):
            call_count.append(fg.flowgroup)
            return FlowgroupResult(
                flowgroup_name=fg.flowgroup,
                pipeline=fg.pipeline,
                code=f"code for {fg.flowgroup}",
                formatted_code=f"formatted code for {fg.flowgroup}",
                source_yaml=None,
                success=True
            )
        
        results = processor.process_flowgroups_parallel(flowgroups, process_func)
        
        # Verify all processed
        assert len(results) == 3
        assert len(call_count) == 3
        
        # Verify results are correct
        for result in results:
            assert result.success
            assert result.code.startswith("code for")
    
    @pytest.mark.performance
    @pytest.mark.skipif(
        os.getenv('CI') is not None,
        reason="Performance test with timing assertions is unreliable in CI environments"
    )
    def test_parallel_processing_for_large_batches(self):
        """Test that large batches use parallel processing.
        
        Note: This is a performance test that measures actual timing and is
        skipped in CI due to resource constraints and variability in virtualized
        environments. Run locally with: pytest -m performance
        """
        processor = ParallelFlowgroupProcessor()
        
        flowgroups = [MockFlowGroup(f"fg{i}", "pipeline1") for i in range(10)]
        
        def process_func(fg):
            time.sleep(0.01)  # Simulate work
            return FlowgroupResult(
                flowgroup_name=fg.flowgroup,
                pipeline=fg.pipeline,
                code=f"code for {fg.flowgroup}",
                formatted_code=f"formatted code for {fg.flowgroup}",
                source_yaml=None,
                success=True
            )
        
        start_time = time.time()
        results = processor.process_flowgroups_parallel(flowgroups, process_func)
        elapsed_time = time.time() - start_time
        
        # Verify all processed
        assert len(results) == 10
        
        # Parallel processing should be faster than 10 * 0.01 = 0.1s
        # (with some tolerance for overhead)
        assert elapsed_time < 0.08  # Should be significantly faster
        
        # Verify results
        for result in results:
            assert result.success
    
    def test_error_handling_in_workers(self):
        """Test that errors in worker threads are captured."""
        processor = ParallelFlowgroupProcessor()
        
        flowgroups = [
            MockFlowGroup("fg1", "pipeline1"),
            MockFlowGroup("fg2_error", "pipeline1"),
            MockFlowGroup("fg3", "pipeline1"),
            MockFlowGroup("fg4_error", "pipeline1")
        ]
        
        def process_func(fg):
            if "error" in fg.flowgroup:
                raise ValueError(f"Test error for {fg.flowgroup}")
            return FlowgroupResult(
                flowgroup_name=fg.flowgroup,
                pipeline=fg.pipeline,
                code=f"code for {fg.flowgroup}",
                formatted_code=f"formatted code for {fg.flowgroup}",
                source_yaml=None,
                success=True
            )
        
        results = processor.process_flowgroups_parallel(flowgroups, process_func)
        
        # Verify all flowgroups have results
        assert len(results) == 4
        
        # Verify error results
        error_results = [r for r in results if not r.success]
        success_results = [r for r in results if r.success]
        
        assert len(error_results) == 2
        assert len(success_results) == 2
        
        # Verify error details
        for result in error_results:
            assert "Test error" in result.error
            assert result.code == ""
            assert result.formatted_code == ""
    
    def test_progress_callback(self):
        """Test that progress callback is called."""
        processor = ParallelFlowgroupProcessor()
        
        flowgroups = [MockFlowGroup(f"fg{i}", "pipeline1") for i in range(5)]
        
        progress_calls = []
        
        def progress_callback(completed, total):
            progress_calls.append((completed, total))
        
        def process_func(fg):
            return FlowgroupResult(
                flowgroup_name=fg.flowgroup,
                pipeline=fg.pipeline,
                code="",
                formatted_code="",
                source_yaml=None,
                success=True
            )
        
        processor.process_flowgroups_parallel(
            flowgroups, process_func, progress_callback=progress_callback
        )
        
        # Verify callback was called for each completion
        assert len(progress_calls) == 5
        
        # Verify final call shows completion
        final_call = progress_calls[-1]
        assert final_call == (5, 5)
    
    def test_correctness_vs_sequential(self):
        """Test that parallel results match sequential results."""
        flowgroups = [MockFlowGroup(f"fg{i}", "pipeline1") for i in range(10)]
        
        def process_func(fg):
            return FlowgroupResult(
                flowgroup_name=fg.flowgroup,
                pipeline=fg.pipeline,
                code=f"code_{fg.flowgroup}",
                formatted_code=f"formatted_{fg.flowgroup}",
                source_yaml=Path(f"source_{fg.flowgroup}.yaml"),
                success=True
            )
        
        # Sequential processing
        sequential_results = [process_func(fg) for fg in flowgroups]
        
        # Parallel processing
        processor = ParallelFlowgroupProcessor()
        parallel_results = processor.process_flowgroups_parallel(flowgroups, process_func)
        
        # Sort both by flowgroup_name for comparison
        sequential_results.sort(key=lambda r: r.flowgroup_name)
        parallel_results.sort(key=lambda r: r.flowgroup_name)
        
        # Verify same results
        assert len(sequential_results) == len(parallel_results)
        
        for seq_result, par_result in zip(sequential_results, parallel_results):
            assert seq_result.flowgroup_name == par_result.flowgroup_name
            assert seq_result.pipeline == par_result.pipeline
            assert seq_result.code == par_result.code
            assert seq_result.formatted_code == par_result.formatted_code
            assert seq_result.success == par_result.success

