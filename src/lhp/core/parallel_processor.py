"""Parallel processing utilities for flowgroup generation."""

import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.config import FlowGroup

logger = logging.getLogger(__name__)


@dataclass
class FlowgroupResult:
    """Result of processing a single flowgroup."""
    flowgroup_name: str
    pipeline: str
    code: str
    formatted_code: str
    source_yaml: Optional[Path]
    success: bool
    error: Optional[str] = None
    processed_flowgroup: Optional['FlowGroup'] = None  # Store processed flowgroup to avoid re-processing


class ParallelFlowgroupProcessor:
    """Parallel processor for flowgroup code generation.
    
    Uses ThreadPoolExecutor for I/O-bound operations (YAML parsing, file reading).
    """
    
    def __init__(self, max_workers: Optional[int] = None) -> None:
        """Initialize parallel processor.
        
        Args:
            max_workers: Maximum worker threads (default: CPU count, max 8)
        """
        self.max_workers: int = max_workers or min(multiprocessing.cpu_count(), 8)
        self.logger: logging.Logger = logging.getLogger(__name__)

    def process_flowgroups_parallel(
        self,
        flowgroups: List['FlowGroup'],
        process_func: Callable[['FlowGroup'], FlowgroupResult],
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> List[FlowgroupResult]:
        """Process flowgroups in parallel.
        
        Args:
            flowgroups: List of flowgroups to process
            process_func: Function to process each flowgroup
            progress_callback: Optional callback(completed, total) for progress
            
        Returns:
            List of FlowgroupResult objects
        """
        if not flowgroups:
            return []

        # For small batches, sequential is faster due to overhead
        if len(flowgroups) < 4:
            return [process_func(fg) for fg in flowgroups]

        results: List[FlowgroupResult] = []
        total: int = len(flowgroups)
        completed: int = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_fg = {
                executor.submit(process_func, fg): fg
                for fg in flowgroups
            }

            # Collect results as they complete
            for future in as_completed(future_to_fg):
                fg = future_to_fg[future]
                try:
                    result: FlowgroupResult = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Error processing {fg.flowgroup}: {e}")
                    results.append(FlowgroupResult(
                        flowgroup_name=fg.flowgroup,
                        pipeline=fg.pipeline,
                        code="",
                        formatted_code="",
                        source_yaml=None,
                        success=False,
                        error=str(e)
                    ))

                completed += 1
                if progress_callback:
                    progress_callback(completed, total)

        return results

