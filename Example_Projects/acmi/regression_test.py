#!/usr/bin/env python3
"""
Regression Testing Script for ACMI Import Management Changes
============================================================

This script compares newly generated pipeline files with the baseline
to ensure that import management changes only affect the intended files
(specifically unirate_api_ingestion) and don't introduce regressions.

Usage:
    python regression_test.py [--verbose]
"""

import os
import sys
import difflib
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set


class RegressionTester:
    """Automated regression testing for pipeline generation."""
    
    def __init__(self, baseline_dir: str = "baseline_validation", 
                 current_dir: str = "generated", verbose: bool = False):
        self.baseline_dir = Path(baseline_dir)
        self.current_dir = Path(current_dir) 
        self.verbose = verbose
        self.differences = {}
        self.missing_files = []
        self.extra_files = []
        
    def run_comparison(self) -> bool:
        """Run full comparison and return True if tests pass."""
        print("🔍 Starting regression testing...")
        print(f"   Baseline: {self.baseline_dir}")
        print(f"   Current:  {self.current_dir}")
        
        # Get file lists
        baseline_files = self._get_python_files(self.baseline_dir)
        current_files = self._get_python_files(self.current_dir)
        
        print(f"   Baseline files: {len(baseline_files)}")
        print(f"   Current files:  {len(current_files)}")
        
        # Check for missing/extra files
        self._check_file_differences(baseline_files, current_files)
        
        # Compare content of matching files
        common_files = baseline_files & current_files
        self._compare_file_contents(common_files)
        
        # Report results
        return self._report_results()
    
    def _get_python_files(self, directory: Path) -> Set[str]:
        """Get relative paths of all Python files in directory."""
        if not directory.exists():
            return set()
        
        files = set()
        for py_file in directory.rglob("*.py"):
            rel_path = py_file.relative_to(directory)
            files.add(str(rel_path))
        return files
    
    def _check_file_differences(self, baseline_files: Set[str], current_files: Set[str]):
        """Check for missing or extra files."""
        self.missing_files = list(baseline_files - current_files)
        self.extra_files = list(current_files - baseline_files)
        
        if self.missing_files:
            print(f"⚠️  Missing files ({len(self.missing_files)}):")
            for file in sorted(self.missing_files):
                print(f"     - {file}")
        
        if self.extra_files:
            print(f"➕ Extra files ({len(self.extra_files)}):")
            for file in sorted(self.extra_files):
                print(f"     + {file}")
    
    def _compare_file_contents(self, common_files: Set[str]):
        """Compare content of files that exist in both directories."""
        print(f"\n📄 Comparing {len(common_files)} common files...")
        
        identical_count = 0
        different_count = 0
        
        for rel_path in sorted(common_files):
            baseline_file = self.baseline_dir / rel_path
            current_file = self.current_dir / rel_path
            
            try:
                baseline_content = baseline_file.read_text(encoding='utf-8')
                current_content = current_file.read_text(encoding='utf-8')
                
                if baseline_content == current_content:
                    identical_count += 1
                    if self.verbose:
                        print(f"   ✅ {rel_path}")
                else:
                    different_count += 1
                    print(f"   ❌ {rel_path}")
                    self.differences[rel_path] = self._generate_diff(
                        baseline_content, current_content, rel_path
                    )
                    
            except Exception as e:
                print(f"   ⚠️  Error comparing {rel_path}: {e}")
                different_count += 1
        
        print(f"\n📊 Comparison Summary:")
        print(f"   ✅ Identical files: {identical_count}")
        print(f"   ❌ Different files: {different_count}")
    
    def _generate_diff(self, baseline: str, current: str, filename: str) -> List[str]:
        """Generate unified diff for changed files."""
        baseline_lines = baseline.splitlines(keepends=True)
        current_lines = current.splitlines(keepends=True)
        
        diff = list(difflib.unified_diff(
            baseline_lines, current_lines,
            fromfile=f"baseline/{filename}",
            tofile=f"current/{filename}",
            lineterm=""
        ))
        return diff
    
    def _report_results(self) -> bool:
        """Report final results and return success status."""
        print("\n" + "="*60)
        print("🎯 REGRESSION TEST RESULTS")
        print("="*60)
        
        # Expected changes
        expected_changes = {"unirate_api_ingestion/api_unirate_ingestion_bronze.py"}
        
        # Check if only expected files changed
        changed_files = set(self.differences.keys())
        unexpected_changes = changed_files - expected_changes
        expected_but_missing = expected_changes - changed_files
        
        success = True
        
        # Report on expected changes
        if expected_changes & changed_files:
            print("✅ Expected changes detected:")
            for file in sorted(expected_changes & changed_files):
                print(f"     📝 {file}")
        
        # Report unexpected changes  
        if unexpected_changes:
            print(f"\n❌ UNEXPECTED CHANGES ({len(unexpected_changes)}):")
            for file in sorted(unexpected_changes):
                print(f"     ⚠️  {file}")
            success = False
        
        # Report missing expected changes
        if expected_but_missing:
            print(f"\n⚠️  Expected changes not found:")
            for file in sorted(expected_but_missing):
                print(f"     ❓ {file}")
        
        # Report file count issues
        if self.missing_files or self.extra_files:
            print(f"\n❌ FILE COUNT ISSUES:")
            if self.missing_files:
                print(f"     Missing: {len(self.missing_files)} files")
            if self.extra_files:
                print(f"     Extra: {len(self.extra_files)} files")
            success = False
        
        # Show detailed diffs if requested
        if self.verbose and self.differences:
            print(f"\n📋 DETAILED DIFFERENCES:")
            for filename, diff in self.differences.items():
                print(f"\n--- {filename} ---")
                for line in diff[:50]:  # Limit to first 50 lines
                    print(line.rstrip())
                if len(diff) > 50:
                    print(f"... (truncated, {len(diff) - 50} more lines)")
        
        # Final verdict
        print("\n" + "="*60)
        if success:
            print("🎉 REGRESSION TESTS PASSED!")
            print("✅ Only expected files changed, no regressions detected.")
        else:
            print("💥 REGRESSION TESTS FAILED!")
            print("❌ Unexpected changes detected - review required.")
        print("="*60)
        
        return success


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run regression tests for import management changes")
    parser.add_argument("--verbose", "-v", action="store_true", 
                      help="Show detailed diff output")
    parser.add_argument("--baseline", default="baseline_validation",
                      help="Baseline directory (default: baseline_validation)")  
    parser.add_argument("--current", default="generated",
                      help="Current generation directory (default: generated)")
    
    args = parser.parse_args()
    
    tester = RegressionTester(
        baseline_dir=args.baseline,
        current_dir=args.current, 
        verbose=args.verbose
    )
    
    success = tester.run_comparison()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 