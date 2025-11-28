#!/usr/bin/env python3
"""
Performance Comparison: v0.7.1 vs Current (with optimizations)

This script creates two virtual environments, installs different versions,
and runs comprehensive benchmarks to measure performance improvements.
"""

import subprocess
import time
import json
import sys
from pathlib import Path
from typing import Dict, Tuple
import shutil

PROJECT_ROOT = Path("/Users/mehdi.modarressi/Documents/Coding/Lakehouse_Plumber")
TEST_PROJECT = PROJECT_ROOT / "Example_Projects/acme_supermarkets_lhp"
VENV_OLD = PROJECT_ROOT / ".venv_v0.7.1"
VENV_NEW = PROJECT_ROOT / ".venv_current"
RESULTS_FILE = PROJECT_ROOT / "benchmark_comparison_detailed.json"


class BenchmarkRunner:
    """Run benchmarks on different LHP versions."""
    
    def __init__(self, venv_path: Path, version: str):
        self.venv_path = venv_path
        self.version = version
        self.lhp_bin = venv_path / "bin" / "lhp"
        
    def setup_venv(self):
        """Create and setup virtual environment."""
        print(f"📦 Setting up {self.version}...")
        
        # Remove existing
        if self.venv_path.exists():
            print(f"  Removing existing venv...")
            shutil.rmtree(self.venv_path)
        
        # Create venv
        print(f"  Creating virtual environment...")
        subprocess.run(
            [sys.executable, "-m", "venv", str(self.venv_path)],
            check=True,
            capture_output=True
        )
        
        # Upgrade pip
        pip = self.venv_path / "bin" / "pip"
        subprocess.run(
            [str(pip), "install", "--upgrade", "pip"],
            check=True,
            capture_output=True
        )
        
        # Install LHP
        if self.version == "v0.7.1":
            print(f"  Installing from git tag v0.7.1...")
            subprocess.run(
                [str(pip), "install", 
                 "git+https://github.com/mmodarre/lakehouse_plumber.git@v0.7.1"],
                check=True,
                capture_output=True
            )
        else:
            print(f"  Installing current version from source...")
            subprocess.run(
                [str(pip), "install", "-e", str(PROJECT_ROOT)],
                check=True,
                capture_output=True,
                cwd=str(PROJECT_ROOT)
            )
        
        # Verify
        result = subprocess.run(
            [str(self.lhp_bin), "--version"],
            capture_output=True,
            text=True
        )
        print(f"  ✅ Installed: {result.stdout.strip()}")
        print()
    
    def clean_project(self):
        """Remove generated files and state."""
        generated = TEST_PROJECT / "generated"
        state = TEST_PROJECT / ".lhp_state"
        
        if generated.exists():
            shutil.rmtree(generated)
        if state.exists():
            shutil.rmtree(state)
    
    def run_generation(self, args: list = None) -> Tuple[float, str]:
        """Run lhp generate and measure time."""
        if args is None:
            args = []
        
        cmd = [str(self.lhp_bin), "generate", "-e", "dev"] + args
        
        start = time.time()
        result = subprocess.run(
            cmd,
            cwd=str(TEST_PROJECT),
            capture_output=True,
            text=True
        )
        elapsed = time.time() - start
        
        return elapsed, result.stdout + result.stderr
    
    def count_files(self, directory: str = "generated") -> int:
        """Count generated files."""
        gen_dir = TEST_PROJECT / directory
        if not gen_dir.exists():
            return 0
        return sum(1 for _ in gen_dir.rglob("*.py"))


def run_benchmark_suite():
    """Run complete benchmark suite."""
    print("=" * 60)
    print("LakehousePlumber Performance Comparison")
    print("=" * 60)
    print()
    print("Comparing:")
    print("  - v0.7.1 (before optimizations)")
    print("  - Current version (with optimizations)")
    print()
    
    results = {
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "project": "acme_supermarkets_lhp",
        "tests": {}
    }
    
    # Setup environments
    print("🔧 SETUP PHASE")
    print("=" * 60)
    print()
    
    runner_old = BenchmarkRunner(VENV_OLD, "v0.7.1")
    runner_new = BenchmarkRunner(VENV_NEW, "current")
    
    runner_old.setup_venv()
    runner_new.setup_venv()
    
    print()
    print("🚀 BENCHMARK PHASE")
    print("=" * 60)
    print()
    
    # Test 1: Clean Generation
    print("📊 Test 1: Clean Generation (Full Rebuild)")
    print("-" * 60)
    
    runner_old.clean_project()
    print("  Running v0.7.1...")
    time_old_clean, _ = runner_old.run_generation(["--force"])
    files_old = runner_old.count_files()
    
    runner_new.clean_project()
    print("  Running current...")
    time_new_clean, _ = runner_new.run_generation(["--force"])
    files_new = runner_new.count_files()
    
    speedup_clean = time_old_clean / time_new_clean if time_new_clean > 0 else 0
    
    results["tests"]["clean_generation"] = {
        "v0.7.1_time": round(time_old_clean, 2),
        "current_time": round(time_new_clean, 2),
        "speedup": round(speedup_clean, 2),
        "files_generated": files_new
    }
    
    print(f"  v0.7.1:    {time_old_clean:.2f}s ({files_old} files)")
    print(f"  Current:   {time_new_clean:.2f}s ({files_new} files)")
    print(f"  Speedup:   {speedup_clean:.2f}x")
    print()
    
    # Test 2: Incremental (No Changes)
    print("📊 Test 2: Incremental Generation (No Changes)")
    print("-" * 60)
    
    # Generate once first
    runner_old.clean_project()
    runner_old.run_generation()
    
    print("  Running v0.7.1 (incremental)...")
    time_old_incr, _ = runner_old.run_generation()
    
    runner_new.clean_project()
    runner_new.run_generation()
    
    print("  Running current (incremental)...")
    time_new_incr, _ = runner_new.run_generation()
    
    speedup_incr = time_old_incr / time_new_incr if time_new_incr > 0 else 0
    
    results["tests"]["incremental_no_changes"] = {
        "v0.7.1_time": round(time_old_incr, 2),
        "current_time": round(time_new_incr, 2),
        "speedup": round(speedup_incr, 2)
    }
    
    print(f"  v0.7.1:    {time_old_incr:.2f}s")
    print(f"  Current:   {time_new_incr:.2f}s")
    print(f"  Speedup:   {speedup_incr:.2f}x")
    print()
    
    # Test 3: Force Regeneration (Warm Cache)
    print("📊 Test 3: Force Regeneration (Warm Cache)")
    print("-" * 60)
    
    # Generate once first
    runner_old.clean_project()
    runner_old.run_generation(["--force"])
    
    print("  Running v0.7.1 (force, 2nd run)...")
    time_old_force, _ = runner_old.run_generation(["--force"])
    
    runner_new.clean_project()
    runner_new.run_generation(["--force"])
    
    print("  Running current (force, 2nd run)...")
    time_new_force, _ = runner_new.run_generation(["--force"])
    
    speedup_force = time_old_force / time_new_force if time_new_force > 0 else 0
    
    results["tests"]["force_warm_cache"] = {
        "v0.7.1_time": round(time_old_force, 2),
        "current_time": round(time_new_force, 2),
        "speedup": round(speedup_force, 2)
    }
    
    print(f"  v0.7.1:    {time_old_force:.2f}s")
    print(f"  Current:   {time_new_force:.2f}s")
    print(f"  Speedup:   {speedup_force:.2f}x")
    print()
    
    # Calculate summary
    avg_speedup = (speedup_clean + speedup_incr + speedup_force) / 3
    results["summary"] = {
        "average_speedup": round(avg_speedup, 2),
        "total_time_saved_per_run": round(
            ((time_old_clean + time_old_incr + time_old_force) / 3) - 
            ((time_new_clean + time_new_incr + time_new_force) / 3),
            2
        )
    }
    
    # Print summary
    print()
    print("=" * 60)
    print("📈 PERFORMANCE SUMMARY")
    print("=" * 60)
    print()
    print(f"{'Test':<35} | {'v0.7.1':>10} | {'Current':>10} | {'Speedup':>8}")
    print(f"{'-' * 35}-|-{'-' * 10}-|-{'-' * 10}-|-{'-' * 8}")
    
    for test_name, test_data in results["tests"].items():
        display_name = test_name.replace("_", " ").title()
        print(f"{display_name:<35} | "
              f"{test_data['v0.7.1_time']:>9.2f}s | "
              f"{test_data['current_time']:>9.2f}s | "
              f"{test_data['speedup']:>7.2f}x")
    
    print()
    print(f"Average Speedup: {avg_speedup:.2f}x")
    print(f"Time Saved (avg): {results['summary']['total_time_saved_per_run']:.2f}s per run")
    print()
    
    print("Key Optimizations:")
    print("  ✅ YAML Parsing Cache (mtime-based)")
    print("  ✅ Parallel Flowgroup Processing")
    print("  ✅ SmartFileWriter Checksum Optimization")
    print("  ✅ Granular Dependency Tracking")
    print()
    
    # Save results
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    
    print("=" * 60)
    print()
    print(f"✅ Benchmark complete!")
    print(f"📊 Detailed results saved to: {RESULTS_FILE}")
    print()
    print("💡 To cleanup virtual environments:")
    print(f"   rm -rf {VENV_OLD} {VENV_NEW}")
    
    return results


if __name__ == "__main__":
    try:
        results = run_benchmark_suite()
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

