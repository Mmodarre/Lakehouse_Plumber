"""
End-to-end integration tests for job orchestration file generation.

Tests the complete workflow of generating job orchestration files using
the `lhp deps -b` command with and without job configuration.
"""

import pytest
import shutil
import os
import hashlib
from pathlib import Path
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestJobOrchestrationE2E:
    """E2E tests for job orchestration file generation."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Set up fresh test project for each test method."""
        # Copy fixture to isolated temp directory
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)
        
        # Change to project directory
        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)
        
        # Set up paths
        self.resources_dir = self.project_root / "resources"
        self.resources_baseline_dir = self.project_root / "resources_baseline"
        
        yield
        
        # Cleanup
        os.chdir(self.original_cwd)

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def run_deps_command(self, *args) -> tuple:
        """Run lhp deps command and return (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ['deps', *args])
        return result.exit_code, result.output

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare hashes of two files, return difference or empty string."""
        
        def get_file_hash(file_path: Path) -> str:
            """Calculate SHA256 hash of file contents."""
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        
        try:
            hash1 = get_file_hash(file1)
            hash2 = get_file_hash(file2)
            
            if hash1 != hash2:
                return f"Hash mismatch: {file1.name} vs {file2.name}"
            return ""
        except (OSError, IOError, UnicodeDecodeError) as e:
            return f"Error comparing files: {e}"

    def uncomment_job_names(self):
        """Uncomment all #job_name: lines in flowgroup YAML files."""
        pipelines_dir = self.project_root / "pipelines"
        
        # Find all YAML files recursively
        yaml_files = list(pipelines_dir.rglob("*.yaml")) + list(pipelines_dir.rglob("*.yml"))
        
        for yaml_file in yaml_files:
            content = yaml_file.read_text()
            # Replace #job_name: with job_name: (uncomment)
            modified_content = content.replace("#job_name:", "job_name:")
            yaml_file.write_text(modified_content)
        
        print(f"Uncommented job_name in {len(yaml_files)} YAML files")

    def uncomment_lines_in_file(self, file_path: Path, line_numbers: list):
        """Uncomment specific lines in a file (remove leading # while preserving indentation).
        
        Args:
            file_path: Path to the file to modify
            line_numbers: List of line numbers to uncomment (1-indexed)
        """
        lines = file_path.read_text().splitlines(keepends=True)
        
        for line_num in line_numbers:
            idx = line_num - 1  # Convert to 0-indexed
            if idx < len(lines):
                line = lines[idx]
                # Find the # character and remove it along with one space after it
                if '#' in line:
                    # Preserve leading whitespace, remove # and one space if present
                    leading_spaces = len(line) - len(line.lstrip())
                    content = line.lstrip()
                    if content.startswith('#'):
                        content = content[1:]  # Remove #
                        if content.startswith(' '):
                            content = content[1:]  # Remove one space after #
                        lines[idx] = ' ' * leading_spaces + content
                        if not lines[idx].endswith('\n'):
                            lines[idx] += '\n'
        
        file_path.write_text(''.join(lines))

    # ========================================================================
    # JOB ORCHESTRATION TESTS
    # ========================================================================

    def test_deps_bundle_without_job_config(self):
        """Test lhp deps -b generates correct orchestration job without job config."""
        # Ensure resources directory exists
        self.resources_dir.mkdir(parents=True, exist_ok=True)
        
        # Run command: lhp deps -b
        exit_code, output = self.run_deps_command('-b')
        
        # Assert command succeeded
        assert exit_code == 0, f"Command should succeed: {output}"
        
        # Check generated file exists (now uses project name from lhp.yaml: acme_edw)
        generated_file = self.resources_dir / "acme_edw_orchestration.job.yml"
        assert generated_file.exists(), f"Generated file should exist: {generated_file}"
        
        # Compare with baseline
        baseline_file = self.resources_baseline_dir / "acme_edw_orchestration.job.yml"
        assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"
        
        # Hash comparison
        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Generated file should match baseline: {hash_diff}"
        
        print("✅ Job orchestration file (without job config) matches baseline")

    def test_deps_bundle_with_job_config(self):
        """Test lhp deps -b -jc generates correct orchestration job with job config applied."""
        # Ensure resources directory exists
        self.resources_dir.mkdir(parents=True, exist_ok=True)
        
        # Run command: lhp deps -b -jc config/job_config.yaml
        exit_code, output = self.run_deps_command('-b', '-jc', 'config/job_config.yaml')
        
        # Assert command succeeded
        assert exit_code == 0, f"Command should succeed: {output}"
        
        # Check generated file exists (now uses project name from lhp.yaml: acme_edw)
        generated_file = self.resources_dir / "acme_edw_orchestration.job.yml"
        assert generated_file.exists(), f"Generated file should exist: {generated_file}"
        
        # Compare with job-config baseline
        baseline_file = self.resources_baseline_dir / "acme_edw_orchestration-JC.job.yml"
        assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"
        
        # Hash comparison
        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Generated file should match baseline with job config: {hash_diff}"
        
        print("✅ Job orchestration file (with job config) matches baseline")

    # ========================================================================
    # MULTI-JOB ORCHESTRATION TESTS
    # ========================================================================

    def test_multi_job_with_default_master(self):
        """Test multi-job generation with default master job name."""
        # Uncomment all job_name lines to enable multi-job mode
        self.uncomment_job_names()
        
        # Ensure resources directory exists
        self.resources_dir.mkdir(parents=True, exist_ok=True)
        
        # Run command: lhp deps -b -jc config/job_config.yaml
        exit_code, output = self.run_deps_command('-b', '-jc', 'config/job_config.yaml')
        
        # Assert command succeeded
        assert exit_code == 0, f"Command should succeed: {output}"
        
        # Expected files: 5 individual job files + 1 master job
        expected_files = [
            "j_one.job.yml",
            "j_two.job.yml",
            "j_three.job.yml",
            "j_four.job.yml",
            "j_nine.job.yml",
            "acme_edw_master.job.yml"  # Default master job name
        ]
        
        # Check all expected files exist
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            assert generated_file.exists(), f"Generated file should exist: {generated_file}"
        
        # Compare each file with baseline
        baseline_dir = self.resources_baseline_dir / "indvidual_jobs"
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            baseline_file = baseline_dir / filename
            
            assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"
            
            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert hash_diff == "", f"File {filename} should match baseline: {hash_diff}"
        
        print("✅ Multi-job orchestration with default master job name matches baseline")

    def test_multi_job_with_custom_master_name(self):
        """Test multi-job generation with custom master job name."""
        # Uncomment all job_name lines to enable multi-job mode
        self.uncomment_job_names()
        
        # Uncomment lines 13-14 in job_config.yaml to set custom master job name
        job_config_file = self.project_root / "config" / "job_config.yaml"
        self.uncomment_lines_in_file(job_config_file, [13, 14])
        
        # Ensure resources directory exists
        self.resources_dir.mkdir(parents=True, exist_ok=True)
        
        # Run command: lhp deps -b -jc config/job_config.yaml
        exit_code, output = self.run_deps_command('-b', '-jc', 'config/job_config.yaml')
        
        # Assert command succeeded
        assert exit_code == 0, f"Command should succeed: {output}"
        
        # Expected files: 5 individual job files + 1 custom-named master job
        expected_files = [
            "j_one.job.yml",
            "j_two.job.yml",
            "j_three.job.yml",
            "j_four.job.yml",
            "j_nine.job.yml",
            "mehdi_master_job.job.yml"  # Custom master job name
        ]
        
        # Check all expected files exist
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            assert generated_file.exists(), f"Generated file should exist: {generated_file}"
        
        # Compare each file with baseline
        baseline_dir = self.resources_baseline_dir / "indvidual_jobs"
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            baseline_file = baseline_dir / filename
            
            assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"
            
            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert hash_diff == "", f"File {filename} should match baseline: {hash_diff}"
        
        print("✅ Multi-job orchestration with custom master job name matches baseline")

    def test_multi_job_without_master(self):
        """Test multi-job generation with master job disabled."""
        # Uncomment all job_name lines to enable multi-job mode
        self.uncomment_job_names()
        
        # Uncomment line 15 in job_config.yaml to disable master job
        job_config_file = self.project_root / "config" / "job_config.yaml"
        self.uncomment_lines_in_file(job_config_file, [15])
        
        # Ensure resources directory exists
        self.resources_dir.mkdir(parents=True, exist_ok=True)
        
        # Run command: lhp deps -b -jc config/job_config.yaml
        exit_code, output = self.run_deps_command('-b', '-jc', 'config/job_config.yaml')
        
        # Assert command succeeded
        assert exit_code == 0, f"Command should succeed: {output}"
        
        # Expected files: 5 individual job files only (NO master job)
        expected_files = [
            "j_one.job.yml",
            "j_two.job.yml",
            "j_three.job.yml",
            "j_four.job.yml",
            "j_nine.job.yml"
        ]
        
        # Check all expected files exist
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            assert generated_file.exists(), f"Generated file should exist: {generated_file}"
        
        # Verify master job file does NOT exist
        master_file = self.resources_dir / "acme_edw_master.job.yml"
        assert not master_file.exists(), f"Master job file should NOT exist: {master_file}"
        
        # Compare each file with baseline
        baseline_dir = self.resources_baseline_dir / "indvidual_jobs"
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            baseline_file = baseline_dir / filename
            
            assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"
            
            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert hash_diff == "", f"File {filename} should match baseline: {hash_diff}"
        
        print("✅ Multi-job orchestration without master job matches baseline")

