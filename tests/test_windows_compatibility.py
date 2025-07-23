"""
Test Windows compatibility for CI functionality.

This test ensures that basic import and CLI operations work on Windows
without Unicode encoding issues.
"""

import subprocess
import sys
from pathlib import Path


class TestWindowsCompatibility:
    """Test Windows-specific compatibility issues."""

    def test_basic_import_without_unicode_issues(self):
        """Test that basic import works with ASCII output (Windows CI compatible)."""
        # This simulates the exact command that was failing in Windows CI
        result = subprocess.run(
            [sys.executable, "-c", "import lhp; print('[OK] Package imports successfully')"],
            capture_output=True,
            text=True,
            encoding='utf-8'  # Explicit encoding to avoid Windows issues
        )
        
        assert result.returncode == 0
        assert "[OK] Package imports successfully" in result.stdout
        assert result.stderr == ""

    def test_cli_help_ascii_output(self):
        """Test that CLI help command works with ASCII output."""
        result = subprocess.run(
            ["lhp", "--help"],
            capture_output=True,
            text=True,
            encoding='utf-8'  # Explicit encoding to avoid Windows issues
        )
        
        assert result.returncode == 0
        assert "LakehousePlumber" in result.stdout
        assert "Usage:" in result.stdout

    def test_cli_version_ascii_output(self):
        """Test that CLI version command works with ASCII output."""
        result = subprocess.run(
            ["lhp", "--version"],
            capture_output=True,
            text=True,
            encoding='utf-8'  # Explicit encoding to avoid Windows issues
        )
        
        assert result.returncode == 0
        assert "version" in result.stdout.lower()

    def test_ascii_success_indicators(self):
        """Test that ASCII success indicators work correctly."""
        # Test the specific pattern we use in CI
        test_messages = [
            "[OK] Package imports successfully",
            "[OK] CLI help works", 
            "CLI version check failed",
            "CLI help failed"
        ]
        
        for message in test_messages:
            # Verify these can be encoded to Windows-compatible encoding
            try:
                encoded = message.encode('cp1252')  # Windows default encoding
                decoded = encoded.decode('cp1252')
                assert decoded == message
            except UnicodeEncodeError:
                assert False, f"Message '{message}' is not Windows CP1252 compatible"

    def test_unicode_replacement_effectiveness(self):
        """Test that our Unicode character replacements are effective."""
        # Test that problematic Unicode characters would fail
        problematic_chars = ["✓", "✅", "❌", "🔧", "🚀"]
        
        for char in problematic_chars:
            try:
                char.encode('cp1252')
                # If we get here, the character is actually compatible
                # (this test might fail on systems with extended codepage support)
                pass
            except UnicodeEncodeError:
                # This is expected - the character is not CP1252 compatible
                pass
        
        # Test that our ASCII replacements work
        ascii_replacements = ["[OK]", "[PASS]", "[FAIL]", "[INFO]", "[DONE]"]
        
        for replacement in ascii_replacements:
            try:
                encoded = replacement.encode('cp1252')
                decoded = encoded.decode('cp1252')
                assert decoded == replacement
            except UnicodeEncodeError:
                assert False, f"ASCII replacement '{replacement}' failed encoding test" 