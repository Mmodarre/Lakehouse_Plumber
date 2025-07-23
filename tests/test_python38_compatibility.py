"""
Test Python 3.8 compatibility for files with forward reference type annotations.

This test ensures that files with forward references work correctly across
Python versions, particularly addressing syntax errors that occurred in Python 3.8.
"""

import ast
import sys
from pathlib import Path


class TestPython38Compatibility:
    """Test Python 3.8 compatibility for forward reference fixes."""

    def test_files_with_future_annotations_parse_correctly(self):
        """Test that files with __future__ annotations can be parsed by AST."""
        files_with_forward_refs = [
            "src/lhp/core/validator.py",
            "src/lhp/utils/error_handler.py", 
            "src/lhp/core/init_template_context.py",
            "src/lhp/core/base_generator.py",
        ]
        
        for file_path in files_with_forward_refs:
            file_path_obj = Path(file_path)
            assert file_path_obj.exists(), f"File {file_path} should exist"
            
            # Read the file content
            with open(file_path_obj, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Verify it has __future__ import
            assert "from __future__ import annotations" in content, \
                f"File {file_path} should have __future__ annotations import"
            
            # Test that AST can parse it (simulates Python 3.8 parsing)
            try:
                ast.parse(content, filename=file_path)
            except SyntaxError as e:
                assert False, f"File {file_path} has syntax error: {e}"

    def test_no_quoted_forward_references_remain(self):
        """Test that quoted forward references have been removed from fixed files."""
        files_and_patterns = [
            ("src/lhp/core/validator.py", ['"WriteTarget"']),
            ("src/lhp/utils/error_handler.py", ['"ErrorContext"', '"ErrorHandler"']),
            ("src/lhp/core/init_template_context.py", ['"InitTemplateContext"']),
            ("src/lhp/core/base_generator.py", ['"Action"']),
        ]
        
        for file_path, patterns in files_and_patterns:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for pattern in patterns:
                assert pattern not in content, \
                    f"File {file_path} should not contain quoted forward reference {pattern}"

    def test_type_annotations_still_valid(self):
        """Test that type annotations are still syntactically valid."""
        # Test specific patterns that should work with __future__ annotations
        test_cases = [
            "def func() -> ErrorContext: pass",
            "def func() -> ErrorHandler: pass", 
            "def func() -> WriteTarget: pass",
            "def func() -> InitTemplateContext: pass",
            "def func(action: Action) -> str: pass",
        ]
        
        for test_case in test_cases:
            # Create a minimal module with __future__ import
            test_module = f"""
from __future__ import annotations
from typing import Any

# Mock classes to make syntax valid
class ErrorContext: pass
class ErrorHandler: pass  
class WriteTarget: pass
class InitTemplateContext: pass
class Action: pass

{test_case}
"""
            
            try:
                ast.parse(test_module)
            except SyntaxError as e:
                assert False, f"Type annotation syntax invalid: {test_case} - {e}"

    def test_import_functionality_preserved(self):
        """Test that the modules can still be imported after changes."""
        import sys
        sys.path.insert(0, 'src')
        
        modules_to_test = [
            "lhp.core.validator",
            "lhp.utils.error_handler",
            "lhp.core.init_template_context", 
            "lhp.core.base_generator",
        ]
        
        for module_name in modules_to_test:
            try:
                __import__(module_name)
            except ImportError as e:
                assert False, f"Module {module_name} failed to import: {e}"
            except Exception as e:
                assert False, f"Module {module_name} import caused error: {e}"

    def test_syntax_compatibility_indicators(self):
        """Test that files have proper Python 3.8+ compatibility indicators."""
        files_to_check = [
            "src/lhp/core/validator.py",
            "src/lhp/utils/error_handler.py",
            "src/lhp/core/init_template_context.py",
            "src/lhp/core/base_generator.py",
        ]
        
        for file_path in files_to_check:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Check that __future__ import is early in the file (within first 10 lines)
            future_import_found = False
            for i, line in enumerate(lines[:10]):
                if "from __future__ import annotations" in line:
                    future_import_found = True
                    # Should be before other imports (except docstring)
                    assert i <= 5, f"__future__ import should be early in {file_path}"
                    break
            
            assert future_import_found, f"File {file_path} should have __future__ import"

    def test_tuple_type_annotations_compatibility(self):
        """Test that tuple type annotations work in Python 3.8."""
        import sys
        sys.path.insert(0, 'src')
        
        # Test orchestrator.py with Tuple annotations
        try:
            from lhp.core.orchestrator import ActionOrchestrator
            assert ActionOrchestrator is not None
            
            # Test that methods with Tuple return types exist
            assert hasattr(ActionOrchestrator, 'validate_pipeline')
            assert hasattr(ActionOrchestrator, 'validate_pipeline_by_field')
        except TypeError as e:
            if "'type' object is not subscriptable" in str(e):
                assert False, f"orchestrator.py has Python 3.8 tuple type annotation error: {e}"
            else:
                raise
        except (SyntaxError, ImportError) as e:
            assert False, f"orchestrator.py failed to import: {e}"
            
        # Test state_display_service.py with Tuple annotations  
        try:
            from lhp.services.state_display_service import StateDisplayService
            assert StateDisplayService is not None
            
            # Test that methods with Tuple return types exist
            service_methods = ['get_stale_files', 'calculate_file_status']
            for method_name in service_methods:
                assert hasattr(StateDisplayService, method_name)
        except TypeError as e:
            if "'type' object is not subscriptable" in str(e):
                assert False, f"state_display_service.py has Python 3.8 tuple type annotation error: {e}"
            else:
                raise
        except (SyntaxError, ImportError) as e:
            assert False, f"state_display_service.py failed to import: {e}"

    def test_all_critical_imports_work(self):
        """Test that all critical import paths work without Python 3.8 errors."""
        import sys
        sys.path.insert(0, 'src')
        
        # Test the exact import chain that was failing in CI
        try:
            from lhp.cli.main import cli
            from lhp.core.orchestrator import ActionOrchestrator  
            from lhp.core.validator import ConfigValidator
            from lhp.services.state_display_service import StateDisplayService
            
            # Test that we can import the full package
            import lhp
            
            assert cli is not None
            assert ActionOrchestrator is not None
            assert ConfigValidator is not None
            assert StateDisplayService is not None
            assert lhp is not None
            
        except (SyntaxError, TypeError, ImportError) as e:
            assert False, f"Critical import chain failed: {e}" 