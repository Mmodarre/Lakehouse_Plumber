"""Tests for yaml_loader utility functions."""

import pytest
import tempfile
from pathlib import Path

from lhp.utils.yaml_loader import (
    load_yaml_file,
    load_yaml_documents_all,
    load_yaml_if_exists,
    safe_load_yaml_with_fallback,
)
from lhp.utils.error_formatter import MultiDocumentError, LHPError, ErrorCategory


class TestLoadYAMLDocumentsAll:
    """Test load_yaml_documents_all() function for multi-document YAML support."""
    
    def test_single_document(self):
        """Test loading single document returns list with one element (backward compat)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: action1
    type: load
    target: table1
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            documents = load_yaml_documents_all(yaml_file)
            assert len(documents) == 1
            assert documents[0]['pipeline'] == 'test_pipeline'
            assert documents[0]['flowgroup'] == 'test_flowgroup'
            assert len(documents[0]['actions']) == 1
        finally:
            yaml_file.unlink()
    
    def test_multiple_documents_with_separator(self):
        """Test loading multiple documents separated by ---."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
actions:
  - name: action1
    type: load
    target: table1
---
pipeline: test_pipeline
flowgroup: flowgroup2
actions:
  - name: action2
    type: transform
    source: table1
    target: table2
---
pipeline: test_pipeline
flowgroup: flowgroup3
actions:
  - name: action3
    type: write
    source: table2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            documents = load_yaml_documents_all(yaml_file)
            assert len(documents) == 3
            assert documents[0]['flowgroup'] == 'flowgroup1'
            assert documents[1]['flowgroup'] == 'flowgroup2'
            assert documents[2]['flowgroup'] == 'flowgroup3'
            assert documents[0]['actions'][0]['name'] == 'action1'
            assert documents[1]['actions'][0]['name'] == 'action2'
            assert documents[2]['actions'][0]['name'] == 'action3'
        finally:
            yaml_file.unlink()
    
    def test_empty_file(self):
        """Test loading empty file returns empty list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            documents = load_yaml_documents_all(yaml_file)
            assert documents == []
        finally:
            yaml_file.unlink()
    
    def test_empty_documents_filtered(self):
        """Test that None/empty documents are filtered out."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
---
---
pipeline: test_pipeline
flowgroup: flowgroup2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            documents = load_yaml_documents_all(yaml_file)
            # Should only have 2 non-empty documents
            assert len(documents) == 2
            assert documents[0]['flowgroup'] == 'flowgroup1'
            assert documents[1]['flowgroup'] == 'flowgroup2'
        finally:
            yaml_file.unlink()
    
    def test_malformed_yaml_raises_error(self):
        """Test that malformed YAML raises ValueError with clear message."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
actions: [missing bracket
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                load_yaml_documents_all(yaml_file)
            
            assert "Invalid YAML" in str(exc_info.value)
            assert str(yaml_file) in str(exc_info.value)
        finally:
            yaml_file.unlink()
    
    def test_file_not_found_raises_error(self):
        """Test that missing file raises ValueError."""
        non_existent_file = Path("/tmp/non_existent_file_xyz123.yaml")
        
        with pytest.raises(ValueError) as exc_info:
            load_yaml_documents_all(non_existent_file)
        
        assert "File not found" in str(exc_info.value)
    
    def test_multiple_documents_different_structures(self):
        """Test loading documents with different structures."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
presets:
  - preset1
  - preset2
---
pipeline: test_pipeline
flowgroup: flowgroup2
use_template: my_template
template_parameters:
  param1: value1
  param2: value2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            documents = load_yaml_documents_all(yaml_file)
            assert len(documents) == 2
            assert 'presets' in documents[0]
            assert documents[0]['presets'] == ['preset1', 'preset2']
            assert 'use_template' in documents[1]
            assert documents[1]['use_template'] == 'my_template'
        finally:
            yaml_file.unlink()
    
    def test_error_context_in_message(self):
        """Test that error_context parameter is used in error messages."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                load_yaml_documents_all(yaml_file, error_context="multi-flowgroup file")
            
            assert "multi-flowgroup file" in str(exc_info.value)
        finally:
            yaml_file.unlink()


class TestLoadYAMLFileBackwardCompat:
    """Ensure existing load_yaml_file() still works as expected."""
    
    def test_load_yaml_file_single_doc(self):
        """Test that load_yaml_file still works for single documents."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: test_flowgroup
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            content = load_yaml_file(yaml_file)
            assert content['pipeline'] == 'test_pipeline'
            assert content['flowgroup'] == 'test_flowgroup'
        finally:
            yaml_file.unlink()


class TestLoadYAMLFileValidation:
    """Test load_yaml_file() validation behavior with single-document constraint."""
    
    def test_load_yaml_file_rejects_multi_document(self, tmp_path):
        """load_yaml_file should reject multi-document files with clear error."""
        multi_doc_file = tmp_path / "multi.yaml"
        multi_doc_file.write_text("a: 1\n---\nb: 2")
        
        with pytest.raises(MultiDocumentError) as exc_info:
            load_yaml_file(multi_doc_file)
        
        error = exc_info.value
        assert "Expected 1, Found 2" in str(error)
        assert "load_yaml_documents_all" in str(error)
    
    def test_load_yaml_file_rejects_empty_file(self, tmp_path):
        """load_yaml_file should reject empty files."""
        empty_file = tmp_path / "empty.yaml"
        empty_file.write_text("")
        
        with pytest.raises(MultiDocumentError) as exc_info:
            load_yaml_file(empty_file)
        
        error = exc_info.value
        assert "Expected 1, Found 0" in str(error)
    
    def test_load_yaml_file_accepts_single_document(self, tmp_path):
        """load_yaml_file should accept single-document files."""
        single_doc = tmp_path / "single.yaml"
        single_doc.write_text("key: value")
        
        result = load_yaml_file(single_doc)
        assert result == {"key": "value"}
    
    def test_load_yaml_file_null_document_with_allow_empty_true(self, tmp_path):
        """Single null document should return {} when allow_empty=True."""
        null_doc = tmp_path / "null.yaml"
        null_doc.write_text("---\n")  # Single document with null content
        
        result = load_yaml_file(null_doc, allow_empty=True)
        assert result == {}
    
    def test_load_yaml_file_null_document_with_allow_empty_false(self, tmp_path):
        """Single null document should return None when allow_empty=False."""
        null_doc = tmp_path / "null.yaml"
        null_doc.write_text("---\n")
        
        result = load_yaml_file(null_doc, allow_empty=False)
        assert result is None
    
    def test_error_has_lhp_error_code(self, tmp_path):
        """MultiDocumentError should have proper LHP error code."""
        multi_doc = tmp_path / "multi.yaml"
        multi_doc.write_text("a: 1\n---\nb: 2")
        
        with pytest.raises(MultiDocumentError) as exc_info:
            load_yaml_file(multi_doc)
        
        assert exc_info.value.code == "LHP-IO-003"


class TestLoadYAMLIfExists:
    """Test load_yaml_if_exists() function for optional file loading."""
    
    def test_load_existing_file(self, tmp_path):
        """Test loading file that exists."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("key: value\nnum: 42")
        
        result = load_yaml_if_exists(config_file)
        assert result == {"key": "value", "num": 42}
    
    def test_load_missing_file_returns_default_none(self, tmp_path):
        """Test loading missing file returns None by default."""
        missing_file = tmp_path / "missing.yaml"
        
        result = load_yaml_if_exists(missing_file)
        assert result is None
    
    def test_load_missing_file_returns_custom_default(self, tmp_path):
        """Test loading missing file returns custom default value."""
        missing_file = tmp_path / "missing.yaml"
        
        result = load_yaml_if_exists(missing_file, default_value={})
        assert result == {}
        
        result = load_yaml_if_exists(missing_file, default_value={"default": "config"})
        assert result == {"default": "config"}
    
    def test_load_with_error_context(self, tmp_path):
        """Test that error_context is passed through to load_yaml_file."""
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("a: 1\n---\nb: 2")  # Multi-document
        
        with pytest.raises(MultiDocumentError) as exc_info:
            load_yaml_if_exists(bad_file, error_context="test config")
        
        assert "test config" in str(exc_info.value)
    
    def test_load_empty_file_with_allow_empty(self, tmp_path):
        """Test loading empty file with allow_empty parameter."""
        empty_file = tmp_path / "empty.yaml"
        empty_file.write_text("---\n")  # Single null document
        
        result = load_yaml_if_exists(empty_file, allow_empty=True)
        assert result == {}
        
        result = load_yaml_if_exists(empty_file, allow_empty=False)
        assert result is None


class TestSafeLoadYAMLWithFallback:
    """Test safe_load_yaml_with_fallback() function for error-tolerant loading."""
    
    def test_load_valid_file(self, tmp_path):
        """Test loading valid file returns content."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("key: value\nnum: 42")
        
        result = safe_load_yaml_with_fallback(config_file)
        assert result == {"key": "value", "num": 42}
    
    def test_load_missing_file_returns_fallback(self, tmp_path):
        """Test loading missing file returns fallback value."""
        missing_file = tmp_path / "missing.yaml"
        
        result = safe_load_yaml_with_fallback(missing_file)
        assert result == {}  # Default fallback
        
        result = safe_load_yaml_with_fallback(missing_file, fallback_value={"default": "value"})
        assert result == {"default": "value"}
    
    def test_load_invalid_yaml_returns_fallback(self, tmp_path):
        """Test loading invalid YAML returns fallback value."""
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("invalid: yaml: [")
        
        result = safe_load_yaml_with_fallback(bad_file)
        assert result == {}
        
        result = safe_load_yaml_with_fallback(bad_file, fallback_value={"fallback": True})
        assert result == {"fallback": True}
    
    def test_multi_document_file_returns_fallback(self, tmp_path):
        """Test loading multi-document file returns fallback value."""
        multi_doc = tmp_path / "multi.yaml"
        multi_doc.write_text("a: 1\n---\nb: 2")
        
        result = safe_load_yaml_with_fallback(multi_doc)
        assert result == {}
    
    def test_with_logging_enabled(self, tmp_path, caplog):
        """Test that errors are logged when log_errors=True."""
        import logging
        
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("invalid: yaml: [")
        
        with caplog.at_level(logging.WARNING):
            result = safe_load_yaml_with_fallback(bad_file, log_errors=True)
        
        assert result == {}
        # Check that warning was logged
        assert any("Could not load" in record.message for record in caplog.records)
    
    def test_with_logging_disabled(self, tmp_path, caplog):
        """Test that errors are not logged when log_errors=False."""
        import logging
        
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("invalid: yaml: [")
        
        with caplog.at_level(logging.WARNING):
            result = safe_load_yaml_with_fallback(bad_file, log_errors=False)
        
        assert result == {}
        # Check that no warning was logged
        assert not any("Could not load" in record.message for record in caplog.records)
    
    def test_with_error_context(self, tmp_path, caplog):
        """Test that error_context is used in log messages."""
        import logging
        
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("invalid: yaml: [")
        
        with caplog.at_level(logging.WARNING):
            result = safe_load_yaml_with_fallback(
                bad_file, 
                error_context="test config file",
                log_errors=True
            )
        
        assert result == {}
        assert any("test config file" in record.message for record in caplog.records)


class TestLoadYAMLDocumentsAllLHPError:
    """Test LHPError re-raising in load_yaml_documents_all."""
    
    def test_lhp_error_reraise(self, tmp_path):
        """Test that LHPError is re-raised as-is without wrapping."""
        from unittest.mock import patch, mock_open
        
        # Create a mock LHPError
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Test LHP Error",
            details="This is a test LHP error"
        )
        
        # Mock yaml.safe_load_all to raise LHPError
        import yaml
        with patch.object(yaml, 'safe_load_all') as mock_yaml_load_all:
            mock_yaml_load_all.side_effect = lhp_error
            
            # Mock file open
            with patch('builtins.open', mock_open(read_data="test: data")):
                # Should re-raise LHPError without modification
                with pytest.raises(LHPError) as exc_info:
                    load_yaml_documents_all(Path("test.yaml"))
                
                # Verify it's the exact same error object
                assert exc_info.value is lhp_error
                assert exc_info.value.title == "Test LHP Error"
    
    def test_other_exceptions_wrapped_as_valueerror(self, tmp_path):
        """Test that non-LHPError exceptions are wrapped as ValueError."""
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("invalid: yaml: [")
        
        with pytest.raises(ValueError) as exc_info:
            load_yaml_documents_all(bad_file)
        
        assert "Invalid YAML" in str(exc_info.value)

