"""Tests for yaml_loader utility functions."""

import pytest
import tempfile
from pathlib import Path

from lhp.utils.yaml_loader import (
    load_yaml_file,
    load_yaml_documents_all,
)


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

