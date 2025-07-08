# DLT Python to YAML Reverse Engineering Research Report

## Executive Summary

This report investigates options for adding reverse engineering capabilities to **Lakehouse Plumber**, enabling the conversion of Databricks Delta Live Tables (DLT) Python code back to YAML configurations. This would complement the existing YAML-to-Python generation functionality.

## Current Application Analysis

### Lakehouse Plumber Overview
- **Purpose**: Action-based Lakeflow Declarative Pipelines code generator for Databricks
- **Current Direction**: YAML → Python (DLT) generation
- **Architecture**: Template-based code generation using Jinja2 templates
- **Core Components**:
  - Load Actions (CloudFiles, Delta, SQL, JDBC, Python)
  - Transform Actions (SQL, Python, Data Quality, Schema)
  - Write Actions (Streaming Table, Materialized View)

### Current Code Generation Architecture
```
YAML Config → Parsers → Models → Generators → Python DLT Code
```

The application uses:
- **Jinja2 templates** for code generation (in `src/lhp/generators/`)
- **Structured models** for configuration (in `src/lhp/models/`)
- **Specialized generators** for each action type (load, transform, write)
- **AST parsing** for complex operations (like source function extraction)

## Reverse Engineering Options

### Option 1: AST-Based Python Parser (Recommended)

**Approach**: Use Python's Abstract Syntax Tree (AST) module to parse DLT Python code and extract pipeline structure.

**Implementation Strategy**:
```python
import ast
from typing import Dict, List, Any

class DLTReverseEngineer:
    def __init__(self):
        self.pipelines = []
        self.actions = []
    
    def parse_dlt_file(self, python_file: str) -> Dict:
        with open(python_file, 'r') as f:
            source = f.read()
        
        tree = ast.parse(source)
        return self._extract_pipeline_config(tree)
    
    def _extract_pipeline_config(self, tree: ast.Module) -> Dict:
        # Extract @dlt decorators and function definitions
        # Map back to YAML structure
        pass
```

**Advantages**:
- **Accurate parsing**: AST provides precise code structure analysis
- **Handles complexity**: Can parse nested decorators, function calls, and complex expressions
- **Robust**: Less prone to breaking with code formatting variations
- **Existing expertise**: LHP already uses AST for source function extraction

**Implementation Details**:
1. **Decorator Analysis**: Parse `@dlt.table()`, `@dlt.view()`, `@dlt.expect()` decorators
2. **Function Mapping**: Map Python functions back to YAML actions
3. **Parameter Extraction**: Extract table properties, expectations, source configurations
4. **Template Reverse Mapping**: Use knowledge of generation templates to reverse-map structures

### Option 2: Pattern-Based String Parsing

**Approach**: Use regular expressions and string parsing to extract DLT patterns.

**Implementation Strategy**:
```python
import re
from typing import Dict, List

class DLTPatternParser:
    def __init__(self):
        self.dlt_patterns = {
            'table_decorator': r'@dlt\.(?:table|create_table|create_streaming_table)\((.*?)\)',
            'append_flow': r'@dlt\.append_flow\((.*?)\)',
            'expectations': r'@dlt\.expect(?:_or_fail|_or_drop|_all)?\((.*?)\)'
        }
    
    def parse_dlt_patterns(self, content: str) -> Dict:
        # Extract patterns and build YAML structure
        pass
```

**Advantages**:
- **Simpler implementation**: Easier to understand and maintain
- **Quick development**: Faster to implement initial version
- **Pattern-specific**: Can target specific DLT patterns

**Disadvantages**:
- **Fragile**: Breaks with code formatting changes
- **Limited scope**: May miss complex nested structures
- **Maintenance overhead**: Requires updates when DLT API changes

### Option 3: Hybrid AST + Template Matching

**Approach**: Combine AST parsing with knowledge of existing Jinja2 templates to improve accuracy.

**Implementation Strategy**:
```python
class HybridDLTReverseEngineer:
    def __init__(self):
        self.template_patterns = self._load_template_patterns()
        self.ast_parser = DLTASTParser()
    
    def reverse_engineer(self, python_file: str) -> Dict:
        # Parse with AST
        ast_result = self.ast_parser.parse(python_file)
        
        # Match against known templates
        template_matches = self._match_templates(ast_result)
        
        # Generate YAML configuration
        return self._generate_yaml_config(ast_result, template_matches)
    
    def _load_template_patterns(self):
        # Load and analyze existing Jinja2 templates
        # Extract common patterns and structures
        pass
```

**Advantages**:
- **High accuracy**: Leverages existing template knowledge
- **Robust parsing**: AST provides structural analysis
- **Template consistency**: Ensures reverse-engineered YAML matches original patterns

## Technical Implementation Plan

### Phase 1: Foundation (2-3 weeks)
1. **Create reverse engineering module**:
   ```
   src/lhp/reverse_engineer/
   ├── __init__.py
   ├── ast_parser.py          # Core AST parsing logic
   ├── yaml_generator.py      # YAML configuration generation
   ├── template_matcher.py    # Template pattern matching
   └── validators.py          # Validation and consistency checks
   ```

2. **Implement core AST parsing**:
   - Function definition extraction
   - Decorator analysis (@dlt.table, @dlt.view, etc.)
   - Import statement handling
   - Source/target relationship mapping

### Phase 2: Action Type Support (3-4 weeks)
1. **Load Action Reverse Engineering**:
   - CloudFiles pattern detection
   - Delta table source extraction
   - SQL query analysis
   - JDBC connection parsing

2. **Transform Action Reverse Engineering**:
   - SQL transformation extraction
   - Python function analysis
   - Data quality expectation parsing
   - Schema transformation detection

3. **Write Action Reverse Engineering**:
   - Streaming table configuration
   - Materialized view detection
   - Append flow analysis
   - CDC configuration extraction

### Phase 3: Advanced Features (2-3 weeks)
1. **Template Pattern Matching**:
   - Analyze existing Jinja2 templates
   - Create reverse mapping rules
   - Handle template variations

2. **YAML Structure Generation**:
   - Pipeline configuration recreation
   - Flowgroup organization
   - Preset and template inference

### Phase 4: CLI Integration (1-2 weeks)
1. **Add CLI command**:
   ```bash
   lhp reverse <python_file> --output <yaml_file>
   lhp reverse <directory> --output-dir <yaml_dir>
   ```

2. **Validation and round-trip testing**:
   - Ensure YAML → Python → YAML consistency
   - Validate generated configurations

## Code Examples

### AST-Based DLT Function Analysis
```python
def extract_dlt_function(self, func_node: ast.FunctionDef) -> Dict:
    """Extract DLT configuration from function definition."""
    action_config = {
        'name': func_node.name,
        'type': None,
        'source': None,
        'target': None
    }
    
    # Analyze decorators
    for decorator in func_node.decorator_list:
        if isinstance(decorator, ast.Call):
            if self._is_dlt_decorator(decorator):
                action_config.update(self._parse_dlt_decorator(decorator))
    
    # Analyze function body for source patterns
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            if self._is_dlt_read_call(node):
                action_config['source'] = self._extract_source_config(node)
    
    return action_config
```

### YAML Generation Example
```python
def generate_yaml_config(self, actions: List[Dict]) -> Dict:
    """Generate YAML configuration from extracted actions."""
    config = {
        'pipeline': self._infer_pipeline_name(actions),
        'flowgroup': self._infer_flowgroup_name(actions),
        'actions': []
    }
    
    for action in actions:
        yaml_action = self._convert_action_to_yaml(action)
        config['actions'].append(yaml_action)
    
    return config
```

## Challenges and Mitigation Strategies

### Challenge 1: Complex Python Logic
**Problem**: DLT Python code may contain complex business logic that doesn't map directly to YAML.
**Mitigation**: 
- Focus on standard DLT patterns first
- Provide warnings for unsupported patterns
- Allow manual intervention for complex cases

### Challenge 2: Template Variations
**Problem**: Generated code may vary based on different template versions or customizations.
**Mitigation**:
- Version template patterns
- Provide fuzzy matching capabilities
- Allow configuration of parsing rules

### Challenge 3: Incomplete Information
**Problem**: Some information may be lost in the Python generation process.
**Mitigation**:
- Include metadata comments in generated Python code
- Provide best-effort reconstruction with warnings
- Allow manual YAML editing for missing information

## Integration Points

### Existing Codebase Integration
1. **Leverage existing models**: Use `src/lhp/models/config.py` Action classes
2. **Reuse validation logic**: Apply existing validation rules to reverse-engineered YAML
3. **Template knowledge**: Analyze existing generators to understand patterns
4. **CLI framework**: Extend existing CLI with new commands

### Future Enhancements
1. **Round-trip validation**: Ensure YAML → Python → YAML consistency
2. **Partial reverse engineering**: Support incremental updates
3. **Code annotation**: Add metadata to Python code for improved reverse engineering
4. **Visual diff tools**: Show differences between original and reverse-engineered YAML

## Estimated Development Timeline

- **Total Duration**: 8-12 weeks
- **Phase 1** (Foundation): 2-3 weeks
- **Phase 2** (Action Support): 3-4 weeks  
- **Phase 3** (Advanced Features): 2-3 weeks
- **Phase 4** (CLI Integration): 1-2 weeks

## Conclusion

The **AST-based approach with template matching** (Option 3) provides the best balance of accuracy, maintainability, and feature completeness for implementing DLT Python to YAML reverse engineering in Lakehouse Plumber. This approach leverages the existing codebase architecture while providing robust parsing capabilities for the complex DLT patterns generated by the application.

The implementation would add significant value by enabling:
- **Migration assistance**: Help users migrate from manual DLT code to LHP configurations
- **Code analysis**: Enable inspection and modification of existing DLT pipelines
- **Round-trip validation**: Ensure consistency between YAML and generated Python
- **Developer productivity**: Reduce manual effort in configuration management

This capability would position Lakehouse Plumber as a comprehensive DLT development and management platform, supporting both forward and reverse engineering workflows.