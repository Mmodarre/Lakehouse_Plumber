# Orchestrator.py Refactoring: Impact & Risk Analysis

## Executive Summary

**Current State:** 1,232 lines, 2 classes, ~31 methods
**Architecture:** Already uses service-based delegation (good foundation)
**Risk Level:** MEDIUM-HIGH due to extensive usage (20+ files depend on it)

---

## Current Issues Identified

### 2. Long Methods (Violate ~50 line guideline)

| Method | Lines | Complexity | Risk to Refactor |
|--------|-------|------------|------------------|
| `generate_pipeline_by_field()` | 146 | High | MEDIUM |
| `generate_pipeline()` | 145 | High | MEDIUM |
| `analyze_generation_requirements()` | 119 | Medium | LOW |
| `_discover_and_filter_flowgroups()` | 58 | Medium | LOW |
| `_enforce_version_requirements()` | 67 | Low | LOW |

### 3. Code Duplication

`generate_pipeline()` and `generate_pipeline_by_field()` share ~70% similar logic:
- Flowgroup processing loop
- Code formatting
- File writing
- State tracking
- Empty flowgroup handling

### 4. Redundant Passthrough Methods (8 methods)

These methods simply delegate to services without adding value:

```python
def discover_flowgroups(self, pipeline_dir):
    return self.discoverer.discover_flowgroups(pipeline_dir)  # Pure passthrough
```

| Method | Delegates To | Usage Count |
|--------|--------------|-------------|
| `discover_flowgroups()` | `discoverer` | 0 external |
| `discover_all_flowgroups()` | `discoverer` | 5+ external |
| `process_flowgroup()` | `processor` | internal only |
| `generate_flowgroup_code()` | `generator` | internal only |
| `determine_action_subtype()` | `generator` | 0 external |
| `build_custom_source_block()` | `generator` | 0 external |
| `group_write_actions_by_target()` | `generator` | 0 external |
| `create_combined_write_action()` | `generator` | 0 external |

### 5. Utility Methods That Should Be Extracted

- `_extract_single_source_view()` - Lines 1084-1115
- `_extract_source_views_from_action()` - Lines 1117-1155
- `_find_source_yaml()` / `_find_source_yaml_for_flowgroup()` - Similar logic

---

## Public API Analysis (Must Preserve)

### High-Usage Methods (Breaking these = HIGH RISK)

```python
# Used by CLI and many tests
ActionOrchestrator(project_root, pipeline_config_path=...)
orchestrator.discover_all_flowgroups()
orchestrator.generate_pipeline_by_field(pipeline_field, env, output_dir, ...)
orchestrator.discover_flowgroups_by_pipeline_field(pipeline_field)
orchestrator.validate_pipeline_by_field(pipeline_field, env)
orchestrator.analyze_generation_requirements(env, pipeline_names, include_tests, ...)
```

### Internal-Only Methods (Safe to refactor)

```python
# Used only within orchestrator
_discover_and_filter_flowgroups()
_apply_smart_generation_filtering()
_find_source_yaml()
_find_source_yaml_for_flowgroup()
_extract_single_source_view()
_extract_source_views_from_action()
_sync_bundle_resources()
```

---

## Risk Assessment Matrix

| Refactoring Action | Impact | Risk | Test Coverage | Recommendation |
|-------------------|--------|------|---------------|----------------|
| Fix `_discover_flowgroups` bug | Low | LOW | Likely covered | **DO NOW** |
| Extract utility methods | Low | LOW | Good | **DO** |
| Consolidate generate methods | Medium | MEDIUM | Extensive tests | **DO WITH CARE** |
| Remove passthrough methods | High | HIGH | Would break imports | **DON'T** |
| Split into multiple classes | High | HIGH | Major refactor | **DEFER** |

---

## Recommended Approach: Incremental Refactoring

---

## PHASE 1: Quick Wins (LOW RISK)

### Task 1.2: Extract Source View Utilities

**Create:** `src/lhp/utils/source_extractor.py`

Move these methods from orchestrator.py:
- `_extract_single_source_view()` (lines 1084-1115)
- `_extract_source_views_from_action()` (lines 1117-1155)

```python
# src/lhp/utils/source_extractor.py
"""Utilities for extracting source view information from action configurations."""

from typing import List, Union, Dict, Any


def extract_single_source_view(source: Union[str, List, Dict]) -> str:
    """Extract a single source view from various source formats.

    Args:
        source: Source configuration (string, list, or dict)

    Returns:
        Source view name as string
    """
    if isinstance(source, str):
        return source
    elif isinstance(source, list) and source:
        first_item = source[0]
        if isinstance(first_item, str):
            return first_item
        elif isinstance(first_item, dict):
            database = first_item.get("database")
            table = (
                first_item.get("table")
                or first_item.get("view")
                or first_item.get("name", "")
            )
            return f"{database}.{table}" if database and table else table
        else:
            return str(first_item)
    elif isinstance(source, dict):
        database = source.get("database")
        table = source.get("table") or source.get("view") or source.get("name", "")
        return f"{database}.{table}" if database and table else table
    else:
        return ""


def extract_source_views_from_action(source: Union[str, List, Dict]) -> List[str]:
    """Extract all source views from an action source configuration.

    Args:
        source: Source configuration (string, list, or dict)

    Returns:
        List of source view names
    """
    if isinstance(source, str):
        return [source]
    elif isinstance(source, list):
        result = []
        for item in source:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, dict):
                database = item.get("database")
                table = item.get("table") or item.get("view") or item.get("name", "")
                if database and table:
                    result.append(f"{database}.{table}")
                elif table:
                    result.append(table)
            else:
                result.append(str(item))
        return result
    elif isinstance(source, dict):
        database = source.get("database")
        table = source.get("table") or source.get("view") or source.get("name", "")
        if database and table:
            return [f"{database}.{table}"]
        elif table:
            return [table]
        else:
            return []
    else:
        return []
```

**Update orchestrator.py:**

```python
# Add import at top
from ..utils.source_extractor import extract_single_source_view, extract_source_views_from_action

# Replace methods with delegations (for backward compatibility)
def _extract_single_source_view(self, source) -> str:
    return extract_single_source_view(source)

def _extract_source_views_from_action(self, source) -> List[str]:
    return extract_source_views_from_action(source)
```

**Verification:** Run `pytest tests/ -k "source" -v`

---

### Task 1.3: Move YAML Finder Methods to Discoverer Service

**File:** `src/lhp/core/services/flowgroup_discoverer.py`

Move these methods from orchestrator.py to FlowgroupDiscoverer:
- `_find_source_yaml()` (lines 804-830)
- `_find_source_yaml_for_flowgroup()` (lines 832-861)

**Add to FlowgroupDiscoverer class:**

```python
def find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
    """Find the source YAML file for a given flowgroup.

    Supports multi-document (---) and flowgroups array syntax.

    Args:
        flowgroup: The flowgroup to find the source YAML for

    Returns:
        Path to the source YAML file, or None if not found
    """
    pipelines_dir = self.project_root / "pipelines"

    if not pipelines_dir.exists():
        return None

    for extension in ["*.yaml", "*.yml"]:
        for yaml_file in pipelines_dir.rglob(extension):
            try:
                flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
                for parsed_flowgroup in flowgroups:
                    if (parsed_flowgroup.pipeline == flowgroup.pipeline and
                        parsed_flowgroup.flowgroup == flowgroup.flowgroup):
                        return yaml_file
            except Exception as e:
                self.logger.debug(f"Could not parse flowgroup {yaml_file}: {e}")

    return None
```

**Update orchestrator.py:**

```python
def _find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
    """Find the source YAML file for a given flowgroup."""
    return self.discoverer.find_source_yaml_for_flowgroup(flowgroup)
```

**Verification:** Run `pytest tests/test_orchestrator.py -v`

---

### Task 1.4: Update Exports

**File:** `src/lhp/utils/__init__.py`

Add new module to exports if needed.

---

### Phase 1 Checklist

- [ ] Fix bug on line 1174
- [ ] Create `src/lhp/utils/source_extractor.py`
- [ ] Add `find_source_yaml_for_flowgroup()` to FlowgroupDiscoverer
- [ ] Update orchestrator imports and delegations
- [ ] Run full test suite: `pytest tests/ -v`
- [ ] Verify no regressions

---

## PHASE 2: Reduce Method Length (MEDIUM RISK)

**Effort: 4-6 hours | Risk: MEDIUM**

### Task 2.1: Create PipelineGenerationExecutor Service

**Create:** `src/lhp/core/services/pipeline_generation_executor.py`

This service handles the common generation logic shared between `generate_pipeline()` and `generate_pipeline_by_field()`.

```python
"""Pipeline generation execution service."""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from ...models.config import FlowGroup, ActionType
from ...utils.formatter import format_code
from ...utils.error_formatter import LHPError


@dataclass
class GenerationContext:
    """Context for pipeline generation execution."""
    env: str
    pipeline_identifier: str
    output_dir: Optional[Path]
    state_manager: Any  # StateManager or None
    include_tests: bool
    substitution_mgr: Any  # EnhancedSubstitutionManager


@dataclass
class GenerationResult:
    """Result of generation execution."""
    generated_files: Dict[str, str]
    files_written: int
    files_skipped: int


class PipelineGenerationExecutor:
    """
    Executes pipeline generation for a list of flowgroups.

    Centralizes the common logic between generate_pipeline() and
    generate_pipeline_by_field() to reduce code duplication.
    """

    def __init__(self, processor, generator, config_validator,
                 yaml_parser, discoverer, dependencies, logger=None):
        """
        Initialize the executor with required dependencies.

        Args:
            processor: FlowgroupProcessor for processing flowgroups
            generator: CodeGenerator for generating code
            config_validator: ConfigValidator for validation
            yaml_parser: YAMLParser for parsing flowgroup files
            discoverer: FlowgroupDiscoverer for finding source YAMLs
            dependencies: OrchestrationDependencies factory
            logger: Optional logger instance
        """
        self.processor = processor
        self.generator = generator
        self.config_validator = config_validator
        self.yaml_parser = yaml_parser
        self.discoverer = discoverer
        self.dependencies = dependencies
        self.logger = logger or logging.getLogger(__name__)

    def execute(self, flowgroups: List[FlowGroup],
                context: GenerationContext) -> GenerationResult:
        """
        Execute generation for a list of flowgroups.

        Args:
            flowgroups: List of flowgroups to generate
            context: Generation context with environment and settings

        Returns:
            GenerationResult with generated files and statistics
        """
        # 1. Process all flowgroups first
        processed_flowgroups = self._process_flowgroups(flowgroups, context)

        # 2. Validate table creation rules
        self._validate_table_creation(processed_flowgroups)

        # 3. Generate code for each flowgroup
        generated_files = self._generate_code(processed_flowgroups, context)

        # 4. Save state
        if context.state_manager:
            context.state_manager.save()

        # 5. Get statistics
        smart_writer = self.dependencies.create_file_writer()
        files_written, files_skipped = smart_writer.get_stats()

        return GenerationResult(
            generated_files=generated_files,
            files_written=files_written,
            files_skipped=files_skipped
        )

    def _process_flowgroups(self, flowgroups: List[FlowGroup],
                           context: GenerationContext) -> List[FlowGroup]:
        """Process all flowgroups: expand templates, apply presets, substitutions."""
        processed = []
        for flowgroup in flowgroups:
            self.logger.info(f"Processing flowgroup: {flowgroup.flowgroup}")
            try:
                processed_fg = self.processor.process_flowgroup(
                    flowgroup, context.substitution_mgr
                )
                processed.append(processed_fg)
            except Exception as e:
                self.logger.debug(f"Error processing flowgroup {flowgroup.flowgroup}: {e}")
                raise
        return processed

    def _validate_table_creation(self, processed_flowgroups: List[FlowGroup]) -> None:
        """Validate table creation rules across all flowgroups."""
        try:
            errors = self.config_validator.validate_table_creation_rules(processed_flowgroups)
            if errors:
                raise ValueError(
                    "Table creation validation failed:\n" +
                    "\n".join(f"  - {e}" for e in errors)
                )
        except Exception as e:
            raise ValueError(f"Table creation validation failed:\n  - {str(e)}")

    def _generate_code(self, processed_flowgroups: List[FlowGroup],
                      context: GenerationContext) -> Dict[str, str]:
        """Generate code for all processed flowgroups."""
        generated_files = {}
        smart_writer = self.dependencies.create_file_writer()

        for flowgroup in processed_flowgroups:
            self.logger.info(f"Generating code for flowgroup: {flowgroup.flowgroup}")

            try:
                result = self._generate_single_flowgroup(
                    flowgroup, context, smart_writer
                )
                if result:
                    filename, code = result
                    generated_files[filename] = code
            except Exception as e:
                self.logger.debug(f"Error generating code for flowgroup {flowgroup.flowgroup}: {e}")
                raise

        return generated_files

    def _generate_single_flowgroup(self, flowgroup: FlowGroup,
                                   context: GenerationContext,
                                   smart_writer) -> Optional[tuple]:
        """Generate code for a single flowgroup. Returns (filename, code) or None if empty."""
        # Find source YAML
        source_yaml = self.discoverer.find_source_yaml_for_flowgroup(flowgroup)

        # Generate code
        code = self.generator.generate_flowgroup_code(
            flowgroup, context.substitution_mgr, context.output_dir,
            context.state_manager, source_yaml, context.env, context.include_tests
        )

        # Format code
        formatted_code = format_code(code)

        # Check if empty
        filename = f"{flowgroup.flowgroup}.py"
        if not formatted_code.strip():
            self._handle_empty_flowgroup(flowgroup, context)
            return None

        # Write file if output directory specified
        if context.output_dir:
            output_file = context.output_dir / filename
            smart_writer.write_if_changed(output_file, formatted_code)

            # Track in state manager
            if context.state_manager and source_yaml:
                self._track_generated_file(
                    flowgroup, output_file, source_yaml, context
                )

            self.logger.info(f"Generated: {output_file}")
        else:
            self.logger.info(f"Would generate: {filename}")

        return (filename, formatted_code)

    def _handle_empty_flowgroup(self, flowgroup: FlowGroup,
                                context: GenerationContext) -> None:
        """Handle empty flowgroup - cleanup existing files if needed."""
        if context.output_dir:
            output_file = context.output_dir / f"{flowgroup.flowgroup}.py"
            if output_file.exists():
                try:
                    output_file.unlink()
                    self.logger.info(f"Deleted empty flowgroup file: {output_file}")

                    if context.state_manager:
                        context.state_manager.remove_generated_file(output_file, context.env)
                        context.state_manager.cleanup_empty_directories(
                            context.env, [str(output_file)]
                        )
                except Exception as e:
                    self.logger.error(f"Failed to delete empty flowgroup file: {e}")
                    raise

        self.logger.info(f"Skipping empty flowgroup: {flowgroup.flowgroup}")

    def _track_generated_file(self, flowgroup: FlowGroup, output_file: Path,
                             source_yaml: Path, context: GenerationContext) -> None:
        """Track generated file in state manager."""
        has_test_actions = any(
            action.type == ActionType.TEST for action in flowgroup.actions
        )
        generation_context = f"include_tests:{context.include_tests}" if has_test_actions else ""

        context.state_manager.track_generated_file(
            generated_path=output_file,
            source_yaml=source_yaml,
            environment=context.env,
            pipeline=context.pipeline_identifier,
            flowgroup=flowgroup.flowgroup,
            generation_context=generation_context,
        )
```

---

### Task 2.2: Update Orchestrator to Use Executor

**File:** `src/lhp/core/orchestrator.py`

**Add initialization in `__init__`:**

```python
from .services.pipeline_generation_executor import PipelineGenerationExecutor, GenerationContext

# In __init__, after other service initializations:
self.generation_executor = PipelineGenerationExecutor(
    processor=self.processor,
    generator=self.generator,
    config_validator=self.config_validator,
    yaml_parser=self.yaml_parser,
    discoverer=self.discoverer,
    dependencies=self.dependencies,
    logger=self.logger
)
```

**Simplify `generate_pipeline_by_field()`:**

```python
def generate_pipeline_by_field(
    self,
    pipeline_field: str,
    env: str,
    output_dir: Path = None,
    state_manager=None,
    force_all: bool = False,
    specific_flowgroups: List[str] = None,
    include_tests: bool = False,
) -> Dict[str, str]:
    """Generate complete pipeline from YAML configs using pipeline field."""
    self.logger.info(f"Starting pipeline generation by field: {pipeline_field} for env: {env}")

    # Validate no duplicate pipeline+flowgroup combinations
    all_flowgroups = self.discover_all_flowgroups()
    self.validate_duplicate_pipeline_flowgroup_combinations(all_flowgroups)

    # Discover and filter flowgroups
    flowgroups = self._discover_and_filter_flowgroups(
        env=env,
        pipeline_identifier=pipeline_field,
        include_tests=include_tests,
        force_all=force_all,
        specific_flowgroups=specific_flowgroups,
        state_manager=state_manager,
        use_directory_discovery=False
    )

    # Set up output directory
    if output_dir:
        pipeline_output_dir = output_dir / pipeline_field
        pipeline_output_dir.mkdir(parents=True, exist_ok=True)
    else:
        pipeline_output_dir = None

    # Initialize substitution manager
    substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
    substitution_mgr = self.dependencies.create_substitution_manager(substitution_file, env)

    # Create context and execute
    context = GenerationContext(
        env=env,
        pipeline_identifier=pipeline_field,
        output_dir=pipeline_output_dir,
        state_manager=state_manager,
        include_tests=include_tests,
        substitution_mgr=substitution_mgr
    )

    result = self.generation_executor.execute(flowgroups, context)

    self.logger.info(f"Pipeline generation complete: {pipeline_field}")
    return result.generated_files
```

**Similarly simplify `generate_pipeline()`** - same pattern but with `use_directory_discovery=True`

---

### Task 2.3: Update Service Exports

**File:** `src/lhp/core/services/__init__.py`

```python
from .pipeline_generation_executor import PipelineGenerationExecutor, GenerationContext, GenerationResult
```

---

### Phase 2 Checklist

- [ ] Create `src/lhp/core/services/pipeline_generation_executor.py`
- [ ] Add `GenerationContext` and `GenerationResult` dataclasses
- [ ] Implement `PipelineGenerationExecutor` class
- [ ] Update orchestrator `__init__` to initialize executor
- [ ] Simplify `generate_pipeline_by_field()` (~146 lines → ~40 lines)
- [ ] Simplify `generate_pipeline()` (~145 lines → ~40 lines)
- [ ] Update service exports
- [ ] Run full test suite: `pytest tests/ -v`
- [ ] Verify no regressions in:
  - `tests/test_orchestrator.py`
  - `tests/test_integration.py`
  - `tests/test_integration_e2e.py`
  - `tests/test_pipeline_field_functionality.py`

---

## Expected Results After Phase 1 & 2

| Metric | Before | After |
|--------|--------|-------|
| orchestrator.py lines | 1,232 | ~950 |
| Longest method | 146 lines | ~60 lines |
| Code duplication | High | Eliminated |
| Bugs | 1 | 0 |
| New services | 0 | 1 (PipelineGenerationExecutor) |
| New utilities | 0 | 1 (source_extractor.py) |

---

## Phase 3: Clean Up Passthrough (HIGH RISK - Optional)

**Effort: 8-12 hours**

This would require updating 20+ files. Only do if:
- You're doing a major version bump
- You have comprehensive test coverage
- You want to expose services directly

---

## What NOT To Do

1. **DON'T remove public methods** - Too many dependents
2. **DON'T change method signatures** - Would break tests/CLI
3. **DON'T split the class yet** - Current facade pattern is working
4. **DON'T refactor without running full test suite** - 20+ test files depend on this

---

## Files That Would Need Updates (If Breaking Changes)

If passthrough methods are removed:
- `src/lhp/cli/commands/generate_command.py`
- `src/lhp/services/state_display_service.py`
- `tests/test_orchestrator.py`
- `tests/test_integration.py`
- `tests/test_integration_e2e.py`
- `tests/test_append_flow.py`
- `tests/test_staleness_analysis_integration.py`
- `tests/test_pipeline_field_functionality.py`
- `tests/test_pipeline_field_output_structure.py`
- ... and 10+ more test files

---

## Conclusion

The orchestrator is **already well-architected** with service delegation. The main issues are:

1. **One bug** that should be fixed immediately
2. **Long methods** that can be shortened by extracting a generation executor
3. **Utility methods** that should be moved to appropriate modules

**Recommended effort:** 6-8 hours for Phase 1 + Phase 2
**Risk if done carefully:** LOW to MEDIUM
**Value:** Improved maintainability, reduced method length, cleaner separation

The passthrough methods (Phase 3) are actually a feature, not a bug - they provide a stable public API while allowing internal refactoring. I recommend keeping them.
