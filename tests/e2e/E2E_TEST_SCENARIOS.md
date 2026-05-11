# E2E Test Scenarios

This document catalogs every scenario covered by the LHP end-to-end test suite. **114 tests across 10 files**, all running against the shared fixture project at `tests/e2e/fixtures/testing_project/`.

Companion documents:
- **[E2E_TEST_ARCHITECTURE.md](E2E_TEST_ARCHITECTURE.md)** — How the suite works (fixture, isolation, hash comparison, state tracking)
- **`.claude/skills/lhp/e2e-test/`** — Skill for writing, maintaining, and running these tests

## Overview

| Test file | # Tests | Scope |
|---|---:|---|
| `test_bundle_manager_e2e.py` | 25 | Bundle resource lifecycle (BM-1..BM-15), Python function dependency tracking, cross-platform state |
| `test_pipeline_config_e2e.py` | 35 | Pipeline config rendering: clusters, environments, configurations, event_log, monitoring |
| `test_multi_job_generation_e2e.py` | 16 | Multi-job CLI integration, master job, validation, format selection |
| `test_test_reporting_spec_e2e.py` | 12 | Test-reporting spec coverage (TC-07 through TC-25) |
| `test_test_reporting_e2e.py` | 7 | `--include-tests` flag: hook generation, test flowgroup, provider copy |
| `test_job_orchestration_e2e.py` | 6 | `lhp deps` orchestration jobs (single, multi-job, file-arrival trigger) |
| `test_quarantine_e2e.py` | 5 | Quarantine flow generation and validation |
| `test_deps_extraction.py` | 4 | Dependency extraction from `write_target` SQL/Python paths |
| `test_local_variables_e2e.py` | 3 | `%{local_var}` resolution and undefined-variable errors |
| `test_test_reporting_cleanup_e2e.py` | 1 | Cleanup when `--include-tests` is removed across runs |

---

## `test_bundle_manager_e2e.py` (25 tests)

### Bundle Manager BM-1..BM-7 — Resource file lifecycle

| Test | Scenario |
|---|---|
| `test_BM1_preserve_existing_lhp_managed_file` | LHP-managed file unchanged → preserved on regenerate (mtime unchanged) |
| `test_BM1_1_regenerate_existing_lhp_managed_file_with_pipeline_config` | LHP-managed file → force regeneration when pipeline config is added |
| `test_BM2_backup_and_replace_user_managed_file` | User-managed file (no LHP header) → backup created (`.bkup`), replaced with LHP version |
| `test_BM3_create_new_resource_file_when_missing` | No resource file → new LHP file created |
| `test_BM4_delete_orphaned_resource_file` | Resource file exists but no Python output → orphan deleted |
| `test_BM5_error_on_multiple_resource_files` | Multiple resource files for same pipeline → error issued |
| `test_BM6_header_based_lhp_detection` | Mixed headers → correct LHP vs user-managed detection |
| `test_BM7_output_directory_missing` | `generated/` directory missing → auto-created |

### Bundle Manager BM-8..BM-15 — Python function dependency tracking

| Test | Scenario |
|---|---|
| `test_BM8a_embedded_python_function_regeneration` | Embedded function modified → exactly 1 dependent file regenerated |
| `test_BM8b_copied_python_function_regeneration` | Copied function modified → exactly 2 dependent files regenerated (importer + copy) |
| `test_BM9_diagnostic_dependency_resolution` | Diagnostic of dependency resolution differences between embedded/copied patterns |
| `test_BM10_staleness_analysis_diagnostic` | Deep diagnostic of staleness analysis for Python function changes |
| `test_BM11_embedded_pattern_staleness_diagnostic` | Diagnostic for embedded pattern staleness analysis |
| `test_BM12_pipeline_decision_logic_comparison` | Pipeline decision logic comparison for both function patterns |
| `test_BM13_cli_vs_orchestrator_analysis_comparison` | CLI command behavior vs direct orchestrator analysis |
| `test_BM14_python_function_overwrite_behavior` | Same source-file changes overwrite existing copied file correctly (no accumulation) |
| `test_BM15_python_function_conflict_failure` | Different source files with same name → `PythonFunctionConflictError` |

### Bundle Manager — cross-cutting

| Test | Scenario |
|---|---|
| `test_pipeline_directory_resource_file_correspondence` | Directories under `generated/{env}/` correspond 1-to-1 with `.pipeline.yml` files |
| `test_baseline_hash_comparison_dev_environment` | Full `lhp generate -e dev` output matches `generated_baseline/dev/` byte-for-byte |
| `test_include_directive_filtering` | `include:` directive filters pipelines correctly and matches baseline |
| `test_multi_environment_consistency` | Same files exist across dev/tst/prod environments with non-empty content |
| `test_flowgroup_deletion_triggers_cleanup` | Deleting flowgroup YAML cleans up generated files |
| `test_pipeline_directory_deletion_cleanup` | Deleting pipeline directory cleans up entire generated pipeline output |
| `test_databricks_yml_variables_synchronization` | `generate -e dev` adds environment-specific variables to dev/tst targets in `databricks.yml` |
| `test_cross_platform_state_file_compatibility` | State files generated on one OS can be loaded on another (path normalization) |

---

## `test_pipeline_config_e2e.py` (35 tests)

### Cluster configuration (5 tests)

| Test | Scenario |
|---|---|
| `test_generate_with_pipeline_config_non_serverless` | Non-serverless cluster configuration produces expected resource YAML |
| `test_generate_different_pipelines_different_clusters` | Multiple pipelines with different cluster configs render distinct outputs |
| `test_comprehensive_cluster_config_matches_baseline` | All cluster options (autoscale, policy, node_type) match baseline |
| `test_generate_with_instance_pool_cluster` | Instance pool config (no `node_type_id`) generates correctly |
| `test_generate_with_mixed_pool_and_node_type` | One pipeline with instance pool + another with `node_type_id` coexist |

### Environment block (5 tests)

| Test | Scenario |
|---|---|
| `test_environment_dependencies_rendered_in_resource` | `environment.dependencies` flows into resource YAML, matches baseline |
| `test_environment_with_substitution_tokens` | `${token}` references in environment resolve correctly |
| `test_environment_absent_when_not_configured` | Environment commented out → no environment block in output |
| `test_environment_with_project_defaults_inheritance` | `project_defaults.environment` inherited by pipelines without explicit override |
| `test_existing_baselines_match_after_template_change` | Full CLI generation produces resource files matching updated baselines |

### Configuration block (5 tests)

| Test | Scenario |
|---|---|
| `test_configuration_entries_rendered_in_resource` | `configuration:` entries flow into resource YAML, match baseline |
| `test_configuration_with_substitution_tokens` | Tokens inside configuration resolve correctly |
| `test_configuration_absent_preserves_default` | No `configuration:` → only `bundle.sourcePath` present |
| `test_configuration_with_project_defaults_inheritance` | `project_defaults.configuration` inherited by pipelines |
| `test_existing_baselines_match_after_configuration_change` | Full CLI generation produces resource files matching baselines |

### Event log (5 tests)

| Test | Scenario |
|---|---|
| `test_event_log_injected_from_project_config` | `lhp.yaml` `event_log` block injected into output with resolved tokens |
| `test_event_log_absent_when_not_configured` | No `event_log` in `lhp.yaml` → no event_log in output |
| `test_pipeline_config_overrides_project_event_log` | Pipeline-level `event_log` wins over `lhp.yaml` event_log |
| `test_pipeline_config_disables_event_log` | `event_log: false` in pipeline_config → output has no event_log |
| `test_backward_compat_pipeline_config_only_event_log` | Existing pipeline_config event_log still works without `lhp.yaml` event_log |

### Monitoring (15 tests)

| Test | Scenario |
|---|---|
| `test_monitoring_absent_no_monitoring_pipeline` | No `monitoring:` in `lhp.yaml` → no monitoring pipeline generated |
| `test_monitoring_enabled_generates_pipeline` | `monitoring: {}` + event_log → monitoring pipeline matches baseline |
| `test_monitoring_resource_no_event_log` | Monitoring pipeline resource YAML has no event_log block |
| `test_monitoring_custom_pipeline_name` | Custom `pipeline_name` changes directory, PIPELINE_ID, resource filename |
| `test_monitoring_catalog_schema_override` | Custom catalog/schema changes ST + MV FQNs; source refs unchanged |
| `test_monitoring_custom_streaming_table` | Custom `streaming_table` changes ST name, append_flow target, MV FROM clause |
| `test_monitoring_custom_mvs` | Custom `materialized_views` replaces default `events_summary` with user-defined MVs |
| `test_monitoring_no_mvs` | Empty `materialized_views: []` → notebook-only, no DLT artifact |
| `test_monitoring_mv_sql_path` | MV with `sql_path` loads SQL from external file at generation time |
| `test_monitoring_enable_job_monitoring` | `enable_job_monitoring: true` generates Python load action + auxiliary loader file |
| `test_monitoring_custom_job_config` | Rich `monitoring_job_config.yaml` loaded, token-substituted, applied |
| `test_monitoring_job_config_file_missing_fails` | `monitoring.job_config_path` pointing to non-existent file → error |
| `test_monitoring_pipeline_config_settings` | Pipeline config document for monitoring pipeline flows into resource YAML |
| `test_monitoring_pipeline_inherits_project_defaults` | Monitoring pipeline inherits project_defaults when no specific doc exists |
| `test_monitoring_alias_resolves_in_generated_bundle` | `pipeline: __eventlog_monitoring` alias resolves to actual monitoring pipeline name |

---

## `test_multi_job_generation_e2e.py` (16 tests)

### End-to-end workflow (5 tests)

| Test | Scenario |
|---|---|
| `test_complete_multi_job_workflow` | Flowgroups with `job_name` → multiple job files generated correctly |
| `test_single_job_backward_compatibility` | Existing projects without `job_name` still work (default single job) |
| `test_validation_prevents_mixed_usage` | Validation prevents mixed `job_name` usage across flowgroups |
| `test_cli_output_shows_multiple_jobs` | CLI output displays each generated job file |
| `test_pipeline_filter_blocked_with_job_name` | `--pipeline` flag is blocked when `job_name` is used |

### Bundle output and config (3 tests)

| Test | Scenario |
|---|---|
| `test_bundle_output_directory_structure` | `--bundle-output` creates correct directory structure |
| `test_project_defaults_applied_to_all_jobs` | `project_defaults` apply to every generated job |
| `test_job_specific_overrides_work` | Per-job configs override `project_defaults` |

### Tags and master job (4 tests)

| Test | Scenario |
|---|---|
| `test_tags_deep_merge` | Tags from defaults + per-job are deep-merged |
| `test_master_job_contains_all_jobs` | Master job references every individual job |
| `test_master_job_respects_dependencies` | Master job creates correct task dependencies |
| `test_master_job_naming` | Master job naming convention applied |

### Validation and format (4 tests)

| Test | Scenario |
|---|---|
| `test_invalid_job_name_format_caught` | Invalid `job_name` format raises validation error |
| `test_malformed_job_config_caught` | Malformed `job_config.yaml` raises validation error |
| `test_job_format_only` | Generating only job format with multi-job |
| `test_all_formats_with_multi_job` | Generating all formats (pipeline + job) with multi-job |

---

## `test_test_reporting_spec_e2e.py` (12 tests)

Test-reporting spec coverage by TC-id.

| Test | Scenario |
|---|---|
| `test_tc07_test_id_recognized_by_validator` | TC-07: `test_id` field does not trigger 'Unknown field' validation error |
| `test_tc17_no_hook_without_include_tests_flag` | TC-17: Without `--include-tests`, no hook file generated |
| `test_tc18_no_hook_without_test_reporting_config` | TC-18: Even with `--include-tests`, no hook when `test_reporting` config absent |
| `test_tc19_hook_tracked_in_state_file` | TC-19: After `--include-tests` generation, hook file appears in state file |
| `test_tc20_validate_with_include_tests_succeeds` | TC-20: `lhp validate --env dev --include-tests` succeeds with valid config |
| `test_tc21_validate_without_flag_checks_file_existence` | TC-21: `validate` without `--include-tests` still checks `module_path` exists |
| `test_tc21b_validate_fails_when_provider_missing` | TC-21b: `validate` fails when `module_path` file doesn't exist |
| `test_tc22_hook_matches_baseline` | TC-22: Generated hook file matches baseline via hash comparison |
| `test_tc22b_test_flowgroup_matches_baseline` | TC-22b: Generated test flowgroup matches baseline via hash comparison |
| `test_tc22c_provider_copy_matches_baseline` | TC-22c: Copied provider module matches baseline via hash comparison |
| `test_tc23_existing_baselines_unaffected` | TC-23: Standard baselines identical whether or not `test_reporting` is in config |
| `test_tc25_hook_in_state_proves_ordering` | TC-25: Hook file in state proves it ran before state save (ordering invariant) |

---

## `test_test_reporting_e2e.py` (7 tests)

| Test | Scenario |
|---|---|
| `test_hook_generation_matches_baseline` | `_test_reporting_hook.py` matches baseline when generated with `--include-tests` |
| `test_test_flowgroup_matches_baseline` | `tst_customer_dq.py` test flowgroup matches baseline when generated with `--include-tests` |
| `test_provider_module_copied_matches_baseline` | Provider module copy under `_lhp_test_reporting/` matches baseline |
| `test_no_hook_without_include_tests` | Without `--include-tests`, no hook file generated |
| `test_no_test_flowgroup_without_include_tests` | Without `--include-tests`, test flowgroup not generated |
| `test_existing_baselines_unaffected_without_include_tests` | Adding `test_reporting` config does NOT change existing baselines (without `--include-tests`) |
| `test_validate_with_include_tests` | Validate command succeeds with valid test_reporting config |

---

## `test_job_orchestration_e2e.py` (6 tests)

| Test | Scenario |
|---|---|
| `test_deps_bundle_without_job_config` | `lhp deps -b` generates correct orchestration job without job config |
| `test_deps_bundle_with_job_config` | `lhp deps -b -jc` generates correct orchestration job with job config applied |
| `test_multi_job_with_default_master` | Multi-job generation with default master job name |
| `test_multi_job_with_custom_master_name` | Multi-job generation with custom master job name |
| `test_multi_job_without_master` | Multi-job generation with master job disabled (`disable_master_job: true`) |
| `test_deps_passes_through_file_arrival_trigger` | `trigger.file_arrival` block in `job_config.yaml` flows through to generated job YAML |

---

## `test_quarantine_e2e.py` (5 tests)

| Test | Scenario |
|---|---|
| `test_quarantine_non_cloudfiles_generation` | Quarantine non-CloudFiles generates expected output matching baseline |
| `test_quarantine_generated_code_structure` | Quarantine output has all required DLQ pipeline components |
| `test_quarantine_validate_passes` | `lhp validate` passes on valid quarantine config |
| `test_quarantine_validate_fails_missing_block` | Validation fails when `mode=quarantine` but quarantine block missing |
| `test_quarantine_runtime_rescued_data_detection` | Quarantine generates runtime `_rescued_data` detection (both branches) |

---

## `test_deps_extraction.py` (4 tests)

Dependency extraction from generated code (the BWH-bug regression class).

| Test | Scenario |
|---|---|
| `test_deps_extracts_write_target_sql_path_upstream` | Materialized-view SQL in `write_target.sql_path` contributes to dependency graph |
| `test_deps_extracts_write_target_python_upstream` | Custom-sink Python in `write_target.module_path` is parsed for upstream tables |
| `test_deps_flowgroup_graph_has_incoming_edges_on_mv_flowgroup` | Flowgroup whose sole content is an MV has incoming edges (the BWH bug invariant) |
| `test_deps_union_of_python_parse_and_explicit_source` | Python actions: parser output is UNIONED with explicit `source:` declarations |

---

## `test_local_variables_e2e.py` (3 tests)

| Test | Scenario |
|---|---|
| `test_local_variables_resolve_correctly` | `%{local_var}` resolves correctly during processing |
| `test_undefined_local_variable_raises_error` | Undefined local variable raises `LHPError` |
| `test_generate_command_with_local_variables` | `lhp generate` works end-to-end with local variables |

---

## `test_test_reporting_cleanup_e2e.py` (1 test)

| Test | Scenario |
|---|---|
| `test_include_tests_transition_cleans_artifacts` | After `--include-tests` run, regenerating without the flag reaps stale test-reporting artifacts (regression test) |

---

## How to interpret test groupings

### BM-* (Bundle Manager)

Numbered `BM-1` through `BM-15`, with sub-letters (`BM-8a`, `BM-8b`) for variants. The numbering corresponds to a test scenario specification document — see `test_bundle_manager_e2e.py` docstrings for the mapping. Tests `BM-9` through `BM-13` are diagnostic tools used during the BM-8 family development; they remain in the suite as regression guards.

### TC-* (Test-reporting Cases)

Numbered `TC-07` through `TC-25` (with gaps where TCs are covered elsewhere or were merged). The TC numbering matches the test-reporting feature specification document.

### Cross-platform tests

`test_cross_platform_state_file_compatibility` exists because state files use file paths that differ between Windows and POSIX. The test simulates loading a state file generated on the other platform.

### Diagnostic tests vs regression tests

Some tests (notably BM-9 through BM-13) were originally written as diagnostic tools. They remain in the suite because the diagnostic assertions form useful regression checks against the same code paths. Don't remove them without reading the docstrings — they often catch issues no other test does.

---

## Updating this document

When adding or removing a test, update this catalog. The total count at the top should match `pytest --collect-only tests/e2e/`.

To regenerate the test name + first-line-of-docstring listing as a starting point:

```bash
cd tests/e2e && python3 -c "
import ast, os
for f in sorted(os.listdir('.')):
    if not (f.startswith('test_') and f.endswith('.py')):
        continue
    print(f'=== {f} ===')
    with open(f) as fh:
        tree = ast.parse(fh.read())
    for n in ast.walk(tree):
        if isinstance(n, ast.FunctionDef) and n.name.startswith('test_'):
            doc = ast.get_docstring(n) or ''
            print(f'{n.name}: {(doc.split(chr(10))[0] or \"(no docstring)\").strip()}')
"
```
