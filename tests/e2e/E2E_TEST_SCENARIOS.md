# E2E Test Scenarios

This document catalogs every scenario covered by the LHP end-to-end test suite. **156 tests across 19 files** (150 passing + 6 xfail), all running against the shared fixture project at `tests/e2e/fixtures/testing_project/`.

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
| `test_negative_paths_e2e.py` | 8 | Negative-path coverage (B6): bad YAML, unknown env, undefined token/secret, unknown action type, duplicate flowgroup, python collision (3 passing + 5 xfail TDD baselines) |
| `test_generate_flags_e2e.py` | 6 | `lhp generate` flag plumbing (B7): `--dry-run`, `--pipeline`, `--no-bundle`, `--no-cleanup`, `--pipeline-config`, `--output` |
| `test_state_cli_e2e.py` | 4 | `lhp state` subcommand (B4): `--orphaned`, `--stale`, `--orphaned --cleanup`, `--stale --regen` |
| `test_load_jdbc_e2e.py` | 2 | JDBC load action (B5.1): table-mode (`.option("dbtable", ...)`) and query-mode (`.option("query", ...)`) with secret substitution |
| `test_load_python_e2e.py` | 1 | Python load action (B5.2): v0.8.6 unified copy-and-import pattern (`from custom_python_functions.<leaf> import ...`), `@dp.temporary_view()` decorator |
| `test_transform_temp_table_e2e.py` | 1 | temp_table transform (B5.3): intermediate staging table via `@dp.table(temporary=True)`, used by multiple downstream actions |
| `test_delta_cdc_reader_e2e.py` | 4 | Delta CDC reader (B8): `readChangeFeed`, `startingVersion`+`ignoreDeletes`, `versionAsOf` (batch time-travel), `skipChangeCommits` |

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

## `test_test_actions_e2e.py` (6 tests, B1)

Covers every documented test action `test_type` — one method per type. All tests run with `--include-tests` and use targeted `_compare_file_hashes` against `generated_baseline_with_tests/dev/12_test_actions/<file>.py`.

| Test | Scenario |
|---|---|
| `test_row_count_matches_baseline` | `row_count`: cross-product of two `SELECT COUNT(*)` subqueries; expectation `abs(source_count - target_count) <= tolerance` |
| `test_referential_integrity_matches_baseline` | `referential_integrity`: LEFT JOIN; expectation `ref_<col> IS NOT NULL` aliased on the joined column |
| `test_schema_match_matches_baseline` | `schema_match`: information_schema diff. **Currently `xfail`** — the baseline encodes the post-fix expected SQL (FQN split into `table_catalog`/`table_schema`/`table_name`, catalog-qualified `information_schema`); LHP's current generator at `src/lhp/generators/test/test_generator.py:42-67` and `:305-313` emits `WHERE table_name = '<FQN>'` which never matches in Unity Catalog. Test flips to `XPASS` automatically when LHP is fixed |
| `test_all_lookups_found_matches_baseline` | `all_lookups_found`: LEFT JOIN against lookup table; expectation `lookup_<col> IS NOT NULL` |
| `test_custom_sql_matches_baseline` | `custom_sql`: pass-through user SQL with attached user expectations |
| `test_custom_expectations_matches_baseline` | `custom_expectations`: synthesized `SELECT * FROM <source>` with multi-expectation grouping under a single `@dp.expect_all` decorator (warn variant) |

**Fixture:** `pipelines/12_test_actions/{01..06}_<test_type>.yaml` — six pure-test-only flowgroups under `pipeline: 12_test_actions`. Without `--include-tests`, the entire pipeline is skipped per the test-only-flowgroup guard at `src/lhp/core/services/code_generator.py:97-107`, so these flowgroups do not pollute the standard `generated_baseline/` comparison.

**Cross-fixture impact:** Adding the `12_test_actions` pipeline forces `union_event_logs.py` (monitoring auto-discovers all pipelines) and the deps orchestration baselines (`acme_edw_orchestration.job.yml`, `acme_edw_orchestration-JC.job.yml`) to include a `12_test_actions` entry. Eight monitoring variant baselines were updated in the same change.

---

## `test_builtin_sinks_e2e.py` (3 tests, B2)

Covers the 3 streaming sink types from `docs/actions/write_actions.rst:847-1646`. All tests run with `lhp generate --env dev --force` (no `--include-tests`) and use targeted `_compare_file_hashes` against `generated_baseline/dev/13_sinks/<file>.py`.

| Test | Scenario |
|---|---|
| `test_delta_sink_matches_baseline` | Delta sink: top-level `dp.create_sink(name=..., format="delta", options={tableName, checkpointLocation, mergeSchema})` followed by `@dp.append_flow` decorating `f_<sink>_1()` reading via `spark.readStream.table(<source_view>)`. Verifies LHP's `dp.create_sink + @dp.append_flow` pattern per `write_actions.rst:957-979` |
| `test_kafka_sink_matches_baseline` | Kafka sink: `dp.create_sink(format="kafka", options={kafka.bootstrap.servers, topic, kafka.security.protocol, kafka.sasl.*, checkpointLocation})` plus auto-injected runtime `if "value" not in df.columns: raise ValueError(...)` guard inside the `@dp.append_flow` function (per `write_actions.rst:1009-1012`). Also verifies `${secret:scope/key}` substitution into the JAAS f-string with `dbutils.secrets.get(...)` (consistent with the existing kafka-load baseline) |
| `test_foreach_batch_sink_matches_baseline` | ForEachBatch sink: `@dp.foreach_batch_sink(name=...)` decorator wrapping a `def <sink_name>(df, batch_id):` whose body is the user's inline `batch_handler` block-scalar (with `${catalog}.${schema}` substitutions resolved). Paired with `@dp.append_flow(target=..., name="f_<sink>_1")` reading the source view. Per `write_actions.rst:1576-1600` |

**Fixture:** `pipelines/13_sinks/{01_delta_sink,02_kafka_sink,03_foreach_batch_sink}.yaml` — three flowgroups under `pipeline: 13_sinks`. Each flowgroup includes a load action (creating the source view) plus the sink write action. Unlike B1, these are not test-only flowgroups, so they generate output regardless of `--include-tests`.

**Cross-fixture impact:** Adding the `13_sinks` pipeline forces the same monitoring/deps baseline updates as B1 (eight monitoring variant `union_event_logs.py` files + two deps orchestration job YAMLs include a `13_sinks` entry).

---

## `test_negative_paths_e2e.py` (8 tests, B6)

Negative-path coverage for `lhp validate` and `lhp generate`. All eight tests mutate a deep-copied tmp project at runtime — no permanent fixtures are added. Five tests are `@pytest.mark.xfail(strict=True)` TDD baselines encoding the expected post-fix behavior; their docstrings cite the specific LHP source location to fix. When LHP is updated, those tests flip to XPASS automatically.

| Test | Scenario |
|---|---|
| `test_invalid_yaml_in_flowgroup_fails` *(xfail)* | Malformed YAML must fail validation. Today LHP skips bad files with a WARNING and exits 0; expected: non-zero exit on parse error. |
| `test_unknown_environment_fails` | `lhp generate --env xyz` fails when no `substitutions/xyz.yaml` exists. |
| `test_missing_environment_flag_fails` | `lhp generate` without `--env` returns Click's "Missing option" usage error (`main.py:323` declares the flag required). |
| `test_undefined_token_reference_fails` *(xfail)* | `${nonexistent_token}` triggers `LHP-CFG-010` from `flowgroup_processor.py:158`. Today the per-pipeline detail is hidden without `--verbose`; expected: code surfaced by default. |
| `test_undefined_secret_reference_fails` *(xfail)* | `${secret:missing_scope/missing_key}` must fail because the scope is not in the available set. Today `SecretValidator()` at `orchestrator.py:139` has empty `available_scopes` so the existence check is a silent no-op; expected: configured scope set + scope-existence check enforced. |
| `test_unknown_action_type_fails` *(xfail)* | `type: bogus_type` must surface `LHP-ACT-001`. Today the Pydantic enum failure becomes a parse-error skip with WARNING; expected: clean LHP-ACT-001 with non-zero exit. |
| `test_duplicate_flowgroup_id_fails` | Two flowgroup files with same `(pipeline, flowgroup)` tuple surface `LHP-VAL-009` via `orchestrator.py:927-952`. Uses `lhp generate` because `validate_pipeline_by_field` does not run the cross-file duplicate check (only the generate path does at `orchestrator.py:1010`). |
| `test_python_import_collision_fails` *(xfail)* | Two different `module_path` values resolving to the same destination stem trigger `PythonFunctionConflictError` (`LHP-VAL-019`, `python_file_copier.py:14-41`). Today the error is displayed but `generate` continues to "✅ completed successfully" with exit 0; expected: non-zero exit when any pipeline fails. |

**Fixture impact:** None — every mutation targets `self.project_root` (deep copy), so no `monitoring_baseline/`, deps orchestration, or other cross-fixture baselines are touched.

---

## `test_generate_flags_e2e.py` (6 tests, B7)

Behavioral verification of `lhp generate` CLI flags. All six pass today and run against the standard fixture without baseline comparison — each asserts the documented effect of one flag on the produced filesystem state.

| Test | Scenario |
|---|---|
| `test_dry_run_emits_no_python_files` | `--dry-run` writes nothing under `generated/dev/`; output includes "Would generate" / "Dry run" preview. Implementation: `generate_command.py:389`. |
| `test_pipeline_filter_generates_only_matching_pipelines` | `--pipeline acmi_edw_bronze` restricts output to a single pipeline dir under `generated/dev/`; all other pipeline dirs are absent or empty. Implementation: `generate_command.py:350-351`. |
| `test_no_bundle_skips_resources_dir` | `--no-bundle` populates `generated/dev/` but leaves `resources/lhp/` empty (no `.pipeline.yml` written). Implementation: `generate_command.py:209` (`if not no_bundle:` gates bundle ops). |
| `test_no_cleanup_preserves_orphaned_files` | After a successful generate, deleting a flowgroup yaml and re-running with `--no-cleanup` preserves the now-orphaned `.py`. Implementation: `generate_command.py:134` (force-wipe guard) + `:242-246` (`state_manager=None`). |
| `test_pipeline_config_explicit_path` | `--pipeline-config config/pipeline_config.yaml` applies the fixture's classic-cluster overrides to `acmi_edw_raw.pipeline.yml` (`serverless: false`, `node_type_id: Standard_D4ds_v5` from substitutions/dev.yaml). |
| `test_output_redirect` | `--output <tempdir>` redirects all generated Python output to the specified path; `generated/dev/` remains empty. Bundle resources still land in `resources/lhp/` (unchanged by `--output`). |

**Fixture impact:** None — all mutations target the deep copy; `--output` redirects to `tempfile.TemporaryDirectory()`.

---

## `test_state_cli_e2e.py` (4 tests, B4)

Pre-refactor confidence for the `lhp state` subcommand. All four tests run with the same seed pattern: `lhp generate --env dev --force` populates `.lhp_state.json`, then the test mutates a source and runs `lhp state` with the appropriate flag combination.

| Test | Scenario |
|---|---|
| `test_state_orphaned_lists_orphaned_files` | After deleting `pipelines/02_bronze/orders/orders_bronze.yaml`, `lhp state --env dev --orphaned` lists `generated/dev/acmi_edw_bronze/orders_bronze.py` as orphaned. No file is deleted (no `--cleanup`). |
| `test_state_stale_lists_stale_files` | After modifying `py_functions/sample_func.py`, `lhp state --env dev --stale` lists the dependent generated files with a "Dependency changed" indicator. |
| `test_state_cleanup_removes_orphans` | `lhp state --env dev --orphaned --cleanup` physically deletes the orphaned `.py` AND removes its entry from `.lhp_state.json`. Implementation: `state_command.py:108-122`. |
| `test_state_regen_regenerates_stale` | After modifying `py_functions/sample_func.py`, `lhp state --env dev --stale --regen` regenerates the stale files. Verified by comparing the dependency-checksum in `.lhp_state.json` before/after: it must change to match the new source. Implementation: `state_command.py:124-142`. |

**Helper reuse:** `_load_state_file` and `_extract_py_function_checksum` mirror the patterns at `test_bundle_manager_e2e.py:1849-1921`.

**Flag combination requirement:** `--cleanup` only acts when paired with `--orphaned`; `--regen` only with `--stale`. The dispatch at `state_command.py:108-142` routes flags through `--orphaned`/`--stale`/`--new` branches, so `--cleanup` alone falls through to the comprehensive view.

**Fixture impact:** None — all mutations target the deep copy. Destructive operations (`--cleanup`, `--regen`) never touch `tests/e2e/fixtures/testing_project/`.

---

## `test_load_jdbc_e2e.py` (2 tests, B5.1)

Covers both modes of the JDBC load source documented in `docs/actions/load_actions.rst:836-988`. Tests run with `lhp generate --env dev --force` and hash-compare both the generated `.py` and the pipeline resource YAML against baselines.

| Test | Scenario |
|---|---|
| `test_jdbc_table_mode_matches_baseline` | `source.table` produces `.option("dbtable", "public.products")` on a `spark.read.format("jdbc")` chain inside `@dp.temporary_view()`. User/password options resolve `${secret:database/...}` to `dbutils.secrets.get(scope="dev_db_secrets", key=...)` via the existing scope alias |
| `test_jdbc_query_mode_matches_baseline` | `source.query` produces `.option("query", """...""")` (triple-quoted block-scalar SQL) on the same `spark.read.format("jdbc")` chain. Operational metadata `_processing_timestamp` is appended via `df.withColumn(..., F.current_timestamp())` after the load |

**Fixture:** `pipelines/14_jdbc_load/{jdbc_table_mode,jdbc_query_mode}.yaml` — two flowgroups under `pipeline: 14_jdbc_load`. The placeholder URL `jdbc:postgresql://example.com:5432/sample` lets generation succeed without an actual database connection.

**Cross-fixture impact:** Adding the `14_jdbc_load` pipeline forces an additional entry in the eight `monitoring_baseline/<variant>/union_event_logs.py` files and in the two deps orchestration job YAMLs (`acme_edw_orchestration.job.yml`, `acme_edw_orchestration-JC.job.yml`).

---

## `test_load_python_e2e.py` (1 test, B5.2)

Covers the v0.8.6 unification of Python load with the custom_datasource/custom_sink copy-and-import pattern (commit `c1058c07`). Verifies all four artifacts: the pipeline `.py`, the copied extractor module, its package `__init__.py`, and the bundle resource YAML.

| Test | Scenario |
|---|---|
| `test_python_load_matches_baseline` | User's `.py` extractor is copied to `<output>/custom_python_functions/api_extractor.py` (with an `LHP-SOURCE:` provenance header and "DO NOT EDIT" notice). The pipeline file imports via `from custom_python_functions.api_extractor import extract_customer_data`, calls it inside `@dp.temporary_view()` with an inline `parameters` dict (JSON-rendered). cloudpickle is NOT used (Python load doesn't register a Spark data source, unlike `custom_datasource` which DOES) |

**Fixture:** `pipelines/15_python_load/python_load_basic.yaml` (flowgroup) + `pipelines/15_python_load/extractors/api_extractor.py` (the user's loader function returning a small in-memory DataFrame). The `module_path` is resolved from the project root per `src/lhp/utils/external_file_loader.py:resolve_external_file_path`, not from the YAML location.

**Cross-fixture impact:** Same as B5.1 — `15_python_load` is added to the eight monitoring variant baselines and the two deps orchestration jobs.

---

## `test_transform_temp_table_e2e.py` (1 test, B5.3)

Covers the temp_table transform documented in `docs/actions/transform_actions.rst:794-897`. LHP emits `@dp.table(temporary=True)` — this matches the documented Python output examples at lines 867-874 (simple passthrough) and 883-897 (with SQL), and is distinct from `@dp.temporary_view()` (used for non-materialized logical views).

| Test | Scenario |
|---|---|
| `test_temp_table_creates_staging_view_matches_baseline` | Full chain load → temp_table → sql transform → MV write. The `staging_orders` temp_table is emitted as `@dp.table(temporary=True)` reading via `spark.readStream.table("v_raw_orders")` from the upstream load. The downstream SQL transform reads `FROM staging_orders` (target name), not from the action name |

**Fixture:** `pipelines/16_temp_table/staging_chain.yaml` — single flowgroup `staging_chain` demonstrating the intermediate-staging use case (one temp_table feeds multiple downstream reads).

**Cross-fixture impact:** Same as B5.1 — `16_temp_table` is added to the eight monitoring variant baselines and the two deps orchestration jobs.

---

## `test_delta_cdc_reader_e2e.py` (4 tests, B8)

Covers the Delta Change Data Feed (CDF) and time-travel options documented in `docs/actions/load_actions.rst:274-420`. LHP's `DeltaLoadGenerator` at `src/lhp/generators/load/delta.py:23-240` enforces seven mutual-exclusion guards at parse time (e.g. `readChangeFeed` vs `skipChangeCommits`, `versionAsOf` vs `timestampAsOf`, ending bounds in stream mode); these tests verify only the happy-path generated code for four common option combinations.

| Test | Scenario |
|---|---|
| `test_delta_read_change_feed_matches_baseline` | `readChangeFeed: "true"` + `startingVersion: "0"` (stream mode) → `spark.readStream.option("readChangeFeed", "true").option("startingVersion", "0").table(...)`. CDF metadata columns (`_change_type`, `_commit_version`, `_commit_timestamp`) are NOT explicitly projected — they appear naturally on the DataFrame per docs:370-395 |
| `test_delta_starting_version_matches_baseline` | `readChangeFeed: "true"` + `startingVersion: "5"` + `ignoreDeletes: "true"` (stream mode) → three options on `spark.readStream`, in YAML-declaration order |
| `test_delta_time_travel_matches_baseline` | `versionAsOf: "10"` (batch mode) → `spark.read.option("versionAsOf", "10").table(...)`. Time travel is batch-only per Delta semantics — `spark.readStream` would be wrong here |
| `test_delta_skip_change_commits_matches_baseline` | `skipChangeCommits: "true"` (stream mode) → `spark.readStream.option("skipChangeCommits", "true").table(...)`. Must NOT include `readChangeFeed` (mutually exclusive per docs:328) |

**Fixture:** `pipelines/17_delta_cdc/{cdc_read_change_feed,cdc_starting_version,cdc_time_travel,cdc_skip_change_commits}.yaml` — four flowgroups under `pipeline: 17_delta_cdc`.

**Cross-fixture impact:** Same as B5.x — `17_delta_cdc` is added to the eight monitoring variant baselines and the two deps orchestration jobs.

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
