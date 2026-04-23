# Changelog

All notable changes to Lakehouse Plumber are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.7] — 2026-04-23

### Fixed

- `lhp deps` now honors `trigger.file_arrival` (and any other Databricks
  Jobs API field) in `job_config.yaml`. Previously, keys the Jinja template
  didn't explicitly handle were silently dropped from the generated
  orchestration job YAML.

### Added

- Job-config **pass-through**: any top-level key in `job_config.yaml` that is
  not one of LHP's explicitly handled keys (`max_concurrent_runs`, `queue`,
  `performance_target`, `timeout_seconds`, `tags`, `email_notifications`,
  `webhook_notifications`, `permissions`, `schedule`, `notebook_cluster`) is
  now rendered verbatim into the generated job YAML. Users can use
  newly-released Databricks Jobs API fields (trigger types, `continuous`,
  `run_as`, `git_source`, `health`, `parameters`, `environments`,
  `edit_mode`, `budget_policy_id`, …) without waiting for an LHP release
  that adds explicit support.

## [0.8.6] — 2026-04-23

### Fixed

- `lhp deps` now correctly extracts source tables from SQL and Python bodies
  externalized inside `write_target`. Previously, materialized views and
  custom sinks using `write_target.sql_path`, `write_target.sql`,
  `write_target.module_path`, or `write_target.batch_handler` appeared to
  have no upstream dependencies, causing gold pipelines and silver-MV
  flowgroups to be reported as root nodes in the dependency graph.

### Changed

- Python dependency extraction now tracks local-variable and module-level
  string-literal bindings. Patterns like
  `tbl = "cat.sch.t"; spark.read.table(tbl)` are now resolved. Reassignments
  and conditional branches emit the union of possible values. Variables whose
  value comes from function parameters, function return values, or string
  concatenation remain unresolvable — for those cases, declare an explicit
  `source:` on the action; for Python actions, parser output and explicit
  `source:` are unioned.

## [0.8.5] — 2026-04-22

- Migrate Jinja2 loaders to `PackageLoader`.

## [0.8.4]

- Minor bug fixes.

## [0.8.3]

- Monitoring race fix.
- Dedicated `monitoring.job_config_path` setting.

## [0.8.2]

- Multi-CDC fan-in.
- Per-pipeline monitoring checkpoints.
- `${token}` migration.

## [0.8.0]

- Performance optimizations.
- CloudFiles improvements.
- Snapshot CDC parameters.
