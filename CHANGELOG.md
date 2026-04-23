# Changelog

All notable changes to Lakehouse Plumber are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
