.. meta::
   :description: Complete catalog of Lakehouse Plumber error codes (LHP-<CATEGORY>-<NUMBER>), grouped by category, with the cause and resolution for every code the CLI can raise.

==========================================
LHP error code catalog
==========================================

Every Lakehouse Plumber error carries a code of the form ``LHP-<CATEGORY>-<NUMBER>`` (for example ``LHP-VAL-001``). The category prefix is one of ``CFG`` (configuration), ``VAL`` (validation), ``IO`` (input/output), ``ACT`` (action), ``DEP`` (dependency), ``GEN`` (general), or ``DEPR`` (deprecation).

When a command fails, the CLI exits non-zero: ``1`` for a domain error (any code below), ``2`` for a usage error, and ``3`` for an unexpected internal error; success is ``0``. Script integrations should branch on the exit code, not on stderr text. Codes noted as warnings are advisory and never fail a run.

Configuration errors (LHP-CFG)
==============================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-CFG-001
     - Configuration conflict — the same setting is declared more than one way (for example a legacy ``format`` field and ``options.cloudFiles.format``).
     - Keep one form only; prefer the ``options:`` mapping.
   * - LHP-CFG-002
     - Project configuration in ``lhp.yaml`` could not be loaded.
     - Fix the reported ``lhp.yaml`` error.
   * - LHP-CFG-003
     - Invalid operational-metadata column config, or an ``include`` field that is not a list of strings.
     - Correct the field to the shape named in the message.
   * - LHP-CFG-004
     - More than one action sets ``create_table: true`` for the same table, or an invalid operational-metadata preset config.
     - Let a single action create each table; fix the preset config.
   * - LHP-CFG-005
     - Empty flowgroup file, or an invalid operational-metadata preset column reference.
     - Add flowgroup content; reference a defined column.
   * - LHP-CFG-006
     - ``event_log`` is not a mapping, or operational-metadata column references are undefined.
     - Define ``event_log`` as a mapping; reference defined columns.
   * - LHP-CFG-007
     - ``event_log`` is missing ``catalog`` or ``schema``, or a schema file failed to parse.
     - Provide both keys; fix the schema file.
   * - LHP-CFG-008
     - Invalid ``monitoring`` config, or ``sql_path`` cannot be resolved without a project root.
     - Fix the ``monitoring`` block per the message.
   * - LHP-CFG-009
     - YAML parsing error (bad indentation, unquoted special characters).
     - Fix YAML syntax; quote values containing ``:`` ``{`` ``}`` ``[`` ``]``.
   * - LHP-CFG-010
     - A removed/deprecated field name was used.
     - Replace it with the field named in the message.
   * - LHP-CFG-011
     - Invalid ``database`` field in a write target, or undefined local variable(s) ``%{...}``.
     - Fix the target; define the referenced local variables.
   * - LHP-CFG-012
     - Required template parameters are missing.
     - Add every missing key under ``template_parameters:``.
   * - LHP-CFG-013
     - A ``schema`` field contains DDL rather than a schema name.
     - Use a schema name or reference, not inline DDL.
   * - LHP-CFG-014
     - No flowgroups were found, or an invalid output format was requested.
     - Fix the pipeline selector; use a supported output format.
   * - LHP-CFG-015
     - Reserved — retired. Formerly raised for an unmatched ``--pipeline`` filter; that failure now surfaces as a validation finding.
     - Not emitted by the current release.
   * - LHP-CFG-016
     - Reserved — no raise site in the current source.
     - Not emitted by the current release.
   * - LHP-CFG-017
     - Reserved — no raise site in the current source.
     - Not emitted by the current release.
   * - LHP-CFG-020
     - Bundle resource operation failed (resource YAML generation, sync, or directory access).
     - Check ``databricks.yml``; verify bundle resource files are valid YAML.
   * - LHP-CFG-021
     - Bundle YAML processing error (parsing, validating, or updating a bundle resource file).
     - Fix the YAML syntax and encoding of the named file.
   * - LHP-CFG-023
     - Bundle support is enabled but ``--pipeline-config`` / ``-pc`` was not passed.
     - Pass ``-pc <path>``, or ``--no-bundle`` to skip bundle resource generation.
   * - LHP-CFG-024
     - Bundle template fetch, render, or apply failed.
     - Check the network and the template path or URL.
   * - LHP-CFG-025
     - Invalid bundle configuration (structure, missing files, or bad settings).
     - Review ``databricks.yml`` against the bundle documentation.
   * - LHP-CFG-026
     - Catalog/schema validation failed for one or more pipelines.
     - Set ``catalog`` and ``schema`` in ``project_defaults`` or per pipeline; check substitutions.
   * - LHP-CFG-027
     - The named template was not found.
     - Check spelling; run ``lhp list templates``.
   * - LHP-CFG-028
     - ``BundleManager`` was constructed without a project root.
     - Internal — pass a non-null project root.
   * - LHP-CFG-029
     - Template rendering referenced an undefined variable.
     - Provide the missing template variable.
   * - LHP-CFG-030
     - Template has a Jinja syntax error.
     - Fix the template syntax.
   * - LHP-CFG-031
     - Generated Python source failed to parse (``ast.parse`` reported a SyntaxError).
     - Almost always a generator/template bug; file a report with the flowgroup YAML.
   * - LHP-CFG-033
     - The ``ruff format`` pass on generated code exited non-zero.
     - Inspect ruff's output; re-run with ``--no-format`` to skip formatting.
   * - LHP-CFG-034
     - The ``ruff`` executable was not found for the formatting pass.
     - ``pip install ruff`` or reinstall LHP; ensure ``ruff`` is on ``PATH``.
   * - LHP-CFG-040
     - A blueprint file was placed in a flowgroup (pipelines) directory.
     - Move the blueprint out of the pipelines directory.
   * - LHP-CFG-047
     - Empty blueprint file.
     - Add blueprint content.
   * - LHP-CFG-048
     - Blueprint file has multiple YAML documents.
     - Keep one document per blueprint file.
   * - LHP-CFG-049
     - File is not a blueprint (missing marker).
     - Add the blueprint marker.
   * - LHP-CFG-050
     - Invalid blueprint definition (parse error).
     - Fix the blueprint YAML.
   * - LHP-CFG-051
     - Empty instance file.
     - Add instance content.
   * - LHP-CFG-052
     - Instance file has multiple YAML documents.
     - Keep one document per instance file.
   * - LHP-CFG-054
     - Invalid instance definition — ``use_blueprint`` is not a single non-empty string.
     - Set ``use_blueprint: <name>`` to one non-empty string.
   * - LHP-CFG-056
     - Reserved — no raise site in the current source.
     - Not emitted by the current release.
   * - LHP-CFG-059
     - Malformed secret reference (empty scope or key).
     - Use ``${secret:scope/key}`` with both parts present.
   * - LHP-CFG-060
     - Malformed top-level ``wheel`` block (not a mapping, or ``artifact_volume`` is not a string).
     - Define ``wheel:`` as a mapping with a string ``artifact_volume``.
   * - LHP-CFG-061
     - ``packaging: wheel`` but ``wheel.artifact_volume`` is missing/empty or is not a ``/Volumes/...`` path.
     - Set ``artifact_volume`` to a ``/Volumes/...`` path, or use ``packaging: source``.
   * - LHP-CFG-062
     - Invalid ``sandbox`` block (not a mapping, unknown strategy, or empty ``allowed_envs``).
     - Define ``sandbox:`` correctly; use ``strategy: table``.
   * - LHP-CFG-063
     - Invalid sandbox ``table_pattern`` (missing or unrecognized placeholders).
     - Use only the ``{namespace}`` and ``{table}`` placeholders.
   * - LHP-CFG-064
     - Invalid sandbox profile in ``.lhp/profile.yaml`` (bad ``namespace`` or empty ``pipelines``).
     - Fix ``namespace`` and ``pipelines`` under a top-level ``sandbox:`` key.
   * - LHP-CFG-065
     - ``--sandbox`` targeted an environment not listed in ``sandbox.allowed_envs``.
     - Run an allowed env, or add the env to ``allowed_envs``.
   * - LHP-CFG-066
     - A Unity Catalog tag key or value is illegal (charset, whitespace, or length).
     - Fix the offending tag key or value to meet Unity Catalog rules.
   * - LHP-CFG-067
     - Invalid UC ``tags_file`` — missing/unknown top-level key, unsupported ``version``, wrong-typed ``table``/``tags``, a ``columns`` block that is not a mapping of ``column_name: {key: value}`` (or a non-string column name), a file declaring neither ``tags`` nor ``columns``, or a ``table`` that does not match the write target.
     - Use the strict ``version``/``table``/``tags``/``columns`` format; declare at least one of ``tags``/``columns``; set ``table:`` to the write target.

Validation errors (LHP-VAL)
===========================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-VAL-001
     - A required field (for example ``source``, ``target``, ``type``) is missing.
     - Add the field named in the message.
   * - LHP-VAL-002
     - Several validation problems in one action, listed together.
     - Fix each listed item.
   * - LHP-VAL-003
     - A ``job_name`` list is empty.
     - Provide at least one job name.
   * - LHP-VAL-004
     - A ``job_name`` value is duplicated.
     - Make job names unique.
   * - LHP-VAL-005
     - A pipeline list is empty.
     - Declare at least one pipeline.
   * - LHP-VAL-006
     - A field has a value outside its allowed set.
     - Use one of the valid values shown.
   * - LHP-VAL-007
     - Invalid ``readMode``.
     - Use ``stream`` or ``batch``.
   * - LHP-VAL-008
     - A field has the wrong data type.
     - Match the expected type from the message.
   * - LHP-VAL-009
     - Duplicate pipeline+flowgroup combinations, or an invalid template-parameter type.
     - Remove duplicates; fix the parameter type.
   * - LHP-VAL-010
     - Duplicate monitoring-pipeline configuration, or misuse of the ``__eventlog_monitoring`` alias.
     - Use only one name or configuration.
   * - LHP-VAL-011
     - Schema syntax error in a schema file's column definitions.
     - Fix the schema syntax against the expected format.
   * - LHP-VAL-012
     - Source given as a bare string where a full source mapping is required.
     - Provide a full ``source`` configuration.
   * - LHP-VAL-013
     - Two options were combined that cannot be used together.
     - Remove one of the conflicting options.
   * - LHP-VAL-014
     - Mixed flowgroup syntax, or a missing/invalid ``source`` for a Python transform.
     - Fix per the message.
   * - LHP-VAL-015
     - Missing flowgroup context for Python file copying.
     - Internal — ensure flowgroup context is supplied.
   * - LHP-VAL-016
     - Invalid schema definition (missing ``columns`` or a malformed column), a temp-table transform with no source view, or a schema file carries a column ``tags:`` key — column tags in schema files are no longer supported (declare them in the write target's ``tags_file`` under ``columns:``). The migration case is raised at ``lhp generate`` time (``lhp validate`` runs no code generation).
     - Fix the schema per the message; move any column tags to the ``tags_file`` ``columns:`` block.
   * - LHP-VAL-017
     - Missing AWS MSK IAM options on a Kafka action, or an invalid source config for a materialized-view write.
     - Add the required options; fix the source.
   * - LHP-VAL-018
     - Missing OAuth options on a Kafka action.
     - Add the required OAuth options.
   * - LHP-VAL-019
     - Python function naming conflict — two source files map to the same destination file.
     - Rename one function, module, or ``module_path``.
   * - LHP-VAL-020
     - Unknown dependency-graph level requested.
     - Use a valid level.
   * - LHP-VAL-021
     - Import name collision in generated code.
     - Resolve the colliding import name.
   * - LHP-VAL-022
     - Invalid table identifier for a ``schema_match`` test.
     - Use a valid table identifier.
   * - LHP-VAL-023
     - A Python-function root directory is itself a package.
     - Remove ``__init__.py`` from the root directory.
   * - LHP-VAL-024
     - A local helper was imported with a plain dotted import.
     - Use a relative import for the local helper.
   * - LHP-VAL-025
     - A local helper module could not be found.
     - Fix the import path, or add the module.
   * - LHP-VAL-041
     - A blueprint instance references an unknown blueprint.
     - Reference an existing blueprint.
   * - LHP-VAL-042
     - A required blueprint parameter is missing in an instance.
     - Provide the required parameter.
   * - LHP-VAL-043
     - An instance sets an unknown parameter key.
     - Remove or rename the key.
   * - LHP-VAL-044
     - A ``${env_token}`` was used in a blueprint field that forbids it.
     - Use a ``%{var}`` template placeholder instead.
   * - LHP-VAL-045
     - Duplicate (pipeline, flowgroup) after blueprint expansion.
     - Vary iteration values so combinations are unique.
   * - LHP-VAL-046
     - Duplicate blueprint name.
     - Rename one blueprint.
   * - LHP-VAL-053
     - Instance file is missing its blueprint reference.
     - Add ``use_blueprint: <name>``.
   * - LHP-VAL-055
     - Unresolved ``%{var}`` in a blueprint template.
     - Define the referenced local variable or iteration key.
   * - LHP-VAL-061
     - Conflicting or mixed blueprint-instance syntax (hard error, not a deprecation).
     - Use one form: ``use_blueprint: <name>``.
   * - LHP-VAL-062
     - A pipeline ``packaging`` value is not ``source`` or ``wheel``.
     - Use exactly ``source`` or ``wheel``.
   * - LHP-VAL-063
     - A ``depends_on`` entry is malformed.
     - Use a well-formed table reference (``catalog.schema.table``, ``schema.table``, or ``table``).
   * - LHP-VAL-064
     - A sandbox profile ``pipelines`` entry matched no pipelines, or names the monitoring pipeline.
     - Fix or remove the entry against the available pipeline names.
   * - LHP-VAL-065
     - Warning (category sandbox) — a sandbox-renamed sink is also produced by an out-of-scope pipeline; the rename still proceeds.
     - Bring the other producer into scope, or accept the split; ``--strict`` promotes to a failure.
   * - LHP-VAL-066
     - Warning (category sandbox) — an in-scope Python table read could not be rewritten (indirect reference).
     - Use a literal table name, or accept the shared read; ``--strict`` promotes to a failure.
   * - LHP-VAL-067
     - Warning (category sandbox) — opaque dynamic SQL could not be verified or rewritten.
     - Use a statically resolvable name, or accept the shared read; ``--strict`` promotes to a failure.
   * - LHP-VAL-902
     - All-or-nothing aggregator — one or more flowgroups failed in a parallel ``lhp generate`` run; no files were written.
     - Fix every listed failure; re-run with ``--verbose`` or ``--log-file``.
   * - LHP-VAL-DUPFG
     - Two flowgroups in the same pipeline share the same name.
     - Rename one flowgroup, or move it to another pipeline.

Input/output errors (LHP-IO)
============================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-IO-001
     - A referenced file (SQL, expectations, schema) was not found.
     - Check the path; paths are relative to the flowgroup YAML.
   * - LHP-IO-002
     - A YAML file could not be read.
     - Check the file's readability and encoding.
   * - LHP-IO-003
     - Wrong document count — expected exactly one YAML document, found another number.
     - One document per schema/expectations file; use ``---`` only in flowgroup files.
   * - LHP-IO-004
     - A YAML file could not be read by the parser.
     - Check the file's readability and encoding.
   * - LHP-IO-005
     - Permission denied reading a file.
     - Fix the file permissions.
   * - LHP-IO-006
     - The substitution file for the environment was not found.
     - Create ``substitutions/<env>.yaml``.
   * - LHP-IO-007
     - An LHP project already exists in this directory.
     - Run ``lhp init`` in an empty directory.
   * - LHP-IO-020
     - The LHP skill is already installed.
     - Uninstall first, or overwrite the existing installation.
   * - LHP-IO-021
     - The LHP skill is not installed.
     - Install the skill first.
   * - LHP-IO-022
     - ``lhp inspect-wheel`` was given a wheel path that does not exist.
     - Check the ``.whl`` path, or build it with ``lhp generate``.
   * - LHP-IO-023
     - ``lhp inspect-wheel`` target is not a ``.whl`` file.
     - Point at the built ``.whl``, not a directory or other file.
   * - LHP-IO-024
     - ``lhp inspect-wheel`` target is a corrupt zip archive.
     - Rebuild with ``lhp generate``.
   * - LHP-IO-025
     - ``--sandbox`` was used but ``.lhp/profile.yaml`` does not exist.
     - Create the personal sandbox profile.
   * - LHP-IO-026
     - ``lhp web`` was run without the webapp extras installed.
     - Install the extra: ``pip install "lakehouse-plumber[webapp]"``.
   * - LHP-IO-027
     - The ``lhp web`` port is already in use.
     - Choose another port with ``--port``.

Action errors (LHP-ACT)
=======================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-ACT-001
     - Unknown action ``type``, ``sub_type``, sink type, or preset name.
     - Fix the spelling; the message includes a "Did you mean?" suggestion.
   * - LHP-ACT-002
     - Action code generation failed.
     - Fix the action configuration named in the message.

Dependency errors (LHP-DEP)
===========================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-DEP-001
     - Circular dependency detected.
     - Break the cycle shown in the message; visualise with ``lhp dag --format dot``.
   * - LHP-DEP-002
     - Warning (never fails a run) — a Python table-read argument could not be statically resolved.
     - Declare the upstream table with ``depends_on``.
   * - LHP-DEP-003
     - Warning (never fails a run) — a SQL body could not be parsed for table extraction.
     - Fix the SQL, or declare upstreams with ``depends_on``.
   * - LHP-DEP-022
     - Circular preset inheritance detected.
     - Break the preset ``extends`` chain.

General errors (LHP-GEN)
========================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-GEN-001
     - Internal-error guard (preflight bypassed, or not exactly one built wheel for a pipeline).
     - Programming bug — see the message; report it if user-visible.
   * - LHP-GEN-901
     - A worker process failed during a parallel ``lhp generate`` run.
     - Re-run with ``--verbose`` or ``--log-file`` and report the failure.
   * - LHP-GEN-902
     - An unexpected non-LHP exception was wrapped by the CLI fallback.
     - Re-run with ``--verbose`` or ``--log-file`` and report the failure.

Deprecation warnings (LHP-DEPR)
===============================

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Code
     - Meaning / Cause
     - Resolution
   * - LHP-DEPR-001
     - Bare-braces ``{token}`` substitution syntax.
     - Use ``${token}`` (the only non-``$`` braces form is ``%{local_var}``).
   * - LHP-DEPR-002
     - The ``database`` field.
     - Use ``catalog`` and ``schema``.
   * - LHP-DEPR-003
     - The schema-transform ``enforcement`` key.
     - Remove the key.
   * - LHP-DEPR-004
     - The preset ``database_suffix`` field.
     - Use ``catalog`` and ``schema``.
