Troubleshooting
===============

.. meta::
   :description: Symptom-based troubleshooting how-to for Lakehouse Plumber — fix common generation, validation, deployment, and editor issues.

Use this page when an LHP command does not behave as expected. Each section starts
from a **symptom** you can observe and walks through the most likely causes and
fixes. For the full catalog of error codes (``LHP-CFG-*``, ``LHP-VAL-*``,
``LHP-IO-*``, ``LHP-ACT-*``, ``LHP-DEP-*``), see :doc:`errors_reference`.

Every LHP error printed to your terminal already includes a description, context,
numbered fix suggestions, and a configuration example. Read the error carefully
before reaching for any of the recipes below.

.. tip::

   Run any command with ``--verbose`` (or ``-v``) for stack traces and internal
   decisions. Use this whenever the surface error does not tell you enough.

``lhp generate`` fails with an error code
-----------------------------------------

Symptom: ``lhp generate --env <env>`` aborts with a banner like
``❌ Error [LHP-CFG-009]: YAML parsing error``.

Do this:

1. Read the **Error code** in the banner. The prefix tells you the category:
   ``CFG`` = configuration, ``VAL`` = validation, ``IO`` = file path, ``ACT`` =
   unknown type, ``DEP`` = dependency cycle.
2. Apply the **numbered fix suggestions** shown under ``💡 How to fix``. The
   error includes a before/after example when applicable.
3. If the cause is still unclear, look up the code in :doc:`errors_reference`
   for the same fix written as documentation, plus extra context and common
   causes.
4. Re-run ``lhp generate --env <env>``. Generation is incremental; only the
   affected FlowGroup is rebuilt.

Common high-impact errors:

- ``LHP-CFG-009`` — Quote any value that contains ``:`` ``{`` ``}`` ``[`` or
  ``]``.
- ``LHP-VAL-001`` — Add the missing ``source`` / ``target`` / ``type`` field
  named in the error.
- ``LHP-ACT-001`` — Fix the spelling of an action ``type``, ``sub_type``, or
  preset name. The error includes "Did you mean?" suggestions.
- ``LHP-IO-001`` — Check that the referenced ``sql_file``, expectations file,
  or schema file exists at the path shown. Paths resolve relative to the
  FlowGroup YAML.

``lhp validate`` reports errors you do not understand
-----------------------------------------------------

Symptom: ``lhp validate --env <env>`` lists multiple ``LHP-VAL-*`` codes for the
same action, or the message text is opaque.

Do this:

1. Address one error at a time, starting with the **first** error printed.
   Later errors often disappear once the earliest cause is fixed.
2. Read the ``📍 Context`` block — it tells you the offending action name and
   the field that triggered the error.
3. Look up the specific code in :doc:`errors_reference`. The reference page
   has the same before/after YAML you saw in the terminal, formatted for
   reading.
4. Use ``lhp show <flowgroup> --env <env>`` to print the **resolved**
   configuration after presets, templates, and substitutions have been
   applied. Most validation errors are easier to diagnose against the
   resolved YAML, not the source.

If the error mentions ``✗`` markers (``LHP-VAL-002``), the action has multiple
problems — fix each ``✗`` item, not just the first one.

My pipeline deploys but does not run
------------------------------------

Symptom: ``databricks bundle deploy`` succeeds, but the pipeline does not
appear, fails to start, or runs the wrong code.

Do this:

1. Confirm generation happened in the **same environment** as the deployment.
   Run ``lhp generate --env <env>`` before ``databricks bundle deploy
   --target <env>``. The ``--env`` and ``--target`` values must match.
2. Check ``resources/lhp/`` for the pipeline resource file. If it is missing,
   the FlowGroup was not regenerated. Force a rebuild:

   .. code-block:: bash

      lhp generate --env dev --force

3. If you see ``LHP-CFG-022`` (missing ``databricks.yml``) or ``LHP-CFG-023``
   (substitution file has no matching target), the bundle is not wired to
   your substitutions. Add the missing target to ``databricks.yml`` or pass
   ``--no-bundle`` to suppress bundle generation entirely.
4. Confirm the deployed Python file matches your source. Open the pipeline
   in Databricks and compare the notebook content against ``generated/``
   in your repo.

IntelliSense or YAML completion is not working
----------------------------------------------

Symptom: Your editor does not autocomplete FlowGroup fields, suggest action
types, or highlight invalid values in ``pipelines/*.yaml``.

Do this:

1. Verify your editor has a YAML language server (the VS Code YAML extension
   or equivalent) installed and active.
2. Check that schema mapping is configured for ``pipelines/*.yaml``,
   ``presets/*.yaml``, ``templates/*.yaml``, and ``substitutions/*.yaml``.
   The JSON Schemas LHP ships with live under
   ``src/lhp/schemas/`` in the installed package.
3. Reload the editor window after installing or updating LHP — language
   servers cache schema paths and do not always detect new schemas
   automatically.

For full editor wiring (schema URLs, settings.json snippets, and
language-server configuration), see :doc:`editor_setup`.

My substitutions are not being resolved
---------------------------------------

Symptom: Generated Python or YAML still contains ``${token}``, ``%{local_var}``,
or ``${secret:scope/key}`` literally — the value was not substituted.

Do this:

1. Confirm the substitution syntax. LHP recognises four forms, in this order:

   - ``%{local_var}`` — local variables defined in the same FlowGroup.
   - ``{{ template_param }}`` — Jinja2 template parameters.
   - ``${env_token}`` — environment tokens from ``substitutions/<env>.yaml``.
   - ``${secret:scope/key}`` — Databricks secret references.

   The bare-braces form ``{token}`` is **deprecated**. Always use ``${token}``
   in code, docs, and examples.

2. List the tokens LHP knows about for your environment:

   .. code-block:: bash

      lhp substitutions --env dev

   If the token is missing, add it to ``substitutions/dev.yaml`` (or the
   equivalent file for your target environment).

3. Check that the token appears inside a **string** value in the YAML. LHP
   does not substitute tokens inside YAML keys.

4. For secrets, confirm the scope and key exist in the Databricks workspace
   you deploy to. LHP does not validate secrets at generation time.

If you see a token that should resolve but does not, run ``lhp show
<flowgroup> --env <env>`` to inspect the resolved configuration. Tokens that
fail to resolve appear unchanged in the output.

My presets, templates, or blueprints did not pick up changes
------------------------------------------------------------

Symptom: You edited a preset, template, or blueprint, but ``lhp generate`` does
not include the change in the generated Python.

Do this:

1. LHP tracks generated files in ``.lhp_state/`` (per-pipeline JSON shards
   as of 0.9.0; pre-0.9 projects had a monolithic ``.lhp_state.json`` that
   auto-removes after the first 0.9 successful run). LHP only regenerates
   FlowGroups whose **content checksum** has changed. Edits to presets or
   templates referenced by a FlowGroup do trigger regeneration of dependent
   FlowGroups — but only on the next ``lhp generate`` run.
2. If state tracking is out of sync (you deleted generated files manually,
   for example), force a full rebuild:

   .. code-block:: bash

      lhp generate --env dev --force

3. To rebuild from a clean slate, delete the state directory:

   .. code-block:: bash

      rm -rf .lhp_state    # (0.9+; pre-0.9 use `rm .lhp_state.json`)
      lhp generate --env dev

   This regenerates every FlowGroup. Use it when state file corruption is
   suspected.

4. Confirm the preset or template is actually referenced. ``lhp show
   <flowgroup> --env <env>`` prints the resolved configuration after preset
   merge and template expansion — your changes should appear there.

I changed code but ``lhp generate`` says nothing to do
------------------------------------------------------

Symptom: You edited a FlowGroup YAML, a SQL file, or a schema file, but
``lhp generate --env <env>`` reports no work and skips the file.

Do this:

1. Confirm the file is **included** by your project's include patterns. The
   ``include`` field in ``lhp.yaml`` filters which FlowGroups LHP discovers.
   Run ``lhp validate --env <env>`` — it lists every FlowGroup it found.
2. If the file is included but unchanged on disk (for example, you saved
   without modifying content, or the change is in a referenced ``.sql``
   file LHP does not checksum), use ``--force`` to bypass the state check:

   .. code-block:: bash

      lhp generate --env dev --force

3. Use a dry run to see exactly what LHP would generate and why:

   .. code-block:: bash

      lhp generate --env dev --dry-run --verbose

   The verbose output shows which FlowGroups are skipped and the reason.

4. If you suspect a stale state file, inspect it directly:

   .. code-block:: bash

      lhp state --env dev

   The output lists tracked files, their checksums, and any orphaned or
   stale entries.

.. note::

   ``lhp generate`` does not detect changes to files referenced **indirectly**
   in every case — for example, an external SQL file loaded via ``sql_file``.
   Use ``--force`` after editing such files.

See also
--------

- :doc:`errors_reference` — exhaustive catalog of every LHP error code with
  cause, fix, and example.
- :doc:`architecture` — how generation, state tracking, and substitution
  resolution work internally.
- :doc:`editor_setup` — JSON schema wiring for YAML completion and
  validation in your editor.
