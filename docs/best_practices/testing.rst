Testing
=======

.. meta::
   :description: Reasoning behind LHP testing patterns — expectations versus test actions, layered CI, dry-run baselines, and E2E pipeline regression patterns.

For the step-by-step procedure for setting up a CI/CD pipeline, see
:doc:`../cicd`. This page explains the testing approaches LHP supports and
when each one is the right tool.

Three kinds of test, not one
----------------------------

LHP supports three test mechanisms that catch three different classes of
bug. Confusing them — or treating any one as a substitute for the others —
is the common testing mistake.

:term:`Expectations <Expectation>` (``data_quality`` transforms) are row-level invariants
checked at runtime by the :term:`DLT` engine. They generate ``@dp.expect_all``,
``@dp.expect_all_or_drop``, or ``@dp.expect_all_or_fail`` decorators.
They run on every row of every batch and surface in the DLT event log.

**Test actions** (the ``test`` action type with subtypes like
``row_count``, ``uniqueness``, ``referential_integrity``) generate
SQL-based validation views. They are evaluated after the main pipeline
runs and check table-level properties — counts match, joins resolve,
lookups complete. They are opt-in: ``lhp generate --include-tests``
generates them; without the flag they are absent.

**Generation tests** (``lhp validate``, ``lhp generate --dry-run``,
unit tests around the code generator) check that the YAML configs
produce the expected Python code. They run in CI before deployment and
have nothing to do with the data.

Each layer catches different failure modes. Expectations catch bad
rows. Test actions catch bad tables. Generation tests catch bad
configs. Pipelines that pass all three are much more likely to behave
in prod.

Why DQE tiering follows the medallion model
-------------------------------------------

LHP supports three expectation tiers: ``warn`` (record metric, keep
row), ``drop`` (drop bad rows), ``fail`` (stop pipeline). The standard
medallion mapping is:

- **Bronze: ``warn`` only.** Raw data is precious, even when imperfect.
  ``expect_or_fail`` at bronze means one corrupt record stops ingestion
  of every subsequent record from the same source. The cost — paused
  ingestion until someone investigates — almost never justifies the
  benefit at bronze.
- **Silver: ``drop`` for structural rules.** Silver is where you commit
  to a schema contract for downstream consumers. Rows that violate
  structural rules (null primary keys, malformed timestamps) do not
  propagate. Pair this with a :term:`quarantine <Quarantine>` table to retain the dropped
  rows for investigation — see :doc:`../quarantine_records`.
- **Gold: ``fail`` on critical invariants.** Gold tables back reports
  and dashboards. A referential-integrity violation that propagates to
  gold can corrupt months of reporting before anyone notices. ``fail``
  at gold means you find out immediately.

The naming convention for expectations matters because the names show
up in the DLT Data Quality tab and event log. ``valid_<column>_<constraint_type>``
— ``valid_order_id_not_null``, ``valid_amount_positive`` — gives you
something useful to grep for when a failure surfaces.

External expectation files keep DQE reusable and reviewable. Store
them in ``expectations/<system>/<layer>/`` so that quality rules can
be reviewed independently of pipeline logic and reused across
FlowGroups. The same null-check rule that applies to bronze
``raw_orders`` probably applies to bronze ``raw_returns``.

Why test actions catch what expectations miss
---------------------------------------------

Expectations are row-by-row. They cannot answer "do we have the right
number of rows?" or "does every foreign key in this table resolve in
the lookup?". Those questions are table-level and need a different
mechanism.

Test actions fill that gap. The nine subtypes (``row_count``,
``uniqueness``, ``referential_integrity``, ``completeness``,
``range``, ``schema_match``, ``all_lookups_found``, ``custom_sql``,
``custom_expectations``) generate SQL views that compute the
table-level metric and assert against an expected value. The views
run after the main pipeline completes; their results are visible in
the DLT event log and can be published to external systems via
``actions/test_reporting``.

The expected use is "run them in staging before production
deployment". Test actions are typically too expensive to run in
production every batch — a ``referential_integrity`` check over a
billion-row table is not free — but they are cheap enough to run in
staging on representative data. A pipeline that passes its test
actions in staging is much less likely to surprise you in prod.

CI layering for fast feedback
-----------------------------

The CI pipeline for an LHP project benefits from layering — cheap
checks first, expensive checks last, so problems surface as quickly
as possible:

.. list-table::
   :header-rows: 1
   :widths: 15 35 30

   * - Layer
     - What it checks
     - Tool
   * - Syntax
     - Valid YAML, indentation
     - ``yamllint``
   * - Schema
     - Required fields, correct types
     - JSON Schema validators against ``src/lhp/schemas/``
   * - Semantic
     - References resolve, no circular deps, parameters present
     - ``lhp validate --env <env>``
   * - Generation
     - Config generates valid Python
     - ``lhp generate --dry-run --env <env>``
   * - Regression
     - No unintended diff against committed baseline
     - Baseline comparison
   * - Functional
     - Table-level assertions pass
     - Pipeline run with ``--include-tests``

The order matters. A YAML syntax error is the cheapest failure to
diagnose; you do not want it surfacing at the generation step. A
generation error is the next cheapest. A test action failure is the
most expensive to investigate because it depends on data state, so it
goes last.

Each layer is independent — a project can adopt them incrementally —
but the value compounds. A project running all six catches every
common class of LHP failure before it hits production.

Dry-run baselines as snapshot testing
-------------------------------------

The regression layer is the LHP equivalent of snapshot testing. Run
``lhp generate --dry-run`` against a known-good environment, commit
the output as a baseline, and have CI re-run and diff on every PR.
Any unexpected diff is flagged for review.

The reason this catches a class of bug that ``lhp validate`` cannot
is that LHP's generator does deep merging across presets and
templates. A change to a preset can produce a different generated
Python file for a FlowGroup that was not touched in the PR. Schema
validation says the FlowGroup is valid; the only way to surface the
behavioural change is to compare the actual generated code.

The trade-off is baseline maintenance. Every legitimate change to
generator behaviour requires updating the baselines. The fix is to
treat baseline updates as a deliberate step — generate, inspect the
diff, commit if expected. A PR that updates baselines without
explaining why should fail review.

The LHP repository uses this pattern itself for E2E testing
(``tests/e2e/fixtures/testing_project/`` against baselines under
``monitoring_baseline/`` and ``resources_baseline/``). Every E2E
test generates code from the fixture project and diffs against a
committed baseline. The same approach works for application
projects.

Why ``lhp validate`` before ``lhp generate`` is non-negotiable
--------------------------------------------------------------

Generation errors are harder to diagnose than validation errors.
Validation runs the structural and semantic checks; the error
messages point at the offending FlowGroup, action, or field, with
fuzzy-match suggestions for unknown fields. Generation runs after
validation and assumes a valid config; its errors usually surface
deep in the template-expansion or code-emission path, with
stack traces rather than friendly messages.

The order is intentional. ``lhp validate`` exists as a fast,
diagnose-friendly check; ``lhp generate`` exists to produce code,
not to diagnose problems with the config. Running generate without
validating first means you optimise for the wrong failure mode.

The recommendation is to wire ``lhp validate`` as a blocking CI
check on every PR. The check takes seconds, catches the largest
class of config errors, and produces actionable error messages.
``lhp validate`` now runs its checks — including cross-flowgroup
conflict detection — on the **resolved** flowgroups (after presets,
templates, and substitutions are applied), so a conflict introduced
by resolution (for example, a template that expands into two
``create_table: true`` actions targeting the same table) is caught
by ``validate`` rather than slipping through to ``generate``.
Anything that gets past it is either a real bug in LHP or a class
of error that only the generator catches — both of which deserve
investigation.

Schema enforcement at the bronze-to-silver boundary
---------------------------------------------------

LHP's ``schema`` transform with ``enforcement: strict`` rejects
unexpected columns. At the bronze-to-silver boundary this acts as a
contract: silver only accepts the columns it declares, and any
bronze schema drift surfaces as a generation-time or runtime error
rather than a silent extension of the silver schema.

The combination with expectations is what makes silver schemas
trustworthy. Schema enforcement ensures the column set is what you
declared; expectations ensure the row content meets your rules.
Together they give downstream consumers a defensible contract.

Anti-patterns
-------------

**Skipping ``lhp validate`` before ``lhp generate``.** Generation
errors are much harder to diagnose than validation errors. The
validate step costs seconds and surfaces the most common bugs with
clear messages.

**``expect_or_fail`` at bronze.** One bad row stops the entire
ingestion. The cost of paused ingestion almost never justifies the
benefit at bronze.

**No regression baselines.** A preset change that silently alters
generated code for an unrelated FlowGroup is invisible without
baseline diffs. The cost of baselines is real (you have to maintain
them), but it is lower than the cost of the bug they catch.

**Testing only in production.** Test actions in particular are
designed for staging. Running them only after deployment defeats
their purpose; the staging run is the cheap iteration loop.

See also
--------

- :doc:`../cicd` for the step-by-step CI/CD setup.
- :doc:`../quarantine_records` for retaining dropped rows from
  silver-layer ``drop`` expectations.
- :doc:`../actions/test_actions` for the full test-action reference.
- :doc:`../actions/test_reporting` for publishing test results to
  external systems.
