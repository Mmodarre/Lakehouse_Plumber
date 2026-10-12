# Topic — Data tests

Outcome: distinguish a transform's row policy from a data assertion and generate
one test. Read [row count](../references/actions-test-row-count.md) and
[uniqueness](../references/actions-test-uniqueness.md); select other leaves from the
source/reference table below when needed. Pause at every checkpoint.

## tests.1 — Choose an assertion

Use the sample's nation key to explain uniqueness, or compare two appropriate
views for row-count agreement. A row-count test requires exactly two sources;
filtering rows intentionally can make equality the wrong assertion. Activity:
learner chooses a meaningful property and identifies the input dataset. Pause.

## tests.2 — Write one test action

Read the selected local reference. Let the learner append the test action to an
appropriate learning flowgroup using its actual available view/table and required
fields. They choose a descriptive name and violation policy. Provide a hint,
not a pre-written completed action. Activity: save the edit and say ready.

## tests.3 — Include tests explicitly

Inspect the attempt and validate with `--include-tests`, plus `-pc` for the sample.
Explain that ordinary validation/generation skips test actions. Pause. After
readiness generate with `--include-tests` and locate the generated expectation;
optionally compare with the prior test-free output. Do not describe generation as
passing a data test. Save the current substep and wait.

## tests.4 — Interpret the result

Inspect real runtime metrics only when an execution is requested and available.
Otherwise walk through labelled pass/fail examples. Explain the difference between
record-level rules and aggregate assertions, and offer [reporting](test-reporting.md).
Pause before another test type.

## Additional assertion references

- [Referential integrity](../references/actions-test-referential-integrity.md): missing parent records.
- [Completeness](../references/actions-test-completeness.md): missing required values.
- [Range](../references/actions-test-range.md): out-of-range values.
- [Schema match](../references/actions-test-schema-match.md): expected structure.
- [Lookup coverage](../references/actions-test-all-lookups-found.md): unmatched lookups.
- [Custom SQL](../references/actions-test-custom-sql.md) and [custom expectations](../references/actions-test-custom-expectations.md): application-specific assertions.

Use the same learner-edit/validate/generate/inspect sequence for the chosen type.
