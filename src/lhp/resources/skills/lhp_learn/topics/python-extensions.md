# Topic — Extend LHP with Python

Outcome: connect a Python module and its parameters to generated code. Read
[Python transform](../references/actions-transform-python.md),
[Python load](../references/actions-load-python.md) and
[custom sources](../references/actions-load-custom-datasource.md) as relevant.
Wait after each checkpoint; existing Python experience is assumed.

## python.1 — Inspect the boundary

Open the sample's `orders_clean.yaml` Python action and
`transforms/normalize_tz.py`. Match module path, function name, source views and
parameters to the actual signature. Activity: locate where the timezone value
comes from and where the function returns its DataFrame. Pause.

## python.2 — Change one parameter

Let the learner choose another valid timezone and edit the corresponding
substitution in an isolated learning copy. Ask them to predict which generated
call changes while the reusable function remains the same. They save the edit;
you inspect it and validate. Pause before generation.

## python.3 — Inspect copying and invocation

Generate after readiness and locate the copied Python module and its invocation.
Explain that local generation checks configuration/copying but does not execute
Spark transformations. For helper packages, read the local-helper section of
[advanced](../references/advanced.md) and inspect the actual copied hierarchy.
Activity: follow one parameter into the generated call. Pause.

## python.4 — Explore another extension point

If requested, use a Python loader or custom data-source leaf to identify its
contract. Let the learner adapt a minimal example in a separate flowgroup using
actual source requirements; do not substitute the transform signature for a loader
or custom source. Validate, pause, then generate and inspect. Record any unavailable
runtime dependency or connection and offer a return to the course. Pause.
