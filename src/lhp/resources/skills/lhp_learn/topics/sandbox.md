# Topic — Personal development sandbox

Outcome: inspect scoped, namespaced outputs while understanding remaining shared
reads. Read [sandbox](../references/sandbox.md). Use a learning copy: sandbox
generation replaces output with its selected scope. Pause every checkpoint.

## sandbox.1 — Define the boundary

Show read-shared/write-own with two related sample pipelines. The namespace changes
produced table names within the chosen scope; unrelated upstream reads remain
shared. It is not an access-control mechanism. Activity: learner predicts which
bronze read stays shared if only silver is in scope. Pause.

## sandbox.2 — The learner creates a profile

Let them choose a valid namespace and actual pipeline scope, then write
`.lhp/profile.yaml` under `sandbox:` following the local example. Do not infer their
namespace from an identity. Check the project's optional sandbox policy.
Activity: learner saves the profile and identifies the matched pipelines. Pause.

## sandbox.3 — Validate the scope

Run validation with `--sandbox`, environment and bundle pipeline config; do not
combine it with `--pipeline`. Explain unmatched-scope errors or warnings using the
actual output and let the learner fix their profile. Activity: inspect the selected
scope. Pause before generation.

## sandbox.4 — Inspect renamed output

Generate with the same sandbox options. Compare one produced table, its in-scope
reader and one shared upstream read with prior normal output. Explain Python/SQL
rewrite warnings if present; monitoring is skipped in this mode. Activity: learner
checks the intended boundary. Save results and pause before deployment or returning
to ordinary generation; do not silently overwrite the output for the next lesson.
