# Topic — Diagnose an LHP problem

Outcome: use configuration context and a specific error to find a repair. Read
[errors](../references/errors.md) and [configuration](../references/project-config.md).
Use the learner's real error, or an isolated learning copy with one learner-created
unresolved token. Never break an existing shared project for a demonstration.
Pause after every checkpoint.

## diagnostics.1 — Identify the failing stage

Locate whether the failure occurred at installation, validation, generation,
deployment or execution. Read the exact error and relevant local error-code entry.
Activity: learner identifies the implicated file/field. Avoid claiming an external
runtime failure can be solved merely by passing local validation. Pause.

## diagnostics.2 — Inspect the cause

Open only the relevant files and follow the reference/token/field to its definition.
For an unresolved-token exercise, compare its spelling with the environment mapping.
Activity: learner proposes a small repair. Give a hint before the answer. Pause.

## diagnostics.3 — Apply and verify

The learner saves the repair. Reread the file, run the previously failing local
command, and inspect its actual result. If the symptom remains, stay at this
checkpoint and explain the next diagnostic observation. Pause before generating or
running any additional stage. Record the observed fix, not an inferred runtime cure.

## diagnostics.4 — Find supporting tools

Use `lhp --help` and the actual inspection subcommand help to show a relevant
project-info, stats, diff or substitution view. Explain telemetry controls if the
learner asks about reporting; do not change their preference automatically. Activity:
learner chooses the command that answers their question. Save and pause.
