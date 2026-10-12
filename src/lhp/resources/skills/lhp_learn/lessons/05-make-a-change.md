# Lesson 5 — Make and verify a change

Prerequisite: successfully inspected generated code. Outcome: complete one small
edit → validate → generate → inspect cycle. The learner makes the edit. For an
existing project choose an equivalent appropriate field in an isolated learning
copy if needed; do not modify unrelated production definitions. Pause each step.

## 5.1 — Choose and predict the change

Open `pipelines/03_gold/sales_by_nation.yaml`. Explain briefly that this write
creates the gold query result and supports a descriptive `write_target.comment`.
Ask the learner to choose a short description and predict where it will appear
in generated Python. Point to the containing mapping without writing the answer.
Activity: learner states or considers the prediction. Pause.

## 5.2 — The learner edits

Ask them to add the chosen `comment` under `write_target`, preserving the other
fields and YAML indentation. Give a targeted hint if needed; show a completed
fragment only if requested. Do not use a tool to make this edit for them.
Activity: they save the file and say ready. Keep the checkpoint awaiting their edit.
If they request a demonstration, perform it and label the outcome demonstrated.

## 5.3 — Check the saved attempt

After readiness, reread the file and inspect the change. Explain any nesting or
syntax issue and let the learner fix it. Run the sample's validation command once
the attempt is ready. Activity: inspect the validation result. Pause before generation.

## 5.4 — See the effect

Regenerate with the same environment and pipeline configuration. Inspect the gold
Python and find the chosen description; compare with the previous generated content
where available. A local text change is the observed result, not a changed deployed
table. Activity: learner connects their YAML edit to the output. Ask where they
would make the next durable change, give feedback, and wait before
[Lesson 6](06-transform-and-reuse.md).
