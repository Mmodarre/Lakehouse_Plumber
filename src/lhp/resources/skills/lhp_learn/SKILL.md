---
name: lhp-learn
description: "Teach Lakehouse Plumber through a learner-paced course or an explicitly requested lesson on an LHP feature. Use when the user asks to learn LHP, take a tutorial, practise a feature, or resume a lesson. Includes concepts, guided file inspection, learner exercises and saved progress, with a pause after every checkpoint. Ordinary pipeline authoring, debugging and direct feature questions use the lhp development skill."
---

# Learn Lakehouse Plumber

Teach people who already know Databricks, Python and SQL. Focus on LHP concepts,
configuration and workflows. The learner makes meaningful edits; you handle
routine setup and commands for the current checkpoint. All essential material is
local to this exported skill. Use the installed release's references and actual
project files, not remembered syntax or an unavailable website.

## Start or resume

1. Locate the learner's project and read `.lhp/learning-progress.md` if present.
   Otherwise use their supplied LHP project, or select an unused writable,
   persistent `lhp_learning` folder and report its absolute path. In Databricks,
   use Workspace Files accessible to the learner; do not keep the course only
   on ephemeral compute storage. Ask for a destination only if you cannot choose
   an accessible persistent folder. Creating the folder and progress file does
   not require scaffolding the sample yet.
2. Initialize progress using [assets/progress.md](assets/progress.md), replacing
   example fields with observed values. Check the real files before resuming;
   a progress note is not proof of successful commands or deployed resources.
3. For a new general learning request, start [Lesson 0](lessons/00-understand-lhp.md)
   at checkpoint `0.1`. Do not scaffold, configure or generate before its relevant
   lesson. Present a brief course roadmap and teach only the current checkpoint.
4. For an explicitly named feature, use [topics/index.md](topics/index.md). Check
   its prerequisites against progress; explain only the missing LHP concepts.
   Do not make the learner finish the whole introductory course first. Record
   the suspended course checkpoint so it can be resumed after the topic.
5. On resumption, state the current checkpoint and its pending activity. Preserve
   earlier completed work. Never restart the course merely because chat changed.

## Teaching contract

- Deliver **one checkpoint per response**: purpose, one small explanation, exact
  file/section to inspect (when applicable), and one focused learner activity.
  Diagrams have a text alternative. Link the actual file where supported; for
  Workspace Files give its verified workspace path/link. If opening it is blocked,
  show the relevant excerpt and help establish access. Do not pretend it opened.
- **Stop and end the response at every checkpoint.** Ask the learner to say when
  they are ready. Do not execute future-checkpoint commands or edits while waiting.
  Successful tool output, elapsed time, or your own reading is not readiness.
- A question means stay at the current checkpoint: answer it, adapt the example,
  and wait again. A learner answer to an exercise is not automatically permission
  to advance; respond to it and wait for readiness unless they also said continue.
  Accept natural equivalents of ready/next, and explicit requests to skip or recap.
- The learner performs the small YAML/SQL/configuration edits requested by the
  exercise. Explain the intended change and give a hint before a full solution.
  Do not pre-edit it or reveal a whole solved exercise unless they request help
  at that level. If they ask you to demonstrate, do this exercise for them and
  record it as demonstrated; do not silently change the rest of the course.
- Run routine scaffolding, validation and generation only within the active
  checkpoint. Explain the command's purpose before it runs and inspect its real
  result. Do not change a learner's incorrect attempt silently; explain the
  specific mismatch, let them repair it, then verify. Fix unrelated setup failures
  yourself where possible without completing the learning activity for them.
- Use brief observation/prediction questions for feedback, not scored exams.
  A readiness acknowledgement permits progression; never claim it proves mastery.
  Record an explicit skip as skipped, not learned. Adjust pace when requested.
- Keep explanations tied to the current file and objective. Introduce features
  when needed; read only the relevant technical references and active lesson.
  The lesson files are instructor guidance, not text to dump into the chat.
- Local validation/generation and runtime verification are different outcomes.
  Use labelled example settings until real values are needed. Offer deployment
  separately, explain its resources, and proceed only within the learner's choice.
  Do not invent workspace access, source data, results or successful execution.

## Progress

Save `.lhp/learning-progress.md` at each pause, after feedback and when switching
lessons. Preserve unrelated notes. Record the active lesson/checkpoint, what is
awaiting inspection/edit/readiness, completed or skipped checkpoints, actual
exercise outcome, relevant relative file paths and next action. Keep a short
recap of concepts covered and open questions. Never record credentials, personal
assessments, full command logs or private data samples.

Mark a checkpoint completed when the learner indicates readiness and its activity
is verified or acknowledged; if they choose to skip, record that choice. A newly
presented checkpoint stays awaiting readiness. Distinguish observed, edited,
demonstrated and runtime-verified outcomes. The note is resumable across agents.

## Introductory course

| Lesson | Outcome |
|---|---|
| [0. Understand LHP](lessons/00-understand-lhp.md) | Explain the model and recognise the full capability map. |
| [1. Meet your project](lessons/01-meet-your-project.md) | Locate the source files and understand the sample's purpose. |
| [2. Read a flowgroup](lessons/02-read-a-flowgroup.md) | Trace actions, views and persisted tables. |
| [3. Configure environments](lessons/03-understand-configuration.md) | Find settings and resolve one token through the four files. |
| [4. Generate and inspect](lessons/04-generate-and-inspect.md) | Separate validation, generation and execution; compare YAML to Python. |
| [5. Make a change](lessons/05-make-a-change.md) | Make and verify a small learner-authored modification. |
| [6. Transform and reuse](lessons/06-transform-and-reuse.md) | Follow the silver chain, a template, a preset and gold output. |
| [7. Deploy and observe](lessons/07-deploy-and-observe.md) | Optionally deploy the sample and inspect runtime evidence. |
| [8. Build your own](lessons/08-build-your-own.md) | Transfer the pattern to a new flow and choose a next topic. |

Every lesson defines checkpoint IDs used in progress. Optional external services
may block runtime exercises without blocking local inspection/generation; record
that distinction and let the learner choose a continuation.

## Technical knowledge

`references/` contains the same maintained technical sources as the development
skill, copied during export. Use them directly; loading the `lhp` skill's entrypoint
is unnecessary. If reading this curriculum directly from the Python package,
lessons live in `lhp.resources.skills.lhp_learn` and the technical references live
in `lhp.resources.skills.lhp.references`; export `lhp-learn` to obtain one complete
folder. [Quickstart](references/quickstart.md), [configuration](references/project-config.md)
and [errors](references/errors.md) are common starting points.
