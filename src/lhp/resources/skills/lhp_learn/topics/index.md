# Topic lessons

Use a named topic directly. Otherwise offer two or three relevant choices. Read
only its lesson and cited technical references. Every topic follows the teaching
contract in SKILL.md: learner edits, one checkpoint per response, explicit readiness,
and progress saved in the project. Preserve the suspended introductory checkpoint.
For an existing project, map sample paths to actual equivalents; when there is no
suitable example, scaffold a separate sample at the learner's request or prepare
only the minimal example material needed for the current checkpoint.

| Request | Lesson | Useful prerequisites |
|---|---|---|
| CDC, SCD2, snapshot history | [CDC](cdc.md) | Load/write actions, generated output |
| Incremental, replace, materialized views | [Write strategies](write-strategies.md) | Flowgroups and source modes |
| Presets, templates, blueprints, multiple flowgroups | [Reuse](reuse.md) | One concrete flow and substitutions |
| Expectations, quarantine, recycling | [Quality and quarantine](quality-and-quarantine.md) | Transform chain and target tables |
| Data tests | [Tests](data-tests.md) | Views and validation/generation |
| Publishing test results | [Reporting](test-reporting.md) | One data test |
| Python transforms, loaders, custom sources | [Python](python-extensions.md) | Action boundaries |
| Environments, secrets, metadata, UC tags | [Configuration](environments-and-metadata.md) | Four configuration roles |
| Event logs, monitoring | [Monitoring](monitoring.md) | Pipeline resources |
| Personal development isolation | [Sandbox](sandbox.md) | Environment and pipeline names |
| Dependencies, jobs, CI, pipeline wheels | [Shipping](jobs-and-ci.md) | Generation and bundles |
| Other input systems or sinks | [Sources and sinks](sources-and-sinks.md) | Load/write actions |
| Browser graph/forms | [LHP Web](lhp-web.md) | The YAML/generation workflow |
| Errors, inspection commands, troubleshooting | [Diagnostics](diagnostics.md) | Accessible project |

If the requested feature has no dedicated checkpoint, use its local technical
reference to construct the same explain → inspect → learner edit → verify → pause
sequence. Say exactly which runtime capabilities cannot be demonstrated locally.
Never send an offline learner to a website as a prerequisite for continuing.
