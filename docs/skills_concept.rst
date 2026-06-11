.. meta::
   :description: What Lakehouse Plumber skills are, who they are written for, and why the AI-facing skill content is allowed to drift in voice from the human-facing docs.

Using Coding Agents
===================

Lakehouse Plumber (LHP) ships a Markdown "skill" alongside the Python package.
The skill is reference content authored for AI coding agents — Claude Code,
Cursor, and similar tools — that need to read LHP knowledge into context
while they help you build pipelines. This page explains what the skill
contains, who it is for, why it exists as a separate artifact, and how its
content relates to the documentation you are reading now.

If you want to install or update the LHP skill in your AI agent setup, see
:doc:`cli` (the ``lhp skill`` command reference). For where the skill sits in
the wider system, see :doc:`architecture`.

What an LHP skill is
--------------------

An LHP skill is a directory of Markdown files. At install time the ``lhp
skill`` command copies the bundle out of the Python package into
``.claude/skills/lhp/`` (project-local) or ``~/.claude/skills/lhp/``
(user-global). The layout is fixed:

- ``SKILL.md`` — the index that an AI agent loads first. It defines the
  skill's name, triggering description, core architecture summary, and a
  catalogue of the reference files.
- ``references/`` — forty topic-scoped Markdown files — one leaf file per
  action sub-type plus ten topic references — each tuned to one area of LHP.
  Agents load only the references they need for the task in front of them.
- ``.lhp_skill_version`` — a marker file written by ``lhp skill install`` that
  records which LHP version produced the installed bundle.

A project-local install additionally writes a short routing block into the
project's ``CLAUDE.md`` (removed again by ``uninstall``). A skill's triggering
description can only read the user's request, not the filesystem, so a request
phrased without LHP terms would otherwise miss the skill; the routing block
supplies the missing signal — "this directory is an LHP project" — so in-project
work routes to the skill regardless of phrasing. A ``--user`` install is global
and writes no project ``CLAUDE.md``.

The skill ships inside the ``lakehouse-plumber`` wheel under
``src/lhp/resources/skills/lhp/``. Versioning the skill with the package
guarantees that the YAML keys, action types, and error codes referenced in
the bundle always match the CLI the user actually has installed.

Who the skill is for
--------------------

An AI coding agent is the primary reader. When a developer asks Claude Code
to "add a CDC silver layer" inside an LHP repository, the agent loads
``SKILL.md`` to orient, then pulls in ``references/cdc-patterns.md`` and
``references/actions-write-streaming-table-cdc.md`` for the relevant syntax.
The agent writes YAML based on that context.

Humans are a secondary reader. You can open the same Markdown directly,
and the content is correct, but the prose is denser and more list-driven
than the docs you are reading now. The skill is not the place to learn LHP
from scratch — the :doc:`quickstart` and the explanation chapters of this
site are.

Why the skill is a separate artifact
------------------------------------

The skill exists because RST documentation and AI-agent context have
incompatible optimisation targets.

LHP documentation is read by humans on a docs site. It uses full prose,
narrative ordering, admonitions, diagrams, and cross-page links. It assumes
the reader can navigate, scroll back, and follow a "see also". Sphinx
toolchain features — ``:doc:`` references, ``.. mermaid::`` diagrams,
``versionchanged`` directives — exist precisely to support that mode of
reading.

An AI agent consumes Markdown differently. It loads a file into a context
window with a strict token budget, parses headings as anchors, and rarely
follows a link out. It prefers tables, dense field references, and short
imperative rules. Narrative paragraphs that help a human reader inflate the
token cost without improving the agent's output. The files under
``references/`` reflect that — each is a flat list of action sub-types,
field tables, key-rule bullets, and minimal worked examples.

Maintaining one source for both audiences would force one of them to read
content optimised for the other. The skill exists so neither has to.

The drift policy
----------------

LHP accepts deliberate drift between ``docs/`` and the skill Markdown, under
rule 42 of the documentation style guide. The policy has two parts.

**Factual content stays in sync.** When a CLI flag, a YAML key, an error
code, an action sub-type, or a default value changes, the writer updates
both the affected RST page and the affected skill Markdown in the same
change set. A reviewer who finds the RST page describes ``--include-tests``
but the skill index does not is justified in blocking the change. Fact
parity is checked file by file, not by diffing prose.

**Voice may differ.** The RST page may explain the rationale for the flag
across two paragraphs. The skill Markdown may compress the same fact into a
table row. That is by design. Reviewers do not flag the prose-vs-table
difference as a defect; they flag only mismatches that would lead either
audience to a wrong configuration.

The practical consequence: when you read both documents for the same
feature, you should see the same facts, possibly the same examples, and
different surrounding tone. If you see different facts, that is a bug —
report it.

Skill content overview
----------------------

The references directory currently contains forty files: thirty
per-sub-type action references and ten topic references. Each maps to a
domain the agent might be asked to work in:

- ``actions-<action>-<sub-type>.md`` — thirty leaf files, one per action
  sub-type (for example ``actions-load-cloudfiles.md``,
  ``actions-transform-sql.md``, ``actions-write-streaming-table-cdc.md``,
  ``actions-test-uniqueness.md``), each with the sub-type's required and
  optional fields.
- ``cdc-patterns.md`` — CDC and SCD Type 2 patterns for Delta CDF,
  PostgreSQL Write-Ahead Log, and snapshot Change Data Capture (CDC)
  sources.
- ``templates-presets.md`` — Template structure, parameter syntax, preset
  matching, and merge behaviour.
- ``blueprints.md`` — whole-flowgroup patterns instantiated per site,
  region, or tenant, and the ``use_blueprint:`` syntax.
- ``quickstart.md`` — first-project setup, from ``lhp init`` through the
  first validate and generate run.
- ``project-config.md`` — ``lhp.yaml``, substitutions, local variables,
  operational metadata, and CLI commands.
- ``sandbox.md`` — developer sandbox mode (``--sandbox``): the personal
  profile, team policy configuration, and table-rename semantics.
- ``advanced.md`` — Declarative Automation Bundles integration, dependency
  analysis, multi-job orchestration, and Continuous Integration /
  Continuous Deployment (CI/CD) patterns.
- ``monitoring.md`` — event log injection, monitoring pipeline, and the
  ``__eventlog_monitoring`` alias.
- ``errors.md`` — every LHP error code (``LHP-CFG``, ``LHP-VAL``,
  ``LHP-IO``, ``LHP-ACT``, ``LHP-DEP``) with causes and fixes.
- ``best-practices.md`` — the BP-1 through BP-20 enterprise defaults and
  the anti-patterns to avoid.

Each reference file has a documentation counterpart on this site. The
mapping is not one-to-one — a single RST page may cover material that the
skill splits across two reference files, or vice versa — but every fact
appears in both places.
