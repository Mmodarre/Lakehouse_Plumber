==============================
Coding agents and the skill
==============================

.. meta::
   :description: What Lakehouse Plumber's agent-facing skill is, why LHP ships it as a separate artifact from this site, and how the two hold one set of facts in two forms.

Lakehouse Plumber writes for two audiences. One is you, reading this site:
prose, worked examples, a path from your first pipeline to production. The other
is an AI coding agent — Claude Code, Cursor, or similar — that you ask to author
flowgroups on your behalf. Those two readers need the same facts in incompatible
forms, so LHP ships them twice: this documentation for you, and a condensed,
machine-oriented **skill** for the agent.

This page is the mental model — what that skill is and why it exists as its own
artifact. To install it and point an agent at it, see the develop guide "Drive
Lakehouse Plumber from a coding agent."

An agent is only as current as its training data
================================================

Ask a coding agent to write a flowgroup and, ungrounded, it writes from whatever
Lakehouse Plumber looked like when its training data was collected. That snapshot
is stale by construction: it predates the version you have installed. The agent
reaches for a deprecated token syntax, drops a wrapper the SQL transform
requires, or suggests a command LHP has removed. The YAML looks right and fails
on the first ``lhp validate``.

The antithesis is an agent grounded in LHP's real conventions against one
guessing at them. A grounded agent authors from the exact YAML keys, action
sub-types, and rules the installed CLI enforces; a guessing agent hallucinates
fields that never existed or lapsed releases ago. The skill is that grounding. It
ships inside the ``lakehouse-plumber`` package, so it is pinned to the version on
your machine — the agent cannot drift onto keys from a release you are not
running.

The line every page of these docs draws is the same: **declare your ETL, don't
hand-write it.** The skill adds a corollary — you can delegate the declaring to an
agent, as long as the agent declares from LHP's conventions and not from memory.

Why a declarative surface suits an agent
========================================

LHP's authoring surface is a small, typed, declarative YAML vocabulary — a fixed
set of load, transform, write, and test actions with named fields — rather than
open-ended PySpark. That shape is what makes agent authoring tractable:

- **Small.** The whole surface is a catalogue of action sub-types and their
  fields, not an unbounded API. It fits into an agent's context as a set of field
  tables.
- **Typed.** Every field has a name, a type, and a place in the flowgroup. An
  agent filling a known slot has far less room to invent than one writing
  free-form Spark.
- **Verifiable.** ``lhp validate`` and ``lhp generate`` are a hard check on the
  output. An agent's YAML either resolves and generates or it does not — you never
  take it on faith. Hand-written PySpark has no equivalent gate short of running
  it on a cluster.

Grounding an agent in hand-written Spark boilerplate would be far harder: there
is no bounded vocabulary to teach and no cheap way to check the result. The
declarative surface is what makes the skill worth shipping in the first place.

What the skill contains
=======================

The skill lives in the package at ``src/lhp/resources/skills/lhp/``. It is an
index plus a set of leaf references:

- ``SKILL.md`` — the file an agent loads first. It carries the core architecture,
  the substitution syntax, the action-type catalogue, and the mistakes an agent
  most often makes, so the correction lands before the agent writes a line.
- ``references/`` — forty topic-scoped Markdown files: thirty leaves, one per
  action sub-type, and ten topic references such as CDC patterns, best practices,
  and error codes. The agent loads only the leaves the task in front of it needs,
  keeping its context small.

The reference files are dense and list-driven, built for a token budget: field
tables, key rules, minimal examples. They carry the same LHP facts as this site,
shaped for a reader that parses headings as anchors and rarely follows a link
out.

Same facts, different form
==========================

The skill and this site describe one product, so they must never contradict each
other — yet they are free to sound nothing alike. That is the dual-audience
contract, and it has two halves.

**The facts stay in sync.** When a CLI flag, a YAML key, an error code, an action
sub-type, or a default changes, both the affected page here and the affected
skill reference change together. If a page and the skill state different facts for
the same feature, that is a bug to report, not a nuance to reconcile.

**The voice is free to diverge.** This site explains a flag's rationale across a
paragraph; the skill compresses the same fact into a table row. A human reader
wants the narrative; an agent on a token budget wants the row. Holding both to
one voice would force one audience to read content shaped for the other, so LHP
keeps them apart on purpose.

.. note::

   The skill is correct for humans too — you can open the Markdown and read it —
   but it is denser and more list-driven than these guides. It is a fast lookup
   once you know Lakehouse Plumber, not the place to learn it from scratch. This
   site is.

Where to go from here
=====================

- **Install the skill and use it.** The develop guide "Drive Lakehouse Plumber
  from a coding agent" covers putting the skill in front of Claude Code and any
  other Markdown-reading agent, and shows the difference between a grounded agent
  and a guessing one.
- **Keep it current.** Because the skill is pinned to your installed version,
  refresh it after you upgrade Lakehouse Plumber so the agent never authors
  against a version you are not running.
