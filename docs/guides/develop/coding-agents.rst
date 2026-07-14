===========================================
Drive Lakehouse Plumber from a coding agent
===========================================

.. meta::
   :description: Install the shipped Lakehouse Plumber skill so an AI coding agent authors flowgroup YAML from LHP's real conventions instead of guessing from stale training data.

You already build pipelines by writing flowgroup YAML and running ``lhp
generate``. When you hand that authoring to an AI coding agent — Claude Code,
Cursor, or similar — the agent is only as good as what it knows about Lakehouse
Plumber. And what a model knows from training data is whatever LHP looked like
whenever that data was collected.

Ask an ungrounded agent to write a flowgroup and it guesses: it reaches for the
deprecated ``{token}`` braces, drops the ``stream(...)`` wrapper on a SQL
transform that reads a streaming source, or suggests ``lhp show`` and
``--force`` — a command and a flag LHP removed. The YAML looks plausible and
fails ``lhp validate`` on the first run.

The idea on every page of these docs is **declare your ETL, don't hand-write
it.** This page adds a corollary: you can delegate the *declaring* to an agent —
as long as the agent is grounded in LHP's real conventions and not guessing.
LHP ships the grounding as a **skill**: a bundle of reference content, versioned
with the package, that teaches the agent the exact YAML keys, action sub-types,
and rules the CLI enforces.

Let's point a coding agent at that skill and watch the difference between a
grounded agent and a guessing one.

Before you start
================

You need Lakehouse Plumber installed and an LHP project to work in — a directory
with an ``lhp.yaml``. If you followed the Get Started course you already have
one; otherwise scaffold an empty project first.

You also need a coding agent. The ``lhp skill`` command installs the skill for
Claude Code specifically — it writes into the ``.claude/`` config directory. Any
agent that reads Markdown can use the same content; the last section covers that.

What the skill is
=================

The skill ships inside the ``lakehouse-plumber`` package, under
``src/lhp/resources/skills/lhp/``. It is two things:

- ``SKILL.md`` — the index an agent loads first. It carries the core
  architecture summary, the substitution syntax, the action-type catalogue, and
  a short list of the mistakes an agent most often makes.
- ``references/`` — forty topic-scoped Markdown files: one leaf file per action
  sub-type (thirty of them — ``actions-load-cloudfiles.md``,
  ``actions-transform-sql.md``, ``actions-write-streaming-table-cdc.md``, and so
  on) plus ten topic references (``cdc-patterns.md``, ``best-practices.md``,
  ``errors.md``, and the rest). The agent loads only the leaves it needs for the
  task in front of it.

Because the skill ships with the package, it is pinned to the LHP version you
have installed. The YAML keys, action sub-types, and error codes in the skill
match the CLI on your machine — an agent reading it cannot drift onto keys from
a version you are not running.

Install the skill for Claude Code
=================================

From your project root, run:

.. code-block:: console

   $ lhp skill install
   ✓ Installed LHP skill v0.9.1 to /Users/you/orders_project/.claude/skills/lhp
   ✓ Wrote LHP routing block to CLAUDE.md.
   Reload your Claude Code session to pick up the skill.

That copies ``SKILL.md`` and the ``references/`` directory into
``.claude/skills/lhp/`` and writes a short routing block into the project's
``CLAUDE.md``.

The routing block earns its place. A skill's triggering description can only
read your request text — it cannot see that ``lhp.yaml`` is present. So a
request phrased without LHP vocabulary, like "ingest this volume into bronze",
would not reliably route to the skill on description alone. The routing block is
project context loaded into every Claude Code session opened here, and it
supplies the missing signal — "this directory is an LHP project" — so in-project
work reaches the skill regardless of how you phrase it.

To install the skill once for every project instead of per-project, run ``lhp
skill install --user``. That writes to ``~/.claude/skills/lhp/`` and touches no
project ``CLAUDE.md``.

Check and refresh the install
=============================

Ask what state the skill is in at any time:

.. code-block:: console

   $ lhp skill status
   Install location: /Users/you/orders_project/.claude/skills/lhp
   Current LHP version: v0.9.1
   ✓ v0.9.1 (up-to-date)

The skill is pinned to the package version, so refreshing it after an upgrade is
the one habit to keep. When you run ``pip install -U lakehouse-plumber``,
``status`` flags the drift and ``lhp skill update`` re-copies the bundle so the
skill tracks the CLI again. ``lhp skill uninstall`` removes the files and strips
the routing block back out of ``CLAUDE.md``.

Ground what the agent writes
============================

The payoff shows up the moment the agent writes YAML. ``SKILL.md`` opens with
the exact errors a model trained on an older LHP tends to make, and corrects
them before the agent writes a line:

- The deprecated ``{token}`` braces become ``${token}`` — the only non-``$``
  braces syntax is ``%{local_var}``.
- A SQL transform reading a streaming source keeps its required ``stream(view)``
  wrapper instead of losing it.
- Secrets never appear inline in YAML; they resolve through ``${secret:scope/key}``.
- ``operational_metadata`` is applied at the load or transform action level, not
  at the write level.
- Removed and deprecated commands — ``lhp show``, ``lhp deps``, ``--force`` — are
  never suggested; the agent reaches for ``lhp dag`` and ``lhp diff`` instead.

Those are precisely the failures an ungrounded agent produces: plausible YAML
that ``lhp validate`` rejects. With the skill loaded, the agent reads the
correction first, so the YAML it hands you validates and generates on the first
pass. The skill also steers the agent toward reuse — it is told to scan
``templates/`` and ``blueprints/`` and propose ``use_template:`` or
``use_blueprint:`` before hand-writing fresh actions.

.. note::

   The skill and this documentation stay fact-synced: a YAML key, flag, error
   code, or default that changes is updated in both. If you ever see the skill
   and a docs page state different facts for the same feature, that is a bug —
   report it.

What you just did
=================

One command wired LHP's knowledge base into your agent. The agent now authors
flowgroup YAML from the same field tables and rules the CLI enforces — forty
reference files, pinned to your installed LHP version — instead of guessing from
training data.

You still own the output. The agent writes the YAML; you run ``lhp validate``
and ``lhp generate``, and you read the Lakeflow Python it produces. The skill
changes where the agent's YAML comes from, not who is in control of the
pipeline.

What's next
===========

- **Use a different agent.** ``lhp skill install`` targets Claude Code, but the
  skill is plain Markdown. Run the install to put the files on disk, then point
  Cursor or another tool at the same content — the installed copy under
  ``.claude/skills/lhp/``, or the bundle shipped in the package at
  ``src/lhp/resources/skills/lhp/`` — and hand it ``SKILL.md`` as the entry point.
- **Keep the skill current.** Re-run ``lhp skill update`` after every ``pip
  install -U lakehouse-plumber`` so the agent never authors against a version you
  are not running.
- **Read it yourself.** The reference content is correct for humans too, just
  denser and more list-driven than these guides. It is a fast lookup once you
  know LHP; it is not where you learn LHP from scratch.
