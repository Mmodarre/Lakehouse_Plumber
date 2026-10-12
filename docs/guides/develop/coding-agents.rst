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

For a guided setup, copy the complete `build prompt <../../agent.md>`_ or
`learning prompt <../../learn.md>`_ into your coding agent. Both include their
own installation instructions; website access is not required by the agent.
The learning prompt selects the separate ``lhp-learn`` skill. It assumes
Databricks, Python and SQL experience and begins with Lesson 0: concepts and a
short overview of each capability. It then guides file inspection and learner-led
edits one checkpoint at a time, waiting for readiness before continuing. Progress
is saved in ``.lhp/learning-progress.md`` in the learning project.

If installing manually, use the routes below. Claude project installation needs
an ``lhp.yaml``; portable export and Genie installation work before scaffolding.

Two skills, one package
=======================

Both skills ship with the same ``lakehouse-plumber`` release and work offline:

- ``lhp`` handles development: authoring, configuration, debugging, validation,
  generation and direct technical explanations. It contains no course lessons.
- ``lhp-learn`` handles explicit teaching requests: Lesson 0, the guided sample
  course, learner exercises, saved progress and advanced topic lessons. It ends
  each checkpoint and waits for the learner to continue.

Their ``SKILL.md`` entrypoints select different behaviours. Technical references
are maintained once under ``src/lhp/resources/skills/lhp/references/`` and copied
into each exported skill. The curriculum source is under
``src/lhp/resources/skills/lhp_learn/``. An exported teaching skill is self-contained
and does not need the development skill installed or loaded.

The build prompt installs ``lhp``; the learning prompt installs ``lhp-learn``.
Existing commands default to ``lhp``. Use ``--name lhp-learn`` to select teaching
for install, export, update, status or uninstall. The selector changes only the
selected skill; teaching installation adds no project-wide development routing
block. You can install both in separate folders.

Install for Genie Code
======================

Run in the authenticated notebook Python context so notebook credentials are
available:

.. code-block:: python

   from lhp.api import SkillFacade
   result = SkillFacade().install_genie_skill(skill_name="lhp")
   print(result.install_dir)

The installer uses the Databricks SDK already in the environment, or the optional
``lakehouse-plumber[genie]`` dependency group. It obtains the authenticated user's
identity, uploads the complete skill to
``/Users/<user>/.assistant/skills/lhp`` through the Workspace API and verifies
the resulting contents. Its filesystem path begins with ``/Workspace/Users/``.
This is Genie's default user skills folder, so custom-folder registration is
unnecessary. A CLI equivalent, ``lhp skill install --agent genie``, is available
when that process has workspace authentication. For teaching, pass
``skill_name="lhp-learn"`` to the API or add ``--name lhp-learn`` to the CLI;
the destination then ends in ``.assistant/skills/lhp-learn``.

Read the installed skill explicitly to continue the current conversation. Native
discovery in a new chat is a separate check; changed skill metadata may require a
new chat. Installation success does not prove that Genie loaded the skill.

Export for another agent
========================

.. code-block:: console

   lhp skill export --output /path/to/skills/lhp
   lhp skill export --name lhp-learn --output /path/to/skills/lhp-learn

Use the agent's supported skills location, or point it directly at this directory.
The Python equivalent is
``SkillFacade().export_skill(Path(...), skill_name="lhp-learn")`` for teaching,
or omit the keyword for development. Export and
Genie installation resume identical/partial installations and reject differing
files with ``LHP-IO-020``. Preserve edits before moving an old copy aside. They do
not use the destructive refresh behaviour of the legacy Claude update command.
``LHP-IO-029`` reports installation/access/verification failures with a safe failure
category; notebook Python also preserves the underlying chained exception.

Install without package-index access
====================================

When an agent cannot retrieve a suitable package, it should give you a full,
clickable `GitHub release URL <https://github.com/Mmodarre/Lakehouse_Plumber/releases/latest>`_,
the exact matching filename and, when verified, direct download and checksum
links. This applies to every coding agent. If a suitable release is unavailable,
the agent should say so and accept a compatible file you supply.

The fallback download is a **complete offline ZIP** containing LHP, both skills,
all pinned CLI/Genie dependencies and the installer. Its filename is
``lhp-offline-VERSION-SYSTEM-ARCH-py312.zip``. Agents should select this archive
when pip cannot provide a usable installation, including when an internal index
has only older releases. They should not ask you to collect dependency wheels.
The standalone LHP wheel remains universal across macOS, Windows and Linux, but
does not contain its dependencies and is not the offline fallback download.
The release build produces four archives for **CPython 3.12**: Linux x86_64,
Linux ARM64, macOS Apple Silicon and Windows x86_64. Select an actual release asset matching
the **environment running Python**, including its OS, architecture and Python
version. An agent running on Linux remotely needs Linux dependencies even if
your laptop uses macOS or Windows. Archive filenames use ``darwin`` for macOS.
If the offline environment uses another Python version, the agent helps you select
a Python 3.12 environment or compatible Databricks compute. Intel macOS has no
official offline archive. If no compatible environment or custom bundle is
available, setup pauses instead of installing a mismatched archive.

For local execution, download the ZIP and checksum into a folder on your machine
and give the agent the ZIP's full path. A connected computer can download them
for transfer to an offline machine. For a container, remote host or cloud workspace,
the agent should guide you through transferring the files into a folder it can
access, then verify and extract the ZIP.

For Genie Code or another agent installing into Databricks, use **Workspace
Files**. Open **Workspace**, choose a writable folder, then use its menu's
**Import > File** to upload the ZIP after checking its published checksum.
Workspace import extracts ZIPs; share the imported folder's workspace link
with the agent. If it cannot resolve that link, use **Copy URL/path > Full path**
on the folder and share the resulting path. See Databricks' `Workspace library
upload instructions <https://docs.databricks.com/aws/en/libraries/workspace-files-libraries>`_.

An offline archive contains pinned dependency wheels, hashes, a compatibility
manifest, installation code and both agent prompts. Verify its published SHA256
before extracting it. Workspace import extracts ZIPs automatically; share the
imported folder's link and have the agent verify its manifest and files. Python
and pip must already be installed; LHP Web's optional dependencies are not included.

Run the archive's ``install.py`` with the selected interpreter, using an isolated
environment on a desktop or notebook Python in Databricks. The installer performs
the network-denied pip operation in a child process so it cannot disable
subsequent Workspace API calls in the parent notebook.

Notebook packages may need reinstalling after a compute session ends. Keep the
version-pinned setup command; skill files in workspace storage persist separately.

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
flowgroup YAML from the packaged field tables and rules, pinned to your installed
LHP version, instead of guessing from
training data.

You still own the output. The agent writes the YAML; you run ``lhp validate``
and ``lhp generate``, and you read the Lakeflow Python it produces. The skill
changes where the agent's YAML comes from, not who is in control of the
pipeline.

What's next
===========

- **Use a different agent.** Export to its supported location and read ``SKILL.md``.
- **Keep the skill current.** After upgrading LHP, export/install its matching
  skill. Preserve differing copies before replacing them. The legacy Claude
  ``lhp skill update`` command replaces its installed files.
- **Read it yourself.** The reference content is correct for humans too, just
  denser and more list-driven than these guides. It is a fast lookup once you
  know LHP. For guided learning, select ``lhp-learn`` and its separate course.
