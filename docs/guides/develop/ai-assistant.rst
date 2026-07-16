=============================================
Get authoring help from the in-IDE assistant
=============================================

.. meta::
   :description: Reach and configure the AI assistant built into the Lakehouse Plumber web IDE — an agent that knows your flowgroups, presets, and conventions and edits your project files in place, instead of a generic chatbot that knows none of them.

When you get stuck writing a flowgroup, the usual move is to paste a snippet
into a general-purpose chatbot. It answers in a vacuum: it has never seen your
``presets/``, it does not know your substitution tokens, and it cannot open the
file you are asking about. You copy its guess back, adapt it by hand, and hope
it matches your project's conventions.

The web IDE ships an assistant that closes that gap. It runs inside the
``lhp web`` server, loads a packaged Lakehouse Plumber skill so it already knows
flowgroups, actions, presets, and templates, and it works directly against the
project open in the IDE — reading, searching, and editing your real files. That
is the difference: **an LHP-aware agent editing your project, not a generic
chatbot guessing at it.**

Let's open the assistant, sign it in, and put it to work on your flowgroups.

Before you start
================

You need the web IDE, which lives in an optional install extra. Install it once:

.. code-block:: bash

   pip install 'lakehouse-plumber[webapp]'

That extra bundles everything the assistant needs, including a self-contained
``claude`` runtime — there is no Node.js to install and no API key to paste.
Then launch the IDE from your project root:

.. code-block:: bash

   lhp web

The assistant is a panel inside that IDE; it is not a separate command.

Open the assistant
==================

In the web IDE header, select the sparkles button to open the assistant panel.
The first time, the panel shows a one-time setup card asking how the assistant
should run. Keep the preselected **Claude (built in)** provider — it is the
agent that runs in-process, in your project directory — and choose how it signs
in.

Sign it in
==========

The built-in assistant never takes an API key. It authenticates one of two
ways, and in both cases Lakehouse Plumber stores only a name (an environment
variable's name or a profile's name), never a secret value.

**Claude subscription.** The simplest path: sign the machine running
``lhp web`` in to Claude Code once, and the assistant reuses those credentials.

.. code-block:: bash

   claude

Log in when prompted, then start ``lhp web`` from that same account. For an
unattended or shared host, create a long-lived token instead and export it, then
give the setup card the *name* of the variable that holds it:

.. code-block:: bash

   claude setup-token

**Databricks workspace.** Route model calls through your workspace's AI
gateway under your own identity. Authenticate a CLI profile:

.. code-block:: bash

   databricks auth login --profile <profile-name>

Then pick that profile in the setup card. Lakehouse Plumber reads profile names
from ``~/.databrickscfg`` only; the bearer token is minted fresh for every turn
and is never cached, persisted, or logged.

Select **Save and start chatting**. You can change the provider or sign-in
later from the same panel — the change takes effect on the next turn.

.. note::

   For subscription sign-in, launch ``lhp web`` from a shell that does not
   export ``ANTHROPIC_BASE_URL``, ``ANTHROPIC_AUTH_TOKEN``, or
   ``ANTHROPIC_API_KEY``. The assistant's runtime inherits the server's
   environment, and those variables would override your Claude Code sign-in.

What it can do
==============

The assistant runs with the project root as its working directory and loads the
project's ``.claude/settings.json``, so it behaves like a collaborator sitting
in your repository rather than a chat window on the side:

- **It knows Lakehouse Plumber.** A packaged LHP skill is installed into the
  project (``.claude/skills/lhp/``) and refreshed as you upgrade, so the agent
  answers with real flowgroup, action, preset, and template knowledge instead
  of generic PySpark advice.
- **It reads and searches your project** to answer questions grounded in your
  actual files — which preset an action inherits, where a token resolves, why
  a load is failing validation.
- **It edits files in place.** Ask it to add a transform or fix a write action
  and it changes the YAML on disk. A files-changed count appears in the panel
  header, and the IDE's open views refresh the way they do for any on-disk edit.
  Review the result with your normal git tooling.
- **It runs commands** — including ``lhp validate`` and ``lhp generate`` — so it
  can check its own work and show you the output.

Because the edits land on disk as ordinary files, everything the assistant
writes is code you own: diff it, revert it, and commit it like anything else.

.. figure:: /_static/web-assistant.png
   :width: 100%
   :alt: The Lakehouse Plumber web IDE assistant panel showing a completed conversation. A chat-session tab with a New-chat-tab plus button and a chat-history control sit in the panel header, next to a "42 files changed" badge. A stack of tool-activity cards traces the agent reasoning, finding the flowgroup file, and reading it. The answer is a table listing the radio_play_bronze flowgroup's six actions with their types, and a usage line at the bottom reads 26 in, 683 out, 89.0k cache, $0.12 est.

   The assistant answering *show me the actions in the radio_play_bronze
   flowgroup*: a chat-session tab and its New-chat-tab button sit at the top,
   tool-activity cards trace the agent reasoning, finding the flowgroup file,
   and reading it, the answer lists the flowgroup's six actions as a table, and
   the per-turn usage line reports tokens in, out, and cache with an estimated
   cost.

Work across chat sessions
=========================

One long thread mixes unrelated work: the transform you fixed this morning and
the write action you are debugging now share a single, ever-growing context.
Keep them apart. The panel header carries a row of **chat-session tabs** — one
per thread, each with its own history. Select **New chat tab** (the ``+`` next
to the tabs) to start a fresh thread, and click a tab to switch back. The
**chat-history** control in the header reopens earlier sessions, so a thread you
closed is still there when you need it.

See what each turn costs
========================

Every turn reports what it spent. The line at the bottom of the panel breaks
down the turn's token use — tokens in, tokens out, and cache read — followed by
an estimated dollar cost, for example ``26 in · 683 out · 89.0k cache · $0.12
est.`` Watch it to catch a turn that reads far more than the task needs, or to
compare what different providers cost you.

Control what runs without asking
================================

The assistant asks before it changes anything. A selector under the message box
sets the approval policy for the messages you send next:

- **Ask every time** (the default) — every file edit, command, and web access
  raises an approval card you accept or decline.
- **Accept edits** — file edits apply without asking; commands and web access
  still ask.
- **Allow all** — nothing asks; the agent runs every tool unattended.

Even under **Ask every time**, actions that only read never interrupt you:
reading and searching project files run silently, and shell commands that
provably only read — ``git status``, ``ls``, ``grep`` and similar — run without
a prompt. When an approval card offers **Always allow**, accepting it saves a
per-project rule so that tool or command prefix stops asking.

You can also pre-approve tools before the assistant ever runs. Because it loads
the project's ``.claude/settings.json``, rules under ``permissions.allow`` are
honored without a prompt:

.. code-block:: json
   :caption: .claude/settings.json

   {
     "permissions": {
       "allow": [
         "Bash(lhp validate:*)",
         "Bash(lhp generate:*)"
       ]
     }
   }

.. important::

   **Allow all** removes the approval step entirely — the assistant can edit any
   file and run any command your account can. Prefer **Accept edits** for
   routine authoring and review the changes with git before you commit.

What's next
===========

- **Learn the rest of the web IDE.** The assistant is one panel among many —
  the graph view, the flowgroup editors, and the sandbox runner. See the web
  IDE guide.
- **See every approval state and setting.** The full list of panel states,
  always-allow rule management, per-turn behavior, and the alternative
  user-managed provider (the omnigent daemon) lives in the AI assistant
  reference.
