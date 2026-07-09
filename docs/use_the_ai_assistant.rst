Use the AI assistant in the web IDE
===================================

.. meta::
   :description: How to set up and use the AI assistant panel in the Lakehouse Plumber web IDE (lhp web) — sign in with your Claude subscription or a Databricks workspace, and chat with an agent that edits your project files in place.

This how-to sets up the AI assistant panel in the Lakehouse Plumber (LHP)
local web IDE. The default provider is **built in**: installing the web IDE
(``pip install 'lakehouse-plumber[webapp]'``) is the only install step — the
assistant runs inside the ``lhp web`` server itself, answers questions about
your project, and edits its files in place. It signs in with either your
Claude subscription or a Databricks workspace; you never paste an API key.

An alternative provider — the user-managed `omnigent
<https://github.com/omnigent-ai/omnigent>`_ daemon — remains selectable; see
:ref:`assistant-omnigent-provider`.

.. versionadded:: 0.9.1

Requirements
------------

* The LHP web IDE, up and running — see :doc:`develop_in_the_web_ide`.
* A way for the assistant to sign in — one of:

  * **Claude subscription** — the machine running ``lhp web`` is signed in
    to Claude Code (run ``claude`` once and log in), or you export a
    long-lived token created with ``claude setup-token`` and give the panel
    that variable's *name*; or
  * **Databricks workspace** — an authenticated Databricks CLI profile
    (``databricks auth login --profile <profile-name>``). Model calls route
    through your workspace's AI gateway under that profile's identity; the
    token is minted fresh for every turn and never stored.

Set up the assistant
--------------------

1. In the web IDE, select **Open assistant panel** (the sparkles button in
   the header). The first time, the panel shows a one-time setup card,
   "Choose how the assistant runs".

2. Keep the preselected **Claude (built in)** provider, then pick how it
   signs in:

   * **Claude subscription (Claude Code login)** — uses the machine's
     Claude Code sign-in. Optionally name an environment variable that
     holds a ``claude setup-token`` token instead; the panel stores the
     variable *name* only and rejects anything that is not a valid name.
   * **Databricks workspace** — select a profile from your
     ``~/.databrickscfg``. LHP reads profile *names* only, never
     credential values.

3. Optionally override the model, then select **Save and start chatting**.
   You can change the provider or sign-in later from the same panel; the
   change takes effect on the next chat turn.

.. note::

   For subscription sign-in, start ``lhp web`` from a shell that does not
   export ``ANTHROPIC_BASE_URL``, ``ANTHROPIC_AUTH_TOKEN``, or
   ``ANTHROPIC_API_KEY`` — the assistant's runtime inherits the server's
   environment, and those variables would override your Claude Code
   sign-in.

Chat with the assistant
-----------------------

Type a message and send it. The assistant streams its answer as rendered
Markdown, and the turn's activity appears inline:

* **Tool cards** summarize each tool call in one line — the action and the
  argument that matters (the file path, the shell command, the search
  pattern) — with a pass/fail icon; expand *Details* for the full arguments
  and output.
* **Reasoning** is collapsed behind a disclosure — expand it to read the
  agent's thinking.
* **Approval cards** appear when the agent asks permission for an action
  that changes anything — editing a file, running a command, fetching a web
  page. Read-only actions (reading and searching project files) run without
  asking. Respond with **Accept**, **Decline**, or **Cancel**; Decline lets
  the turn continue another way, Cancel stops it.
* Stop a running turn with the stop button in the composer; the thread
  marks the turn *Interrupted*.
* Reloading the browser rehydrates the active conversation, and the
  new-session button in the panel header archives the current conversation
  and starts a fresh one.

Drag the panel's left edge to resize it; the width is remembered in your
browser.

Choose how much to approve
~~~~~~~~~~~~~~~~~~~~~~~~~~

The selector under the message box sets the approval policy for the
messages you send next:

* **Ask every time** (the default) — every file edit, command, and web
  access raises an approval card.
* **Accept edits** — file edits run without asking; commands and web
  access still ask.
* **Allow all** — nothing asks. The agent runs every tool, shell commands
  included, unattended.

The choice is remembered in your browser and applies from the next
message; it never changes a turn that is already running.

.. important::

   **Allow all** removes the approval step entirely — the assistant can
   edit any file and run any command your user account can. Prefer
   **Accept edits** for routine work and review changes with your normal
   git tooling.

The assistant edits files directly on disk. During a turn, a files-changed
chip in the panel header counts the files the turn has touched, and the
IDE's open views — file tree, editors, flowgroup pages — refresh themselves
the same way they do for any on-disk edit. Review the changes with your
normal git tooling.

Troubleshoot from the panel
---------------------------

Every failure mode surfaces as a state inside the panel:

* **"Claude runtime unavailable"** — the runtime bundled with
  ``claude-agent-sdk`` is missing. Reinstall the webapp extra in the
  environment running ``lhp web``:
  ``pip install 'lakehouse-plumber[webapp]'``.
* **"Assistant session failed"** — the card names the fix: sign in with
  ``claude`` on this machine (or export the configured token variable
  before starting ``lhp web``), or run
  ``databricks auth login --profile <profile-name>``, then send the
  message again.
* **"Connection to the assistant was lost"** — the turn's runtime ended
  unexpectedly. Send the message again to start a fresh turn; the
  conversation history is kept locally and is not lost.

.. _assistant-omnigent-provider:

Alternative provider: the omnigent daemon
-----------------------------------------

Instead of the built-in provider, the assistant can proxy to `omnigent
<https://github.com/omnigent-ai/omnigent>`_, an agent daemon you install
and run yourself. Select **Omnigent daemon** as the provider in the setup
card.

.. important::

   The omnigent daemon's local REST API is **unauthenticated**, and the
   agent edits real project files in place — any process on your machine can
   drive it. LHP's protections (the loopback-only proxy and the web IDE's
   session token) guard the browser path only, not the daemon itself. Run
   the daemon only on a trusted, single-user machine.

Install it yourself — LHP never installs it and does not depend on it:

.. code-block:: bash

   uv tool install omnigent

or ``pip install omnigent`` inside a Python 3.12 environment (omnigent
requires 3.12 — not 3.11, not 3.13; ``uv tool install`` provisions the
right interpreter for you).

With the Omnigent provider selected, the panel walks the daemon ladder
before chat is available:

1. If the daemon is not running, click **Start it for me**. LHP spawns
   ``omnigent server start`` and ``omnigent host`` as detached, user-owned
   processes; their output lands in ``.lhp/logs/omnigent-server.log`` and
   ``.lhp/logs/omnigent-host.log``. Alternatively run those two commands
   yourself in a terminal. LHP never stops the daemons it starts: they
   deliberately outlive the IDE so live agent sessions survive a server
   restart.

2. Pick the executor mode in the setup card:

   * **omnigent defaults** — the credentials configured with
     ``omnigent setup``. No further fields.
   * **Databricks workspace** — a profile from your ``~/.databrickscfg``;
     model calls go through the Databricks gateway under that profile's
     identity.
   * **API key from environment variable** — the name of an environment
     variable (for example ``ANTHROPIC_API_KEY``) set in the omnigent
     server's environment. Never paste the key itself.

By default LHP probes ``http://127.0.0.1:6767``. If your daemon listens
elsewhere, set ``LHP_WEBAPP_ASSISTANT_URL`` before launching:

.. code-block:: bash

   LHP_WEBAPP_ASSISTANT_URL=http://127.0.0.1:7878 lhp web

The URL must point at a loopback host — ``127.0.0.1``, ``localhost``, or
``::1``. Anything else is rejected at startup, because the daemon's API is
unauthenticated: a non-loopback URL would send your project's content to
another machine.

Omnigent-specific panel states: **"omnigent is not installed"** (the
binary is not on the ``PATH`` of the ``lhp web`` process — install it and
reopen the panel), **"The omnigent server is not running"** / **"No
omnigent host is online"** (click **Start it for me** or run the two
daemon commands; if the state persists, check the logs under
``.lhp/logs/``), and **"Assistant session failed"** naming
``omnigent setup`` as the fix (executor credentials missing).

Related articles
----------------

* :doc:`develop_in_the_web_ide` — launch and use the web IDE the assistant
  panel lives in.
* `omnigent on GitHub <https://github.com/omnigent-ai/omnigent>`_ —
  installation, ``omnigent setup``, and daemon documentation.
