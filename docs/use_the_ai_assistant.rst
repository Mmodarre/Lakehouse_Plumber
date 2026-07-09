Use the AI assistant in the web IDE
===================================

.. meta::
   :description: How to set up and use the AI assistant panel in the Lakehouse Plumber web IDE (lhp web) — install the omnigent daemon, choose a model executor, and chat with an agent that edits your project files in place.

This how-to sets up the AI assistant panel in the Lakehouse Plumber (LHP)
local web IDE. The panel connects ``lhp web`` to `omnigent
<https://github.com/omnigent-ai/omnigent>`_, an agent daemon that runs on
your machine, answers questions about your project, and edits its files in
place. You install and run the daemon yourself; LHP detects it, proxies the
chat through its own loopback server, and shows the agent's work in the IDE.

.. versionadded:: 0.9.1

.. important::

   The omnigent daemon's local REST API is **unauthenticated**, and the
   agent edits real project files in place — any process on your machine can
   drive it. LHP's protections (the loopback-only proxy and the web IDE's
   session token) guard the browser path only, not the daemon itself. Run
   the daemon only on a trusted, single-user machine.

Requirements
------------

* The LHP web IDE, up and running — see :doc:`develop_in_the_web_ide`.
* omnigent, installed by you. LHP never installs it and does not depend on
  it:

  .. code-block:: bash

     uv tool install omnigent

  or ``pip install omnigent`` inside a Python 3.12 environment.

  .. note::

     omnigent requires Python 3.12 — not 3.11, not 3.13. ``uv tool install``
     provisions the right interpreter for you; with ``pip``, the environment
     itself must be 3.12.

* Model credentials the daemon can use — one of:

  * omnigent's own defaults, configured once with ``omnigent setup``;
  * an authenticated Databricks CLI profile
    (``databricks auth login --profile <profile-name>``), which routes model
    calls through the Databricks gateway; or
  * an API key exported as an environment variable in the environment that
    runs the omnigent server.

Start the daemon
----------------

1. In the web IDE, select **Open assistant panel** (the sparkles button in
   the header). The panel opens as a right-hand dock and detects the daemon.

2. If the daemon is not running, click **Start it for me**. LHP spawns
   ``omnigent server start`` and ``omnigent host`` as detached, user-owned
   processes, and the panel updates as soon as they answer. Their output
   lands in ``.lhp/logs/omnigent-server.log`` and
   ``.lhp/logs/omnigent-host.log``.

   Alternatively, run the same two commands yourself in a terminal:

   .. code-block:: bash

      omnigent server start
      omnigent host

LHP never stops the daemons it starts: they deliberately outlive the IDE so
live agent sessions survive a server restart. They are ordinary processes —
stopping them is your call.

Choose an executor
------------------

The first time the daemon is up, the panel shows a one-time setup card,
"Choose how the assistant runs". Pick the executor mode:

* **omnigent defaults** — the credentials configured with
  ``omnigent setup``. No further fields.
* **Databricks profile** — select a profile from your ``~/.databrickscfg``;
  model calls go through the Databricks gateway under that profile's
  identity. LHP reads profile *names* only, never credential values.
* **API key from environment variable** — enter the name of an environment
  variable (for example ``ANTHROPIC_API_KEY``) that is set in the omnigent
  server's environment. Never paste the key itself; the panel rejects
  anything that is not a valid variable name.

Optionally override the model, then select **Save and start chatting**. You
can change the executor later from the same panel; the change takes effect
on the next chat turn.

Chat with the assistant
-----------------------

Type a message and send it. The assistant streams its answer as rendered
Markdown, and the turn's activity appears inline:

* **Tool cards** show each tool call with its status; expand *Details* for
  the raw arguments.
* **Reasoning** is collapsed behind a disclosure — expand it to read the
  agent's thinking.
* **Approval cards** appear when the agent asks permission for an action;
  respond with **Accept**, **Decline**, or **Cancel**.
* Stop a running turn with the stop button in the composer; the thread
  marks the turn *Interrupted*.
* Reloading the browser rehydrates the active conversation from the
  daemon's session snapshot, and the new-session button in the panel header
  archives the current conversation and starts a fresh one.

The assistant edits files directly on disk. During a turn, a files-changed
chip in the panel header counts the files the turn has touched, and the
IDE's open views — file tree, editors, flowgroup pages — refresh themselves
the same way they do for any on-disk edit. Review the changes with your
normal git tooling.

Point LHP at a different daemon
-------------------------------

By default LHP probes ``http://127.0.0.1:6767``. If your daemon listens
elsewhere, set ``LHP_WEBAPP_ASSISTANT_URL`` before launching:

.. code-block:: bash

   LHP_WEBAPP_ASSISTANT_URL=http://127.0.0.1:7878 lhp web

The URL must point at a loopback host — ``127.0.0.1``, ``localhost``, or
``::1``. Anything else is rejected at startup, because the daemon's API is
unauthenticated: a non-loopback URL would send your project's content to
another machine.

Troubleshoot from the panel
---------------------------

Every failure mode surfaces as a state inside the panel:

* **"omnigent is not installed"** — the ``omnigent`` binary is not on the
  ``PATH`` of the ``lhp web`` process. Install it (see Requirements) and
  reopen the panel.
* **"The omnigent server is not running"** or **"No omnigent host is
  online"** — click **Start it for me**, or run ``omnigent server start``
  and ``omnigent host`` in a terminal. If the state persists, check the
  logs under ``.lhp/logs/``.
* **"Assistant session failed"** — the card names the fix: run
  ``omnigent setup`` (executor credentials missing) or
  ``databricks auth login --profile <profile-name>`` (Databricks profile
  not authenticated), then send the message again.
* **"Connection to the assistant was lost"** — the stream dropped
  mid-turn. Confirm the daemon is still up, then send the message again;
  the conversation lives on the daemon and is not lost.

Related articles
----------------

* :doc:`develop_in_the_web_ide` — launch and use the web IDE the assistant
  panel lives in.
* `omnigent on GitHub <https://github.com/omnigent-ai/omnigent>`_ —
  installation, ``omnigent setup``, and daemon documentation.
