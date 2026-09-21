=========
Telemetry
=========

.. meta::
   :description: Reference for Lakehouse Plumber anonymous usage telemetry — every field that is sent, the never-collected list, the install and project identifiers, the project_id key in lhp.yaml, endpoint and retention, every off switch, and the lhp telemetry command.

Lakehouse Plumber (LHP) reports anonymous usage telemetry so the project can
see which commands and features are actually used. It is on by default and
opt-out. One event is recorded per CLI command, and the web IDE adds one event
per browser tab and per run. Every field is a bounded enum, a boolean or a
counter — never a name, a path, file content or an error message.

An event is appended to a local spool file and posted in the background, so
telemetry never blocks a command, never changes an exit code and never raises.
When the endpoint is unreachable — a proxy, a firewall, an air-gapped machine
— the upload fails silently and the events stay on disk.

Run ``lhp telemetry show`` to print the exact event this machine would send,
and ``lhp telemetry off`` to turn telemetry off.

.. versionadded:: 0.9.2

What is sent
------------

Envelope
~~~~~~~~

Every event carries the same envelope.

.. list-table::
   :header-rows: 1
   :widths: 22 16 62

   * - Field
     - Type
     - Value
   * - ``schema_version``
     - integer
     - ``1``.
   * - ``event_id``
     - string
     - A fresh UUID v4 per event, used to discard duplicates.
   * - ``event``
     - string
     - ``cli.command``, ``web.session``, ``web.run``, ``install.first_seen`` or ``install.upgraded``.
   * - ``ts``
     - string
     - UTC timestamp with millisecond precision, ``YYYY-MM-DDTHH:MM:SS.mmmZ``.
   * - ``install_id``
     - string or null
     - The installation's UUID v4 (see `Identifiers`_). Always null in CI.
   * - ``project_id``
     - string or null
     - 32 lowercase hex characters, a salted hash (see `Identifiers`_). Null outside a project.
   * - ``project_id_source``
     - string
     - ``lhp_yaml``, ``bundle_uuid``, ``name_hash`` or ``none``.
   * - ``lhp_version``
     - string
     - The installed LHP version.
   * - ``python``
     - string
     - Major and minor only, for example ``3.12``.
   * - ``os``
     - string
     - ``linux``, ``macos``, ``windows`` or ``other``.
   * - ``arch``
     - string
     - ``x86_64``, ``arm64`` or ``other``.
   * - ``install_kind``
     - string
     - ``wheel``, ``editable`` or ``unknown``. See `How install_kind is decided`_.
   * - ``ci_vendor``
     - string
     - ``github_actions``, ``gitlab_ci``, ``azure_devops``, ``jenkins``, ``circleci``, ``buildkite``, ``teamcity``, ``bitbucket``, ``codebuild``, ``travis``, ``drone``, ``other_ci`` or ``none``. Detected from the vendor's own marker variable; ``other_ci`` means only a generic ``CI`` variable was set.
   * - ``agent``
     - string
     - ``claude_code``, ``cursor``, ``gemini_cli``, ``codex``, ``copilot_cli``, ``other`` or ``none``. Detected from the agent's own marker variable.
   * - ``databricks_runtime``
     - boolean
     - Whether ``DATABRICKS_RUNTIME_VERSION`` is set. The runtime version itself is a value and is never collected.
   * - ``interactive``
     - boolean
     - Whether stderr is a terminal.
   * - ``props``
     - object
     - The per-event fields below.

How install_kind is decided
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``install_kind`` is read from the distribution's PEP 610 ``direct_url.json``
metadata, and only its ``dir_info.editable`` field. The record's ``url`` field
is a local filesystem path and is never read.

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Value
     - Meaning
   * - ``wheel``
     - Installed from a built wheel — from an index, or from a local directory or archive that was not installed as editable.
   * - ``editable``
     - An editable (PEP 610 ``dir_info.editable``) install.
   * - ``unknown``
     - The metadata is missing or unreadable, so nothing trustworthy can be said. A source tree or a zipapp reports this.

``cli.command``
~~~~~~~~~~~~~~~

One event per command run, including failed runs.

.. list-table::
   :header-rows: 1
   :widths: 22 16 62

   * - Field
     - Type
     - Value
   * - ``command``
     - string
     - The command that ran, with spaces replaced by dots: ``generate``, ``validate``, ``dag``, ``deps``, ``diff``, ``init``, ``inspect-wheel``, ``substitutions``, ``web``, ``list.presets``, ``list.templates``, ``list.blueprints``, ``skill.install``, ``skill.update``, ``skill.status``, ``skill.uninstall``, ``telemetry.status``, ``telemetry.show``, ``telemetry.on``, ``telemetry.off``.
   * - ``flags``
     - list of strings
     - The sorted NAMES of the parameters you passed on the command line. Values are never read, so ``--env prod`` contributes ``env`` and nothing else.
   * - ``env_class``
     - string or null
     - ``development`` or ``production`` when the ``--env`` target names a mode in ``databricks.yml``; ``unspecified`` when a bundle exists but the target or its mode does not; ``none`` when there is no ``databricks.yml``. Null for commands with no ``--env`` option. The environment name itself is never sent.
   * - ``duration_ms``
     - integer
     - Wall-clock time inside the command's error boundary.
   * - ``exit_code``
     - integer
     - ``0`` success, ``1`` domain error, ``2`` usage error, ``3`` internal error, ``130`` interrupted.
   * - ``error_code``
     - string or null
     - The ``LHP-<CATEGORY>-<NUMBER>`` code of the failure, or ``LHP-GEN-902`` for an unexpected one. See the :doc:`error code catalog </reference/errors>`.
   * - ``exception_class``
     - string or null
     - The exception's class name only. No message, no stack trace.
   * - ``warning_codes``
     - object
     - Error codes counted, for example ``{"LHP-DEP-002": 3}``. Codes only, never messages.
   * - ``failure_codes``
     - object
     - The same shape for failures.
   * - ``files_written``
     - integer or null
     - How many files the run wrote.
   * - ``bundle_enabled``
     - boolean or null
     - Whether bundle support was active for the run.
   * - ``cache_used``
     - boolean or null
     - Whether the run used the discovery cache.
   * - ``project``
     - object or null
     - The project shape below. Present for ``generate``, ``validate`` and ``dag`` only, and only when reading it stays inside a 250 ms budget.

Project shape
~~~~~~~~~~~~~

``project`` is a fixed allowlist of 51 keys — counters and booleans that
describe a project's size and which features it configures. A value LHP does
not recognise folds into its family's ``*_other`` key, so the number of keys
that reach the wire is a property of LHP, not of your project.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Keys
     - What they count
   * - ``pipelines``, ``flowgroups``, ``actions``
     - Project totals.
   * - ``tables``
     - Distinct write targets declared in the discovered flowgroups, counted once each however many actions write to them. Discovery expands neither templates nor presets, so a target that only a template supplies is not counted. Sinks are not counted.
   * - ``load_cloudfiles``, ``load_delta``, ``load_sql``, ``load_python``, ``load_jdbc``, ``load_custom_datasource``, ``load_kafka``, ``load_other``
     - Load actions by source type.
   * - ``transform_sql``, ``transform_python``, ``transform_data_quality``, ``transform_temp_table``, ``transform_schema``, ``transform_other``
     - Transform actions by transform type.
   * - ``write_streaming_table``, ``write_materialized_view``, ``write_sink``, ``write_other``
     - Write actions by target type.
   * - ``write_mode_standard``, ``write_mode_cdc``, ``write_mode_snapshot_cdc``, ``write_mode_other``
     - Table write actions by mode. Sinks carry no mode.
   * - ``test_row_count``, ``test_uniqueness``, ``test_referential_integrity``, ``test_completeness``, ``test_range``, ``test_schema_match``, ``test_all_lookups_found``, ``test_custom_sql``, ``test_custom_expectations``, ``test_other``
     - Test actions by test type.
   * - ``templates``, ``presets``, ``blueprints``, ``blueprint_instances``, ``environments``, ``flowgroups_using_templates``
     - How many of each the project declares, and how many flowgroups use a template.
   * - ``has_operational_metadata``, ``has_event_log``, ``has_monitoring``, ``has_uc_tagging``, ``has_test_reporting``, ``has_wheel``, ``has_sandbox``, ``has_required_lhp_version``, ``apply_formatting``
     - Booleans: whether the project configures the feature. The configured values are never collected.

``web.session``
~~~~~~~~~~~~~~~

One event per browser tab of ``lhp web``, emitted when the tab's event stream
disconnects, when the tab goes idle, or when the server shuts down. A session
shorter than five seconds with no counter moved is dropped rather than sent.

.. list-table::
   :header-rows: 1
   :widths: 24 16 60

   * - Field
     - Type
     - Value
   * - ``session_id``
     - string
     - A UUID v4 the tab mints for itself and keeps in ``sessionStorage``. It joins this session's runs to it and identifies nothing else.
   * - ``duration_s``
     - integer
     - How long the session lasted, in seconds.
   * - ``end_reason``
     - string
     - ``disconnect``, ``idle`` or ``shutdown``.
   * - ``sse_seen``
     - boolean
     - Whether the tab ever opened the event stream.
   * - ``requests_by_family``
     - object
     - API calls counted by route FAMILY, for example ``{"files.write": 12, "runs.validate": 3}``. Concrete URLs and paths are never read.
   * - ``files_created``, ``files_updated``, ``files_deleted``
     - object
     - File mutations counted by kind: ``flowgroup``, ``preset``, ``template``, ``substitution``, ``project_config``, ``pipeline_config``, ``job_config``, ``blueprint``, ``schema``, ``sandbox_profile``, ``sql``, ``python``, ``other``. File names are never collected.
   * - ``runs``
     - object
     - Counts for ``validate_manual``, ``validate_auto``, ``generate`` and ``sandbox``.
   * - ``dag_views``, ``lineage_views``, ``sandbox_toggles``
     - integer
     - Derived from the ``ui`` counters below.
   * - ``assistant_used``
     - boolean
     - Whether the AI assistant was invoked.
   * - ``assistant_provider``
     - string or null
     - ``claude_sdk``, ``omnigent`` or ``other``.
   * - ``assistant_mode``
     - string or null
     - ``claude_subscription``, ``databricks``, ``omnigent_defaults``, ``api_key_env`` or ``other``.
   * - ``ui``
     - object
     - Counts keyed ``<surface>.<action>`` (plus ``.<via>`` for a creation), for example ``{"pipeline_dag.opened": 4}``. ``surface`` names a part of the IDE such as ``file_editor`` or ``problems``, and ``action`` is ``opened``, ``toggled`` or ``created``. A value the server does not recognise is dropped.

``web.run``
~~~~~~~~~~~

One event per validate or generate run started from the web IDE.

.. list-table::
   :header-rows: 1
   :widths: 24 16 60

   * - Field
     - Type
     - Value
   * - ``session_id``
     - string
     - The tab that started the run.
   * - ``kind``
     - string
     - ``validate`` or ``generate``.
   * - ``trigger``
     - string
     - ``manual``, or ``auto`` for the validation that follows a clean YAML save.
   * - ``env_class``
     - string
     - As for ``cli.command``. The environment name is never sent.
   * - ``sandbox``
     - boolean
     - Whether the run used sandbox mode.
   * - ``pipeline_filter``
     - boolean
     - Whether a pipeline filter was applied. The pipelines it selected are never collected.
   * - ``bundle_enabled``
     - boolean or null
     - Whether bundle support was active.
   * - ``duration_ms``
     - integer
     - Wall-clock time of the run.
   * - ``success``
     - boolean
     - Whether the run reported success.
   * - ``aborted``
     - boolean
     - True when the stream ended with no terminal result, for example because the browser disconnected.
   * - ``error_code``
     - string or null
     - An ``LHP-<CATEGORY>-<NUMBER>`` code, and only such a code.
   * - ``error_count``, ``warning_count``
     - integer
     - How many errors and warnings the run reported.
   * - ``files_written``
     - integer or null
     - How many files a generate run wrote.

Install events
~~~~~~~~~~~~~~

``install.first_seen`` carries no fields and is recorded once, when the state
file is created. ``install.upgraded`` carries ``previous_version`` and is
recorded on the first run after the installed version changes. Neither is ever
recorded in CI, because CI runs write no state file.

Never collected
---------------

LHP never collects any name (project, pipeline, flowgroup, action, table,
catalog, schema, environment), paths, YAML, SQL or Python content, generated
code, error or warning messages, environment-variable values, secrets,
usernames, hostnames, email addresses, git remotes, machine identifiers, IP
addresses (discarded at ingest), assistant prompts, responses or tool
arguments, or token counts.

Failures are reported as the ``LHP-<CATEGORY>-<NUMBER>`` code plus the
exception's class name, and nothing else.

The project name itself is never sent either. When a project declares neither
``project_id`` nor ``bundle.uuid``, LHP sends a salted hash of the name
instead, and that identifier is pseudonymous rather than anonymous: the salt
is a public constant, so anyone who guesses the name can reproduce the hash.
`Identifiers`_ explains how to replace it with an opaque value.

Identifiers
-----------

Two identifiers travel with an event, and neither identifies a person.

**Install id.** A UUID v4 stored in the state file, created on the first run
that records an event. It distinguishes one installation from another so that
twenty commands from one machine are not read as twenty users. It is never
created and never sent in CI, where a fresh runner would otherwise look like a
new developer on every build.

**Project id.** The first 32 characters of the lowercase hex digest of
``sha256("lhp-project:" + <raw identifier, trimmed and lowercased>)``.
``project_id_source`` says which raw identifier was hashed.

.. list-table::
   :header-rows: 1
   :widths: 10 45 25 20

   * - Order
     - Raw identifier
     - Read from
     - ``project_id_source``
   * - 1
     - ``project_id``
     - ``lhp.yaml``
     - ``lhp_yaml``
   * - 2
     - ``bundle.uuid``
     - ``databricks.yml``
     - ``bundle_uuid``
   * - 3
     - ``name``
     - ``lhp.yaml``
     - ``name_hash``
   * - 4
     - none — ``project_id`` is null
     - —
     - ``none``

The same hash is applied whichever source is found, so the raw identifier
never leaves the machine — a ``bundle.uuid`` is sent hashed, not as the value
your bundle deployment uses. Sources 1 and 2 normally hash the UUID that
``lhp init`` minted, which is opaque, and the result is anonymous. Source 3
hashes a guessable string, which makes it pseudonymous — add a ``project_id``
to ``lhp.yaml`` to move to source 1.

Project identity — ``project_id`` in ``lhp.yaml``
-------------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 16 12 16 56

   * - Field
     - Type
     - Default
     - Description
   * - ``project_id``
     - string
     - — (absent)
     - Opaque per-project identifier, a UUID v4. Used only in hashed form, and only by telemetry: nothing in code generation reads it. Safe to commit — that is the point, because it makes every developer's and every CI run's events roll up to one project.

``lhp init`` writes a fresh ``project_id`` into the scaffolded ``lhp.yaml``,
and writes the same value as ``bundle.uuid`` in ``databricks.yml`` when the
project is scaffolded with bundle support.

.. code-block:: yaml

   # lhp.yaml
   name: my_project
   project_id: 3f1c9e4a-6b2d-4e8f-9a70-5c1d2e3f4a5b
   version: "1.0"

LHP never edits an existing ``lhp.yaml`` to add the key. A project created
before 0.9.2 keeps working and resolves to source 2 or source 3 above; adding
the key yourself changes the project's identifier once, after which it is
stable.

Endpoint, transport and retention
---------------------------------

Events are posted as one ``POST`` request with a JSON body to an LHP-owned
HTTPS endpoint. ``lhp telemetry status`` prints the endpoint this build uses.
The request carries ``Content-Type: application/json`` and a
``User-Agent: lhp/<version>`` header, and nothing else — no authentication, no
cookies. The default ``urllib`` opener is used, so ``HTTPS_PROXY`` and
``NO_PROXY`` are honoured like every other Python HTTP client.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Property
     - Value
   * - Attempts
     - One per command. There is no retry loop inside a run; a batch that was not accepted waits for the next command.
   * - Timeout
     - 3 seconds.
   * - Added exit latency
     - At most 1 second in the worst case. The upload runs on a daemon thread and the command waits up to one second for it before exiting.
   * - Batch caps
     - At most 500 events and 512 KB per request.
   * - Spool caps
     - At most 500 events and 512 KB (524,288 bytes) on disk. When either cap is exceeded, the oldest events are dropped.
   * - 2xx response
     - The batch is accepted and removed from the spool. An empty body is a plain success; a non-empty body that is not JSON came from a proxy or a captive portal rather than the receiver, so the batch is kept.
   * - 400, 413 and any other 4xx except 429
     - The batch is discarded. The receiver will not accept it however often it is offered.
   * - 429, 5xx, timeout, connection error
     - The batch is kept and offered again on the next command.
   * - Remote pause
     - A 2xx response may carry ``{"disabled": true}``, which silences this installation for 24 hours.
   * - IP addresses
     - Discarded at ingest. They are never logged or stored, and no region or other location is derived from them.
   * - Retention
     - Raw events are deleted after 12 months. A monthly aggregate that carries no identifiers is kept indefinitely, and those aggregates are published in the release notes.

.. note::

   The telemetry upload is the only outbound network request the ``lhp``
   command line makes. (The ``lhp web`` AI assistant talks to whichever
   provider you configure for it, which is a separate feature with its own
   settings.)

Turning telemetry off
---------------------

.. list-table::
   :header-rows: 1
   :widths: 32 36 32

   * - Switch
     - Turns telemetry off when
     - Scope
   * - ``LHP_TELEMETRY``
     - set to ``off``, ``0`` or ``false`` (case-insensitive)
     - The process.
   * - ``DO_NOT_TRACK``
     - set to anything except an empty string, ``0``, ``false`` or ``no``
     - The process.
   * - ``LHP_DISABLE_ANALYTICS``
     - the same values as ``DO_NOT_TRACK``
     - The process. A permanent alias, kept for existing CI configurations.
   * - ``PYTEST_CURRENT_TEST``
     - set — pytest sets it for every test phase
     - Any pytest run, so a test suite that imports LHP never emits.
   * - ``lhp telemetry off``
     - run once
     - This user on this machine, until ``lhp telemetry on``.

Any off wins. The switches are checked in the order above and the first one
that says off decides; ``lhp telemetry status`` names it. The stored
preference is read only after every environment switch has passed, so an
opted-out environment never touches the config directory at all.

Blocking the endpoint's hostname at the network level also stops anything
leaving the machine, but it is not an off switch: events are still recorded
and still accumulate in the spool, up to the caps above.

Environment variables
---------------------

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Variable
     - Effect
   * - ``LHP_TELEMETRY``
     - ``off``, ``0`` or ``false`` turns telemetry off. ``log`` writes the event as one JSON line to stderr and neither spools nor sends it. Any other value is ignored.
   * - ``DO_NOT_TRACK``
     - Turns telemetry off.
   * - ``LHP_DISABLE_ANALYTICS``
     - Turns telemetry off. A permanent alias of ``LHP_TELEMETRY=off``.
   * - ``LHP_CONFIG_DIR``
     - Overrides the config directory holding the state file and the spool.
   * - ``LHP_TELEMETRY_ENDPOINT``
     - Overrides the endpoint. Accepted only for an ``https://`` URL, or an ``http://`` URL whose host is ``127.0.0.1``, ``localhost`` or ``::1``; any other value is ignored and the default endpoint is used.
   * - ``LHP_UPDATE_CHECK``
     - ``off``, ``0`` or ``false`` silences the update hint.

The ``lhp telemetry`` command
-----------------------------

- ``lhp telemetry status`` — print whether telemetry is on, which layer decided that, the mode, the config directory, the install id, the endpoint, how many events are spooled, and a link to this page.
- ``lhp telemetry show`` — print the ``cli.command`` event this invocation would send, then the newest spooled events, one compact JSON object per line.
- ``lhp telemetry on`` — turn telemetry on for this user on this machine.
- ``lhp telemetry off`` — turn telemetry off for this user on this machine. The install id and any spooled events are kept, and nothing is recorded or sent while it is off.

Every option is listed in the :doc:`CLI reference </reference/cli>`.

``lhp telemetry on`` and ``lhp telemetry off`` are the only telemetry
operations that can fail a command: when the state file cannot be written they
raise ``LHP-IO-028``, because a preference you asked to change has to be
reported when it does not take. Every other telemetry failure is logged at
debug level and ignored.

Update hint
-----------

The receiver's reply to a telemetry upload may name the latest released
version. LHP stores it and, on a later run, prints one line:

.. code-block:: text

   lhp 0.9.3 is available (installed 0.9.2): pip install -U lakehouse-plumber  [LHP_UPDATE_CHECK=off to silence]

The line is printed only when the command succeeded, the run is interactive
and outside CI, ``LHP_UPDATE_CHECK`` does not opt out, the stored version is
newer than the installed one, and no hint has been shown in the past 24 hours.
The check never makes a request of its own, so it works only while telemetry
is on.

Local files
-----------

The config directory is resolved in this order: ``LHP_CONFIG_DIR`` verbatim;
then ``%APPDATA%\lhp`` on Windows; then ``$XDG_CONFIG_HOME/lhp`` when that
variable holds an absolute path; otherwise ``~/.config/lhp``.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Path
     - Contents
   * - ``<config dir>/telemetry.json``
     - The install id, your on/off preference, the version last seen, and the latest version and hint timestamps behind the update hint.
   * - ``<config dir>/telemetry/spool.jsonl``
     - Events waiting to be sent, one JSON object per line.

Both files, and the directories holding them, are created with owner-only
permissions on the first write. Neither is created in ``LHP_TELEMETRY=log``
mode or on a run where telemetry is off. The state file is additionally never
created in CI, which is why ``install_id`` is always null there; the spool is
still used, because that is how a CI run's events reach the endpoint.

Deleting either file is safe: the install id is minted again on the next
recorded event, and a deleted spool discards the events it was holding.
