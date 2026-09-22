=========
Telemetry
=========

.. meta::
   :description: Reference for Lakehouse Plumber anonymous usage telemetry — every field that is sent, the never-collected list, the install and project identifiers, the project_id key in lhp.yaml, endpoint and retention, every off switch, and the lhp telemetry command.

Lakehouse Plumber (LHP) reports anonymous usage telemetry so the project can
see which commands and features are actually used. It is on by default and
opt-out. LHP records one event per CLI command; the web integrated development
environment (IDE) adds one per browser tab and per run. Every field is a
bounded enum, a boolean, a counter, a version string or an opaque identifier —
never a name, a path, file content or an error message.

LHP appends the event to a local spool file and posts it in the background, so
telemetry never blocks a command, never changes an exit code and never raises.
When the endpoint is unreachable the upload fails silently and the events stay
on disk.

Run ``lhp telemetry show`` to print the shape of the event this machine would
send — the preview's ``duration_ms`` and ``exit_code`` are zero and ``flags``
is empty — and ``lhp telemetry off`` to turn telemetry off.

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
     - A fresh UUID v4 per event. The receiver can store an event more than once, and ``event_id`` identifies the copies.
   * - ``event``
     - string
     - ``cli.command``, ``web.session``, ``web.run``, ``install.first_seen`` or ``install.upgraded``.
   * - ``ts``
     - string
     - UTC timestamp with millisecond precision, ``YYYY-MM-DDTHH:MM:SS.mmmZ``.
   * - ``install_id``
     - string or null
     - The installation's UUID v4 (see :ref:`Identifiers <telemetry-identifiers>`). Always null under continuous integration (CI).
   * - ``project_id``
     - string or null
     - 32 lowercase hex characters, a salted hash (see :ref:`Identifiers <telemetry-identifiers>`). Null outside a project.
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
     - ``wheel``, ``editable`` or ``unknown``. See :ref:`How install_kind is decided <telemetry-install-kind>`.
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

.. _telemetry-install-kind:

How install_kind is decided
~~~~~~~~~~~~~~~~~~~~~~~~~~~

LHP reads ``install_kind`` from the distribution's PEP 610 ``direct_url.json``
metadata, and only its ``dir_info.editable`` field. The record's ``url`` field
is a local filesystem path and is never read.

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Value
     - Meaning
   * - ``wheel``
     - Installed from a built wheel: either no ``direct_url.json`` at all, which is what an install from an index leaves behind, or one whose ``dir_info`` is present and not editable, which is what a local directory leaves behind.
   * - ``editable``
     - An editable (PEP 610 ``dir_info.editable``) install.
   * - ``unknown``
     - The metadata is missing, unreadable, or describes a direct URL with no ``dir_info`` — an archive, a URL or a version-control install. Nothing trustworthy can be said, so nothing is claimed. A source tree or a zipapp reports this too.

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
     - The sorted *names* of the parameters you passed on the command line. Values are never read, so ``--env prod`` contributes ``env`` and nothing else.
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
     - The ``LHP-<CATEGORY>-<NUMBER>`` code of the error that ended the command, or ``LHP-GEN-902`` for an unexpected one. When ``generate`` stops on failures, this is the sole failure's own code, or ``LHP-VAL-902`` when it found more than one; a Python function naming conflict (``LHP-VAL-019``) counts as one failure alongside the failed pipelines. A run that completes and reports its failures itself, such as a ``validate`` run that finds errors, sends null here; their codes are in ``failure_codes``. A value that is not a recognized LHP error code is sent as null. See the :doc:`error code catalog </reference/errors>`.
   * - ``exception_class``
     - string or null
     - The exception's class name only. No message, no stack trace.
   * - ``warning_codes``
     - object
     - Error codes counted, for example ``{"LHP-DEP-002": 3}``. Codes only, never messages. A warning without a recognized LHP error code is counted as ``other``.
   * - ``failure_codes``
     - object
     - The same shape for failures, with the same ``other`` count: one code per failed pipeline for ``generate``, and one per error found for ``validate``, except that a ``validate`` run stopped by its project-level checks counts a single code, the first one they found.
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
     - The project shape below. Present for ``generate``, ``validate`` and ``dag`` (including its ``deps`` alias) only, and only when reading it stays inside a 250 ms budget. Null when a ``generate`` run stops on an error before any pipeline has failed (for example a configuration error or an empty project), when a ``validate`` run is aborted by an exception before any pipeline has failed, or when you interrupt the run. A ``validate`` run on an empty project, or one stopped by its project-level checks, still carries the shape.

Project shape
~~~~~~~~~~~~~

``project`` is a fixed allowlist of 51 keys — counters and booleans that
describe a project's size and which features it configures. A value LHP does
not recognize folds into its family's ``*_other`` key, so the number of keys
that reach the wire is a property of LHP, not of your project.

The counts come from project discovery, which expands blueprint instances but
not templates: actions that a template supplies are missing from ``actions``
and from every per-type counter. The pipeline LHP generates when
:doc:`monitoring </reference/config/monitoring>` is enabled is counted, with
its flowgroup and actions.

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

LHP emits one event per browser tab of ``lhp web``, when the tab's event
stream disconnects, when the tab goes idle, or when the server shuts down. It
drops a session shorter than five seconds that moved no counter rather than
sending it.

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
     - Seconds from the tab's first recorded activity to its last, or to the close of its event stream when that is later. Activity is any application programming interface (API) request the tab makes other than the health check, including opening the event stream, plus the end of each run the tab starts. A tab still connected when the server shuts down counts up to the shutdown. LHP does not count the waits that end a session: 30 seconds after the event stream closes, or 30 minutes without activity.
   * - ``end_reason``
     - string
     - ``disconnect``, ``idle`` or ``shutdown``.
   * - ``sse_seen``
     - boolean
     - Whether the tab ever opened the event stream.
   * - ``requests_by_family``
     - object
     - API calls counted by route *family*, for example ``{"files.write": 12, "runs.validate": 3}``. Concrete URLs and paths are never read.
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
     - Counts keyed ``<surface>.<action>`` (plus ``.<via>`` for a creation), for example ``{"pipeline_dag.opened": 4}``. ``surface`` names a part of the IDE such as ``file_editor`` or ``problems``, and ``action`` is ``opened``, ``toggled`` or ``created``. A value the ``lhp web`` server does not recognize is dropped.

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
     - An LHP error code such as ``LHP-ACT-001``, and only such a code. Any other value is sent as null.
   * - ``error_count``, ``warning_count``
     - integer
     - For a run that reports totals, such as a completed ``validate`` run, those totals. For a run that reports none, such as ``generate``, the number of failed pipelines (``1`` when the run failed with no failed pipeline) and the number of warnings it emitted. ``0`` for both when the run was aborted.
   * - ``files_written``
     - integer or null
     - How many files a generate run wrote.

Install events
~~~~~~~~~~~~~~

LHP records ``install.first_seen`` when it mints the install id; the event
carries no fields. It records ``install.upgraded``, which carries
``previous_version``, on the first run after the installed version changes. It
records neither in CI, where it never mints an install id.

Never collected
---------------

LHP never collects any of the following.

- **Identity** — usernames, hostnames, email addresses, machine identifiers, IP
  addresses. The receiver never reads or stores the IP address a request comes
  from. It runs on Cloudflare, whose network sees that address to deliver the
  request and to apply a rate limit (see :ref:`telemetry-endpoint`).
- **Project content** — the names of projects, pipelines, flowgroups, actions,
  tables, catalogs, schemas and environments; paths; YAML, SQL or Python
  content; generated code.
- **Diagnostics** — error and warning messages, environment-variable values,
  secrets, git remotes.
- **Assistant** — prompts, responses, tool arguments, token counts.

LHP reports a failure as the ``LHP-<CATEGORY>-<NUMBER>`` code plus the
exception's class name, and nothing else.

LHP never sends the project name itself either. When a project declares
neither ``project_id`` nor ``bundle.uuid``, LHP sends a salted hash of the name
instead, and that identifier is pseudonymous rather than anonymous: the salt
is a public constant, so anyone who guesses the name can reproduce the hash.
:ref:`Identifiers <telemetry-identifiers>` explains how to replace it with an
opaque value.

.. _telemetry-identifiers:

Identifiers
-----------

Two identifiers travel with an event, and neither identifies a person.

Install id
~~~~~~~~~~

A UUID v4 stored in the state file, created on the first run that records an
event. It distinguishes one installation from another so that twenty commands
from one machine are not read as twenty users. LHP never creates it and never
sends it in CI, where a fresh runner would otherwise look like a new developer
on every build.

Project id
~~~~~~~~~~

The first 32 characters of the lowercase hex digest of
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
     - Opaque per-project identifier, a UUID v4 by convention (any non-empty string is accepted). Used only in hashed form, and only by telemetry: nothing in code generation reads it. Safe to commit — that is the point, because it makes every developer's and every CI run's events roll up to one project.

``lhp init`` writes a fresh ``project_id`` into the scaffolded ``lhp.yaml``,
and writes the same value as ``bundle.uuid`` in ``databricks.yml`` when the
project is scaffolded with bundle support.

.. code-block:: yaml
   :caption: lhp.yaml

   name: my_project
   project_id: 3f1c9e4a-6b2d-4e8f-9a70-5c1d2e3f4a5b
   version: "1.0"

LHP never edits an existing ``lhp.yaml`` to add the key. A project created
before 0.9.2 keeps working and resolves to source 2 or source 3 above; adding
the key yourself changes the project's identifier once, after which it is
stable.

.. _telemetry-endpoint:

Endpoint, transport and retention
---------------------------------

LHP posts events as one ``POST`` request with a JSON body to
``https://telemetry.lakehouse-plumber.dev/v1/events``, a receiver that the LHP
project operates on Cloudflare. The endpoint is fixed at release time, and
``lhp telemetry status`` prints the one this build uses. A build whose endpoint
is unreachable keeps events in the local spool until the caps drop them. The
request carries ``Content-Type: application/json`` and a
``User-Agent: lhp/<version>`` header, and nothing else — no authentication, no
cookies. LHP uses the default ``urllib`` opener, so ``HTTPS_PROXY`` and
``NO_PROXY`` are honored like every other Python HTTP client.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Property
     - Value
   * - Attempts
     - At most one per command, and at most one per event the web IDE emits; sessions that end together share one attempt. LHP starts no attempt while another from the same process is still in flight. There is no retry loop inside a run: a batch kept after a failed upload is offered again by a later upload (see the response rows below).
   * - Timeout
     - 3 seconds.
   * - Added exit latency
     - At most 1 second in the worst case. The upload runs on a daemon thread and the command waits up to one second for it before exiting.
   * - Batch caps
     - At most 500 events and 512 KB per request.
   * - Spool caps
     - At most 500 events and 512 KB (524,288 bytes) on disk. When either cap is exceeded, the oldest events are dropped.
   * - Per-event cap
     - 8 KB; an event larger than that is dropped rather than spooled.
   * - 2xx response
     - The receiver accepted the batch, so LHP removes it from the spool. An empty body is a plain success; a non-empty body that is not JSON came from a proxy or a captive portal rather than the receiver, so LHP keeps the batch.
   * - 400, 413 and any other 4xx except 429
     - LHP discards the batch. The receiver does not accept it however often LHP offers it.
   * - 3xx redirect
     - LHP discards the batch. The endpoint must not redirect: the HTTP client re-sends a redirected upload as a request without its body, so the reply says nothing about the batch.
   * - 429, 5xx, or a connection that failed before the request was fully sent
     - The receiver has not accepted the batch, so LHP keeps it and offers it again on the next command, with no limit other than the spool caps.
   * - Sent but unanswered, or cut off at exit
     - The receiver may already hold the batch, so LHP limits the resends: it offers the batch again and discards an event whose send goes unanswered a second time. A request that times out waiting for the reply, or whose connection drops, is unanswered. So is a batch still in flight when the command exits: LHP leaves it on disk after the one-second exit wait, and the first upload it attempts more than a minute after claiming the batch puts it back in the spool. A resent event keeps its ``event_id``, which identifies the copies.
   * - Remote pause
     - A 2xx response may carry ``{"disabled": true}``, which silences this installation for 24 hours (where a state file exists — never in CI).
   * - Receiver
     - ``telemetry.lakehouse-plumber.dev``. It never redirects, and it reads neither the ``Content-Type`` nor the ``User-Agent`` header.
   * - Delivery
     - A 200 reply means the receiver has queued the batch's events durably. It then writes them to storage and retries on failure, so a storage outage never reaches LHP. It rejects a malformed event on its own and accepts the rest of the batch. Delivery is at least once, so an event can be stored more than once; ``event_id`` identifies the copies.
   * - Rate limit
     - 10 requests per 10 seconds from one IP address, applied by Cloudflare. A request over the limit gets 429 and its batch waits for the next command. CI runners behind one network address translation (NAT) gateway share its IP address and can reach the limit together, which delays their events; a runner discarded at the end of its job discards whatever is still in its spool.
   * - Storage
     - A Databricks workspace on Azure, in the West US 2 region (United States).
   * - IP addresses
     - The receiver never reads the IP address a request comes from, never stores it, and derives no location from it. Cloudflare's network sees the address to deliver the request and to apply the rate limit.
   * - Retention
     - Stored events are kept: no automatic deletion is in place yet. Two things are planned but not in place: a job that deletes raw events after 12 months, and published monthly aggregates that carry no identifiers.

.. note::

   The telemetry upload is the only outbound network request the ``lhp``
   command line makes. (The ``lhp web`` AI assistant talks to whichever
   provider you configure for it, which is a separate feature with its own
   settings.)

Off switches
------------

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
opted-out environment never touches the config directory when LHP records an
event. The ``lhp telemetry`` commands are the exception: ``status`` reads the
state file and the spool, ``show`` reads the spool, and ``on`` and ``off``
write the state file, whatever the switches say.

Blocking ``telemetry.lakehouse-plumber.dev`` at the network level also stops
anything leaving the machine, but it is not an off switch: events are still
recorded and still accumulate in the spool, up to the caps above.

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

- ``lhp telemetry status`` — print whether telemetry is on, which layer decided that, the mode, the config directory, the install id, the endpoint, how many events are waiting to be delivered, and a link to this page. The count includes any batch an upload has claimed and not yet settled.
- ``lhp telemetry show`` — print the ``cli.command`` event this invocation would send, then the newest events waiting to be delivered, in-flight batches included, one compact JSON object per line. Standard output carries only those JSON lines, so it pipes into ``jq`` unchanged; the closing count, and the notice that nothing would be sent when telemetry is off, go to standard error.
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

The receiver's reply to a telemetry upload may name the newest final release
of LHP on PyPI. LHP stores it and, on a later run, prints one line:

.. code-block:: text

   lhp 0.9.3 is available (installed 0.9.2): pip install -U lakehouse-plumber  [LHP_UPDATE_CHECK=off to silence]

LHP prints the line only when the command succeeded, the run is interactive
and outside CI, ``LHP_UPDATE_CHECK`` does not opt out, the stored version is
newer than the installed one, and it has shown no hint in the past 24 hours.
Pre-releases are ignored unless the installed version is itself a
pre-release. The check never makes a request of its own, so it works only
while telemetry is on.

Local files
-----------

LHP resolves the config directory in this order: ``LHP_CONFIG_DIR`` verbatim;
then, on Windows, ``%APPDATA%\lhp`` (or ``~\AppData\Roaming\lhp`` when that
variable is unset); elsewhere ``$XDG_CONFIG_HOME/lhp`` when it holds an
absolute path, otherwise ``~/.config/lhp``.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Path
     - Contents
   * - ``<config-dir>/telemetry.json``
     - Its own ``schema_version``, the install id, your on/off preference, when the file was created, the version last seen, the latest version and hint timestamps behind the update hint, and the expiry of a remote pause (``server_disabled_until``).
   * - ``<config-dir>/telemetry/spool.jsonl``
     - Events waiting to be sent, one JSON object per line. An event whose send went unanswered ends with an extra ``"_unconfirmed":true`` key, which is how LHP counts its unanswered sends. LHP strips the key before sending the event, and ``lhp telemetry show`` does not print it.
   * - ``<config-dir>/telemetry/spool.inflight-<pid>-<ms>.jsonl``
     - A batch an upload has claimed from the spool, named after the claiming process's id and the claim time in milliseconds. LHP deletes it once the upload is settled. One left behind by a process that exited mid-upload goes back into the spool at the first upload attempted more than a minute after the claim.

LHP creates these files, and the directories holding them, with owner-only
permissions. Apart from ``lhp telemetry on`` and ``lhp telemetry off``, which
write the state file whatever the mode, the switches or CI and never mint an
install id, LHP creates none of them in ``LHP_TELEMETRY=log`` mode or on a run
where telemetry is off, and never creates the state file in CI.
``install_id`` is always null in CI, because LHP neither mints nor sends it
there. The spool is still used in CI,
because that is how a CI run's events reach the endpoint.

Deleting any of them is safe: the install id is minted again on the next
recorded event, and a deleted spool or in-flight file discards the events it
was holding.
