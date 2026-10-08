Unity Catalog tagging
=====================

**Project settings:** ``uc_tagging`` in ``lhp.yaml``.
**Table settings:** ``tags`` or ``tags_file`` under ``write_target``.
For usage, see :doc:`/build/metadata`.


.. versionadded:: 0.9.1
   The ``tags_file`` field and column-level UC tags, read from a unified
   schema/tags file that ``table_schema`` and ``tags_file`` can share.

A ``streaming_table`` or ``materialized_view`` write target can carry Unity
Catalog (UC) tags at the table level (a ``tags`` mapping on ``write_target``, or the
``tags:`` block of a ``tags_file``) and at the column level (the per-column
``tags:`` inside a ``tags_file`` — see :ref:`uc-tags-file` below). Because
Lakeflow Spark Declarative Pipelines (SDP) cannot set UC tags as part of table
creation, Lakehouse Plumber (LHP) collects every declared tag and emits one
per-pipeline ``_uc_tagging_hook.py`` that applies them through the Unity Catalog
*Entity Tag Assignments* REST API rather than table DDL.

.. code-block:: yaml

   - name: write_orders_silver
     type: write
     source: v_orders_bronze
     write_target:
       type: streaming_table
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: orders
       tags:
         team: platform
         cost_center: "1234"
         pii: ""            # key-only tag: "", ~, or an omitted value

The feature is on by default; declaring ``tags`` (or ``tags_file``) opts a table
in. Set ``uc_tagging.enabled: false`` in ``lhp.yaml`` to disable it. Only the
table-creating action is tagged (``create_table: true``); temporary tables and
sinks are excluded.

uc_tagging config block
-----------------------

The optional ``uc_tagging`` block in ``lhp.yaml`` tunes the hook:

.. list-table::
   :header-rows: 1
   :widths: 30 14 12 44

   * - Key
     - Type
     - Default
     - Notes
   * - ``enabled``
     - bool
     - ``true``
     - Set ``false`` to disable tag generation entirely.
   * - ``remove_undeclared_tags``
     - bool
     - ``false``
     - ``false`` is additive (create/update declared tags only); ``true``
       reconciles to the declared set, deleting existing tags whose key is not
       declared for a managed entity. An explicit ``tags: {}`` then means
       "managed with an empty set".
   * - ``tag_update_concurrency``
     - int
     - ``16``
     - Max concurrent tag operations (range 1–20).
   * - ``max_allowable_consecutive_failures``
     - int or null
     - ``null``
     - Consecutive-failure budget passed to the hook's ``@dp.on_event_hook``.
       Must be an integer >= 0 or ``null``. ``null`` (the default, matching
       Lakeflow SDP) means there is no limit and the hook is never disabled; an
       integer makes SDP disable the hook after that many consecutive failures.

How the hook applies tags
-------------------------

The generated hook runs as a ``@dp.on_event_hook`` during the pipeline update.
It fires on ``update_progress`` ``RUNNING`` — when streaming tables already
exist — and on the terminal states, which catch materialized views that
materialize later; each entity is tagged at most once. Tagging is best-effort
and non-blocking: tag-write failures surface as pipeline event-log warnings and
never fail the update (event hooks cannot). Key-only tags use ``""``, ``~``, or
an omitted value.

A run raises at most twice — one combined ``RUNNING`` warning and one terminal
warning. By default there is no failure limit: a hook that keeps failing keeps
raising until the cause is fixed, and tags are applied again on the next
successful run. Set ``max_allowable_consecutive_failures`` to have SDP disable
the hook after that many consecutive failures; Databricks documents that a
disabled hook does not process new events until the pipeline is restarted.

Existing tag state is read once at module import with a single
``system.information_schema`` query (``table_tags`` ``UNION ALL``
``column_tags``); a read failure is caught at import, re-raised as a warning on
the first ``RUNNING`` event, and tagging then proceeds create-only.

.. important::

   Unity Catalog requires the pipeline's run-as identity to hold ``APPLY TAG``
   on the table and ``ASSIGN`` on any required governed tags to write tags via
   the REST API, plus ``USE CATALOG``, ``USE SCHEMA``, and ``SELECT`` on
   ``system.information_schema`` to read existing tag state. LHP does not verify
   these grants; a missing grant surfaces as an event-log warning at run time.

Tagging error handling
----------------------

Tagging errors never fail the pipeline. Event hooks cannot fail an update, so a
tag-write failure — a missing ``APPLY TAG`` grant, an unassignable governed tag,
a table that never materialized — surfaces as a warning rather than an error: the
update still completes successfully while the tags go unapplied. Each failure is
raised inside the hook and recorded in the pipeline event log as a
``hook_progress`` event whose state is ``FAILED``, alongside the
``[LHP UC Tagging] ERROR``/``WARNING`` messages the hook prints.

By default the hook keeps retrying on every subsequent update, because
``max_allowable_consecutive_failures`` is ``null`` (no limit). Set it to an
integer to have SDP disable the hook after that many consecutive failures.
Either way the update's final status stays successful, so a run that failed to
apply tags looks the same as a clean one unless you read the warnings.

In production, detect tagging failures with a scheduled Databricks SQL alert
over the unioned event log table that LHP monitoring writes (see
:doc:`/reference/config/monitoring` for the configuration and
:doc:`/guides/ops/monitoring` for setting it up). That table's fully qualified
name is ``{catalog}.{schema}.{streaming_table}``, where ``streaming_table``
defaults to ``all_pipelines_event_log`` and the catalog and schema inherit from
``event_log`` unless ``monitoring`` overrides them. The following query returns
one row per tagging failure in the last 24 hours, with the underlying exception
messages:

.. code-block:: sql

   SELECT timestamp, origin.pipeline_name,
          transform(error.exceptions, exc -> exc.message) AS messages
   FROM IDENTIFIER(:catalog || '.' || :schema || '.all_pipelines_event_log')
   WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
     AND event_type = 'hook_progress'
     AND details:hook_progress:hook_name = 'uc_tagging_hook'
     AND details:hook_progress:state = 'FAILED'
   ORDER BY timestamp DESC;

Run it on a daily schedule and notify when the row count exceeds zero — see
`Create an alert <https://docs.databricks.com/aws/en/sql/user/alerts/create>`_
for the trigger condition, schedule, and notification destinations. Supply
``catalog`` and ``schema`` as query parameters on the alert, and edit the table
name in the string literal if ``monitoring.streaming_table`` is overridden.

See :doc:`schema-files` for shared table/column schema and tag files.
