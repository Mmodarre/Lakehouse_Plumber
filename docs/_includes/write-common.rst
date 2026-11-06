Action fields
-------------

These fields sit on the action, alongside ``write_target``.

.. list-table::
   :header-rows: 1
   :widths: 22 16 16 46

   * - Field
     - Type
     - Default
     - Requirement and behaviour
   * - ``name``
     - string
     - required
     - Action name, unique within the flowgroup.
   * - ``type``
     - string
     - required
     - Always ``write``.
   * - ``source``
     - string / list
     - conditional
     - Input view(s). Standard streaming writes and most sinks accept a list. CDC, replace, replace_where and foreachbatch use one view. Snapshot CDC takes its source inside ``snapshot_cdc_config``. Materialized views require one of action ``source``, target ``sql`` or target ``sql_path``.
   * - ``write_target``
     - mapping
     - required
     - Target settings; ``type`` selects ``streaming_table``, ``materialized_view`` or ``sink``.
   * - ``description``
     - string
     - derived
     - Comment/docstring for the generated flow or view.
   * - ``readMode``
     - string
     - ``stream``
     - ``stream`` or ``batch`` for standard streaming-table append flows. Replace requires streaming; replace_where reads in batch and rejects ``stream``. Materialized views use batch queries; sinks use streaming reads. The read must match the kind of the view it reads, or ``lhp validate`` fails with ``LHP-VAL-070``; a batch read in a standard streaming-table flow also needs ``once: true``.
   * - ``once``
     - bool
     - ``false``
     - Emits ``once=True`` for standard append and CDC flows. Not emitted for snapshot CDC, replace, replace_where, materialized views or sinks.
   * - ``operational_metadata``
     - bool / list
     - —
     - Accepted on the model; writes do not add metadata columns. Select columns on load or supported transform actions instead.
   * - ``depends_on``
     - list[string]
     - —
     - Extra upstream table/view references. See :doc:`/reference/dependency-analysis`.
