Action-level fields
-------------------

These fields apply to every load sub-type. ``name``, ``type``, ``source`` and ``target`` are required.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Unique action name within the flowgroup.
   * - ``type``
     - string
     - Yes
     - —
     - Always ``load``.
   * - ``source``
     - mapping
     - Yes
     - —
     - Source configuration. ``source.type`` selects the sub-type. Use a mapping for every load type, including SQL.
   * - ``target``
     - string
     - Yes
     - —
     - Name of the temporary view this action creates.
   * - ``readMode``
     - string
     - No
     - sub-type
     - ``batch`` or ``stream``; selects ``spark.read`` vs ``spark.readStream``. Default and constraints vary by sub-type (below). Ignored by ``sql``, ``python``, and ``jdbc``.
   * - ``description``
     - string
     - No
     - auto
     - Docstring for the generated view function.
   * - ``operational_metadata``
     - bool or list[string]
     - No
     - —
     - Explicit column names to add; action-level ``false`` disables inherited selection. ``true`` alone adds no columns. See :doc:`/reference/config/operational-metadata`.
   * - ``depends_on``
     - list[string]
     - No
     - —
     - Extra upstream table or view references added to the dependency graph. See :doc:`/reference/dependency-analysis`.
