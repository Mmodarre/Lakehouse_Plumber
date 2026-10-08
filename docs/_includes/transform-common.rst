Common fields
-------------


Fields accepted by every ``transform_type``. Type-specific fields are listed in
the options above.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Action name, unique within the flowgroup.
   * - ``type``
     - string
     - Yes
     - —
     - Always ``transform``.
   * - ``transform_type``
     - string
     - Yes
     - —
     - One of ``sql``, ``python``, ``data_quality``, ``temp_table``, ``schema``.
   * - ``target``
     - string
     - Yes
     - —
     - Name of the produced view (or temporary table). Required for all transforms.
   * - ``source``
     - string / list / mapping
     - Yes
     - —
     - Upstream view name(s). Accepted shape and requirement vary per type — see this type's options.
   * - ``description``
     - string
     - No
     - auto
     - Comment on the generated view/table. Defaults to an auto-generated string.
   * - ``operational_metadata``
     - bool / list
     - No
     - —
     - Add operational metadata columns; a list selects column names. Honored by ``sql``, ``python``, ``data_quality``, ``temp_table``; ignored by ``schema``.
   * - ``depends_on``
     - list
     - No
     - —
     - Extra upstream table or view references added to the dependency graph. See :doc:`/reference/dependency-analysis`.
