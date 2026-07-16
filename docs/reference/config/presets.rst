Presets
=======

.. meta::
   :description: Reference for LHP presets — the preset file schema (name, version, extends, description, defaults) and the defaults structure keyed by action type and sub-type, with a minimal YAML example.

A preset is a YAML file in the project's ``presets/`` directory that supplies
default configuration merged into actions by type. A flowgroup or template
applies presets by listing their names under ``presets:``; presets are merged in
list order. A preset file has the shape::

   name: <preset_name>
   version: "1.0"
   extends: <parent_preset_name>
   description: <text>
   defaults:
     load_actions:
       <source_type>:
         ...
     transform_actions:
       <transform_type>:
         ...
     write_actions:
       <target_type>:
         ...

.. seealso::

   Concept: :doc:`/concepts/presets-templates-blueprints`.

Top-level fields
----------------

.. list-table::
   :header-rows: 1
   :widths: 20 14 12 16 38

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Preset identifier. Flowgroups and templates reference the preset by this name.
   * - ``version``
     - string
     - No
     - ``"1.0"``
     - Version string for the preset.
   * - ``extends``
     - string
     - No
     - —
     - Name of a parent preset. This preset's ``defaults`` are merged over the parent's resolved ``defaults``.
   * - ``description``
     - string
     - No
     - —
     - Free-text description of the preset.
   * - ``defaults``
     - mapping
     - No
     - —
     - Default configuration keyed by action type (see below).

``defaults`` keys
-----------------

Each key under ``defaults`` supplies defaults merged into matching actions by
their type and sub-type.

.. list-table::
   :header-rows: 1
   :widths: 28 30 42

   * - Key
     - Matches
     - Merge target
   * - ``load_actions.<source_type>``
     - Load actions whose ``source.type`` equals ``<source_type>`` (e.g. ``cloudfiles``, ``delta``, ``jdbc``).
     - Deep-merged into the action's ``source`` mapping.
   * - ``transform_actions.<transform_type>``
     - Transform actions whose ``transform_type`` equals ``<transform_type>`` (e.g. ``sql``, ``python``).
     - Merged into the action's top-level fields.
   * - ``write_actions.<target_type>``
     - Write actions whose ``write_target.type`` equals ``<target_type>`` (e.g. ``streaming_table``, ``materialized_view``).
     - Deep-merged into the action's ``write_target`` mapping. A ``schema_suffix`` value is appended to the target ``schema``.
   * - ``defaults``
     - The flowgroup itself.
     - Each key sets a flowgroup-level field default.

Example
-------

.. code-block:: yaml

   name: bronze_defaults
   version: "1.0"
   description: "Standard bronze-layer defaults"
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.schemaEvolutionMode: addNewColumns
     write_actions:
       streaming_table:
         table_properties:
           quality: bronze

A preset may build on another with ``extends``:

.. code-block:: yaml

   name: bronze_cloudfiles
   version: "1.0"
   extends: bronze_defaults
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.maxFilesPerTrigger: 200
