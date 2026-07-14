Blueprints
==========

.. meta::
   :description: Reference for LHP blueprints — the blueprint file schema (name, version, description, parameters, flowgroups), the parameter-entry and flowgroup-spec schemas, the instance file schema (use_blueprint, parameters, overrides), and discovery defaults.

A blueprint is a YAML file under ``blueprints/`` that declares parameters and a
list of flowgroup specs. An instance file names the blueprint with
``use_blueprint:`` and supplies parameter values under ``parameters:``; LHP
expands each spec once per instance, so a blueprint with N specs and M instances
yields ``N × M`` generated flowgroups. A blueprint file has the shape::

   name: <blueprint_name>
   version: "1.0"
   description: <text>
   parameters:
     - name: <param_name>
       required: <bool>
       default: <value>
   flowgroups:
     - pipeline: "%{param_name}_..."
       flowgroup: "%{param_name}_..."
       actions:
         - ...

Blueprint file fields
---------------------

Top-level fields of a blueprint file.

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
     - Blueprint identifier. Unique across the project; referenced by ``use_blueprint:`` in instance files.
   * - ``version``
     - string
     - No
     - ``"1.0"``
     - Version string for the blueprint.
   * - ``description``
     - string
     - No
     - —
     - Free-text description of the blueprint.
   * - ``parameters``
     - list[mapping]
     - No
     - ``[]``
     - Parameter declarations (entry schema below).
   * - ``flowgroups``
     - list[mapping]
     - Yes
     - —
     - Flowgroup specs. Each entry is expanded once per instance (spec schema below).

Parameter fields
----------------

Keys of a single entry in the ``parameters:`` list.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Key
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Parameter name. An instance binds a value to this key, and ``%{name}`` resolves to that value inside the spec.
   * - ``required``
     - bool
     - No
     - ``false``
     - When ``true``, every instance must supply this parameter or validation fails.
   * - ``default``
     - any
     - No
     - —
     - Value used when ``required: false`` and the instance omits the parameter.
   * - ``description``
     - string
     - No
     - —
     - Human-readable note.

Flowgroup spec fields
---------------------

Keys of a single entry in the ``flowgroups:`` list. Same schema as a flowgroup
file's fields.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``pipeline``
     - string
     - Yes
     - —
     - Target pipeline name. Template string; only ``%{var}`` is legal here — ``${...}`` tokens are rejected.
   * - ``flowgroup``
     - string
     - Yes
     - —
     - Target flowgroup name. Template string; only ``%{var}`` is legal here — ``${...}`` tokens are rejected.
   * - ``job_name``
     - string
     - No
     - —
     - Optional job name passed through to the flowgroup.
   * - ``variables``
     - mapping
     - No
     - —
     - Local ``%{var}`` values available within the spec.
   * - ``presets``
     - list[string]
     - No
     - ``[]``
     - Preset names applied to the spec's actions.
   * - ``use_template``
     - string
     - No
     - —
     - Name of a template expanded into the spec's actions.
   * - ``template_parameters``
     - mapping
     - No
     - —
     - Values supplied to the referenced template.
   * - ``actions``
     - list[mapping]
     - No
     - ``[]``
     - Inline action list. Same schema as flowgroup actions.
   * - ``operational_metadata``
     - bool or list[string]
     - No
     - —
     - Toggle metadata columns, or a list of metadata column names.

.. code-block:: yaml

   name: erp_ingestion
   version: "1.0"
   description: "ERP ingestion, one instance per site"
   parameters:
     - name: site_name
       required: true
     - name: partition_key
       required: false
       default: order_date
   flowgroups:
     - pipeline: "%{site_name}_erp_raw"
       flowgroup: "%{site_name}_orders"
       actions:
         - name: load_orders
           type: load
           source:
             type: cloudfiles
             path: "${landing_volume}/%{site_name}/orders"
             format: json
           target: v_orders_raw
         - name: write_orders
           type: write
           source: v_orders_raw
           write_target:
             type: streaming_table
             catalog: "${catalog}"
             schema: bronze
             table: "%{site_name}_orders_raw"
             partition_columns:
               - "%{partition_key}"

Instance file fields
--------------------

An instance file names one blueprint and supplies its parameter values. The
file is strict — unknown top-level keys are rejected. The legacy ``blueprint:``
key with flat parameter siblings is deprecated and removed in V0.9; use
``use_blueprint:`` with a nested ``parameters:`` block.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``use_blueprint``
     - string
     - Yes
     - —
     - Name of the blueprint to instantiate. Must match a discovered blueprint's ``name``.
   * - ``parameters``
     - mapping
     - No
     - —
     - Flat ``parameter_name: value`` map. Every required blueprint parameter must appear here.
   * - ``overrides``
     - mapping
     - No
     - —
     - Reserved for future use; accepted but currently unused.

.. code-block:: yaml

   use_blueprint: erp_ingestion
   parameters:
     site_name: apac_sg
     partition_key: order_date

Discovery
---------

Blueprint files are discovered under ``blueprint_include``; instance files under
``instance_include``. Both default to the globs below and are configurable in
``lhp.yaml``.

.. code-block:: yaml

   blueprint_include: ["blueprints/**/*.yaml", "blueprints/**/*.yml"]
   instance_include:  ["pipelines/**/*.yaml",  "pipelines/**/*.yml"]

Discovery routes by content shape, not by path: a document with top-level
``parameters:`` and ``flowgroups:`` (and no ``actions:``) is a blueprint; a
document with ``use_blueprint:`` (or legacy ``blueprint:``) is an instance.
