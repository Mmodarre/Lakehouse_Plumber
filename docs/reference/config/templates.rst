Templates
=========

.. meta::
   :description: Reference for LHP template files — top-level fields (name, version, description, presets, parameters, actions), the parameter-entry schema, and the use_template / template_parameters keys a flowgroup uses to invoke one.

A template is a YAML file under ``templates/`` that declares a list of
parametrised actions. A flowgroup invokes it with ``use_template:`` and supplies
``template_parameters:``; LHP renders the ``{{ param }}`` placeholders with those
values and expands the template into the flowgroup's actions.

.. code-block:: yaml

   name: <template_name>
   version: "1.0"
   presets: []
   parameters:
     - name: <param_name>
       required: <bool>
       default: <value>
   actions:
     - name: "<action_{{ param_name }}>"
       type: <load|transform|write|test>
       ...

Template file fields
--------------------

Top-level fields of a template file. Only ``name`` is required; every other
field has a default.

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
     - Template identifier, surfaced in validation errors.
   * - ``version``
     - string
     - No
     - ``"1.0"``
     - Version tag for change tracking.
   * - ``description``
     - string
     - No
     - —
     - Human-readable summary of the template.
   * - ``presets``
     - list[string]
     - No
     - ``[]``
     - Preset names applied to the actions this template generates.
   * - ``parameters``
     - list[mapping]
     - No
     - ``[]``
     - Parameter declarations (entry schema below).
   * - ``actions``
     - list[mapping]
     - No
     - ``[]``
     - Action patterns carrying ``{{ param }}`` placeholders. Same schema as flowgroup actions.

Parameter fields
----------------

Keys of a single entry in the ``parameters:`` list. The engine reads only
``name``, ``required``, and ``default``.

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
     - Parameter name. Referenced as ``{{ name }}`` in the template body and supplied by key under ``template_parameters:``.
   * - ``required``
     - bool
     - No
     - ``false``
     - When ``true``, the invoking flowgroup must supply this parameter or generation fails.
   * - ``default``
     - any
     - No
     - —
     - Value used when the flowgroup omits the parameter.
   * - ``description``
     - string
     - No
     - —
     - Human-readable note. Not read by the engine.

Invoke from a flowgroup
-----------------------

A flowgroup expands a template through these two fields.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``use_template``
     - string
     - No
     - —
     - File name (stem, without ``.yaml``) of a template under ``templates/`` to expand into this flowgroup.
   * - ``template_parameters``
     - mapping
     - No
     - —
     - Values supplied to the template's parameters, keyed by parameter name. Merged over declared defaults.

.. code-block:: yaml

   name: csv_to_bronze
   version: "1.0"
   parameters:
     - name: table_name
       required: true
     - name: file_format
       default: csv
   actions:
     - name: "load_{{ table_name }}"
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "${landing_volume}/{{ table_name }}/*.{{ file_format }}"
         format: "{{ file_format }}"
       target: "v_{{ table_name }}_raw"

.. code-block:: yaml

   pipeline: bronze
   flowgroup: customer_ingestion
   use_template: csv_to_bronze
   template_parameters:
     table_name: customer
     file_format: csv
