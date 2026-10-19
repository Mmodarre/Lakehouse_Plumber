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

.. seealso::

   How-to guide: :doc:`/guides/reuse-and-scale/templates`.

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
   * - ``render_sql_files``
     - bool
     - No
     - ``false``
     - When ``true``, the ``sql_path`` files of this template's actions are rendered with the template parameters, so they can use ``{{ param }}`` too. See :ref:`template-sql-files`.
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
     - When ``true``, the invoking flowgroup must supply this key explicitly or generation fails. This check runs before defaults: a declared default does not satisfy ``required: true``.
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
     - Path under ``templates/``, without ``.yaml``, of the template to expand. For ``templates/ingestion/csv.yaml``, use ``ingestion/csv``. This is independent of the template's declared ``name``.
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

.. _template-sql-files:

Parameters in SQL files
-----------------------

Template rendering covers the template's YAML only. A ``sql_path`` value can
use a parameter (``sql_path: "sql/gold/{{ table }}.sql"``), but the file it
points at is read later and gets only ``${token}`` substitution. Set
``render_sql_files: true`` to render those files with the template parameters
as well:

.. code-block:: yaml

   name: gold_materialized_view
   render_sql_files: true
   parameters:
     - name: table
       required: true
     - name: silver_schema
       default: silver
   actions:
     - name: "write_{{ table }}"
       type: write
       write_target:
         type: materialized_view
         catalog: "${catalog}"
         schema: gold
         table: "{{ table }}"
         sql_path: "sql/gold/{{ table }}.sql"

.. code-block:: text

   -- sql/gold/orders.sql
   SELECT * FROM ${catalog}.{{ silver_schema }}.orders WHERE __END_AT IS NULL

Every rendered action that references a ``sql_path`` file (an SQL load's
``source``, an SQL transform, or a materialized view's ``write_target``) gets
the parameters, with defaults applied, as its ``sql_parameters``. Keys the
action sets in ``sql_parameters`` itself take precedence. A flowgroup action
that does not come from a template can set ``sql_parameters`` directly.

The file is rendered as a Jinja template first, then ``${token}`` and
``${secret:...}`` references are substituted, so environment values keep
coming from ``substitutions/<env>.yaml`` and a parameter value may itself hold
a token. Rendering differs from the YAML rendering above in three ways:

- It is strict: a name that is not a parameter fails ``lhp validate`` and
  ``lhp generate`` with ``LHP-CFG-071``, instead of rendering as an empty
  string.
- The whole file is a template, so literal ``{{``, ``{%`` or ``{#`` in the SQL
  must be wrapped in ``{% raw %}...{% endraw %}``.
- It runs in Jinja's immutable sandbox.

Dependency analysis (``lhp dag``) parses the rendered SQL, so table references
built from parameters still order the pipelines and flowgroups. It matches a
reference to the table's producer by its text, without substituting
``${token}`` values, so write each table name the same way on both sides. If
the producing write target uses ``${silver_schema}``, give the parameter that
default (``default: "${silver_schema}"``) rather than the literal schema name;
otherwise the reference is reported as an external source and the pipelines
lose their ordering. A file of an action without ``sql_parameters`` is read
exactly as before.

Rendering and authoring behavior
--------------------------------

The runtime opens ``templates/<use_template>.yaml``. Nested paths work; ``.yml``
files may be viewed and edited, but are not invocable by the current runtime.
Two files may declare the same ``name``: their paths still identify different
templates. Renaming the declared name does not rename the invocation reference.

Parameter ``type`` is optional advisory metadata for tools, not an enforced
runtime contract. Omission is distinct from an explicit ``null``, ``false``,
``0``, ``""``, ``[]`` or ``{}``. An optional parameter without a default remains
undefined when omitted; a simple undefined Jinja interpolation currently
renders as an empty string. Additional supplied parameters are permitted.

Rendering visits scalar string values recursively inside each action. A scalar
is evaluated by Jinja only when it contains both ``{{`` and ``}}``. Inline
filters, loops and conditions can therefore be used within an eligible scalar;
a block containing only ``{% ... %}`` remains literal. Mapping keys are not
rendered. Whole-document Jinja loops and conditional insertion/removal of action
entries are not supported.

Rendered results use the engine's compatibility conversion rules. Lists,
objects, booleans, integers and ``None`` can become native values. Decimal and
scientific-notation results remain strings; a numeric-looking string such as
``"001"`` can become the integer ``1``. Use the expanded preview to inspect the
actual output rather than treating parameter ``type`` as a coercion rule.

The web editor previews an unsaved template without saving it. Expanded preview
checks template rendering and action-model construction. Resolved preview also
uses saved project configuration, presets and environment substitutions through
the normal flowgroup resolver; it does not execute Databricks code. Required
sample values and resolved-preview context are requested separately from source
errors. Sample values are not saved as defaults.

Preview uses an immutable Jinja sandbox, limits source to 512 KiB, output to
2 MiB and execution to ten seconds. Expressions that access unsafe Python
attributes or mutate objects are refused with a preview-specific diagnostic;
this does not alter the generator's rendering behavior. ``.yml`` sources and
YAML values outside the JSON-compatible preview contract remain editable in
Code. Preview reads only project-contained dependencies and reports a stale
result if saved files change during resolution.
