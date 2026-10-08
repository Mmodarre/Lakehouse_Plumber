Flowgroup YAML
==============

**Files:** YAML files discovered under ``pipelines/``. Each document describes
one flowgroup. Use ``---`` to put multiple flowgroups in one file.
For composition examples, see :doc:`/build/compose`.

Minimal example
---------------

.. code-block:: yaml

   pipeline: bronze
   flowgroup: orders
   actions:
     - name: read_orders
       type: load
       source:
         type: sql
         sql: SELECT 1 AS order_id
       target: v_orders
     - name: write_orders
       type: write
       source: v_orders
       write_target:
         type: streaming_table
         catalog: main
         schema: bronze
         table: orders

This loads a temporary view and writes an output table. A normal flowgroup
needs at least one load and one write action. A materialized-view write with
``sql``/``sql_path`` or a snapshot-CDC write with ``source_function`` can supply
its own data without a load. Test-only flowgroups are also supported. See
:doc:`/reference/actions/write` for targets.

Fields
------

.. list-table::
   :header-rows: 1
   :widths: 26 17 20 37

   * - Field
     - Type
     - Required / default
     - Meaning
   * - ``pipeline``
     - string
     - Required
     - Generated pipeline name. All flowgroups with this name contribute to the same pipeline.
   * - ``flowgroup``
     - string
     - Required
     - Flowgroup identifier; also used for its generated Python filename.
   * - ``job_name``
     - string or null
     - unset
     - Orchestration job grouping, consumed by ``lhp dag``. See :doc:`jobs`.
   * - ``variables``
     - mapping[string, string]
     - unset
     - Local ``%{name}`` substitutions. See :doc:`substitutions` for resolution order.
   * - ``presets``
     - list[string]
     - ``[]``
     - Presets applied in order. See :doc:`presets` for merge behaviour.
   * - ``use_template``
     - string or null
     - unset
     - Template name to expand into actions. See :doc:`templates`.
   * - ``template_parameters``
     - mapping
     - unset
     - Values for the selected template's declared parameters.
   * - ``actions``
     - list[mapping]
     - ``[]`` in the model
     - Explicit actions or template output. Use the references below for the required fields of each action.
   * - ``operational_metadata``
     - boolean or list[string]
     - unset
     - Column selection. Use explicit lists; see :doc:`operational-metadata` for selection and disabling rules.

Action syntax
-------------

- :doc:`Load actions </reference/actions/load>`: ``source.type`` selects the reader.
- :doc:`Transform actions </reference/actions/transform>`: ``transform_type`` selects the operation.
- :doc:`Write actions </reference/actions/write>`: ``write_target.type`` selects the output.
- :doc:`Test actions </reference/actions/test>`: ``test_type`` selects the assertion.

File paths do not determine pipeline membership; ``pipeline`` does. Reference
temporary views only within their pipeline. Cross-pipeline dependencies use
persisted tables. See :doc:`/reference/dependency-analysis` for ``depends_on``.

.. toctree::
   :maxdepth: 1
   :hidden:

   Dependency declarations </reference/dependency-analysis>
