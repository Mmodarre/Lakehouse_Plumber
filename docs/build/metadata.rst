Add table metadata
==================

Operational columns and Unity Catalog tags serve different purposes. Columns
become part of the data; tags describe tables and columns in Unity Catalog.

Add an ingestion timestamp
--------------------------

Select built-in metadata columns on the action that should add them:

.. code-block:: yaml

   - name: load_orders
     type: load
     source:
       type: cloudfiles
       path: "${landing_path}/orders/*.json"
       format: json
     target: v_orders
     operational_metadata: [_ingestion_timestamp]

To define your own column expression, add it under
``operational_metadata.columns`` in ``lhp.yaml`` and select its name on the
flowgroup or action. Use an explicit list; ``true`` alone selects no columns.
See :doc:`/reference/config/operational-metadata` for built-in names, applicable
target types and selection rules.

Apply Unity Catalog tags
------------------------

Add a ``tags`` mapping to a table's ``write_target``:

.. code-block:: yaml

   write_target:
     type: streaming_table
     catalog: "${catalog}"
     schema: "${bronze_schema}"
     table: orders
     tags:
       owner: data_platform
       quality: bronze

For shared table and column definitions, use a
:doc:`schema and tags file </reference/config/schema-files>`.
Project hook controls belong in ``lhp.yaml`` under
:doc:`uc_tagging </reference/config/uc-tagging>`.

Generate and inspect the resulting Python before deployment. After a pipeline
run, check the table's tags and its event log: tag application uses the run-as
identity's permissions and failures appear as hook warnings. See
:doc:`/operate/troubleshooting` for diagnosis.
