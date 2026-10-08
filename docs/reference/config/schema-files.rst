Schema and tag files
====================

Choose the grammar by the consumer. A table schema describes stored columns;
a schema transform describes how to rename and cast input fields.

.. _uc-tags-file:

Table schema and tags
---------------------

``tags_file`` and ``table_schema`` both point at a **unified schema/tags file**
(project convention ``schemas/<table>.yaml``). One file can serve both fields —
``table_schema`` reads the column types, ``tags_file`` reads the UC tags — or
they can point at different files. ``tags_file`` is mutually exclusive with an
inline ``tags`` mapping; ``table_schema`` is orthogonal and combines with
either.

The file is a mapping whose recognised keys are an optional identifier
(``table``, or its alias ``name``), a table-level ``tags`` mapping, and a
``columns`` list. The legacy schema keys ``version``, ``description``, and
``primary_key`` are tolerated and ignored. Each ``columns`` entry has a required
``name`` plus optional ``type``, ``nullable``, and ``comment`` (read by
``table_schema``) and ``tags`` (read by ``tags_file``). ``type`` is required
when the file is used as ``table_schema``; it is optional in a tags-only file. A
tag value of ``""``, ``~``, or an omitted value is a key-only tag, at either
level.

.. code-block:: yaml
   :caption: schemas/orders.yaml — point BOTH table_schema and tags_file here

   table: orders               # optional identifier; 'name' is an accepted alias
   tags:                       # table-level UC tags (read by tags_file)
     team: platform
     cost_center: "1234"
   columns:
     - name: email
       type: STRING            # required for table_schema use; optional tags-only
       nullable: false         # schema use
       comment: "PII"          # schema use
       tags:                   # column-level UC tags (read by tags_file)
         pii: high
     - name: region
       type: STRING
       tags:
         classification: public

.. code-block:: yaml

   - name: write_orders_silver
     type: write
     source: v_orders_bronze
     write_target:
       type: streaming_table
       catalog: main
       schema: silver
       table: orders
       table_schema: schemas/orders.yaml   # column types (+ nullable/comment)
       tags_file: schemas/orders.yaml       # same file: UC table + column tags

``lhp generate`` raises ``LHP-CFG-067`` when the file is not a mapping, carries
an unknown top-level key (the retired ``column_tags`` key is now rejected as
unknown), has a ``columns`` that is not a list, or a ``columns`` entry that is
not a mapping or carries an unknown key. Read as a ``tags_file`` it also rejects
a wrong-typed ``table``/``name``/``tags``, a column ``name`` that is missing,
empty, or duplicated, and a per-column ``tags`` that is not a mapping.

The identifier is optional. When present in a ``tags_file`` it should equal the
write target's table name; a mismatch logs a warning (``LHP-CFG-068``) and
generation proceeds using the write target's table. A file that declares both
``table`` and ``name`` with differing values also warns ``LHP-CFG-068`` (with
``table`` winning). A missing ``tags_file`` raises ``LHP-IO-001`` with the
searched locations. Under ``--sandbox`` the identifier cross-check is skipped
(sandbox renames the write target's table), and the file's tags are applied to
the renamed table.

Because a preset's ``tags`` default deep-merges into the write target before
validation, pairing a preset ``tags`` default with a flowgroup ``tags_file`` is
rejected as both-set (``cannot specify both 'tags' and 'tags_file'``).

.. note::

   A file set as ``table_schema`` but **not** also wired as ``tags_file`` has
   its UC ``tags`` silently dropped — the schema reader consumes only the column
   types. ``lhp generate`` emits an ``LHP-CFG-069`` warning in that case (from
   the streaming-table and materialized-view writes only, never the cloudfiles
   load path); point ``tags_file`` at the same file to apply the tags.
   (``lhp validate`` runs no code generation, so the warning surfaces at
   generate time.)


Schema-transform definitions
----------------------------

``schema_file`` / ``schema_inline`` on a ``transform_type: schema`` action use
the transform's mapping grammar. See :doc:`/reference/actions/transform/schema` and
:doc:`the schema-transform guide </guides/transform/schema>` for the accepted
inline/file formats, enforcement modes and a complete example. Do not pass a
table-schema file to this consumer solely because both files are YAML.
