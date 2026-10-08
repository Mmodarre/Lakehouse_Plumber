Schema transform (schema)
=========================

Renames, casts, and (in ``strict`` mode) filters columns of the source view via a
schema mapping, emitting a ``@dp.temporary_view``. Requires exactly one of
``schema_inline`` or ``schema_file``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: enforce_schema
     type: transform
     transform_type: schema
     source: v_orders
     target: v_orders_typed
     schema_inline: |
       order_id: BIGINT
       amount: DECIMAL(18,2)
     enforcement: strict

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Input view name. Must be a plain string (the nested ``source.view`` form is not supported).
   * - ``schema_inline``
     - string
     - Cond.
     - —
     - Inline schema definition (arrow or structured YAML). Exactly one of ``schema_inline`` / ``schema_file``.
   * - ``schema_file``
     - string
     - Cond.
     - —
     - Path to an external schema file. Exactly one of ``schema_inline`` / ``schema_file``.
   * - ``enforcement``
     - string
     - No
     - ``permissive``
     - ``strict`` keeps only defined columns; ``permissive`` transforms defined columns and passes the rest through unchanged.
   * - ``readMode``
     - string
     - No
     - ``stream``
     - ``batch`` or ``stream``.

Schema definitions use arrow syntax, one entry per column:

- ``old_col -> new_col: BIGINT`` — rename and cast.
- ``old_col -> new_col`` — rename only.
- ``col: DECIMAL(18,2)`` — cast in place.
- ``col`` — pass through / explicit keep (``strict`` mode).

A ``$`` is allowed in a source-column name (left of ``->``, a cast-only ``col``,
or a pass-through ``col``). A rename target (right of ``->``) must be a clean
identifier of letters, digits, and underscores; anything else raises
``LHP-VAL-011``.

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/schema`
- :doc:`All transform actions </reference/actions/transform>`
