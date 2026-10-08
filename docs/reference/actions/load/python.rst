Python function (python)
========================

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: python``.

Calls a Python function that returns a DataFrame. Fields live under
``source:``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_external
     type: load
     source:
       type: python
       module_path: "loaders/external.py"
       function_name: get_df
       parameters:
         region: "${region}"
     target: v_external

Source fields
-------------

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``python``.
   * - ``module_path``
     - string
     - Yes
     - —
     - Path to a ``.py`` file (relative to project root); must end in ``.py``.
   * - ``function_name``
     - string
     - No
     - ``get_df``
     - Entry function, called as ``fn(spark, parameters) -> DataFrame``.
   * - ``parameters``
     - mapping
     - No
     - ``{}``
     - Passed as the function's second argument.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/python>` or
:doc:`choose another load source </reference/actions/load>`.
