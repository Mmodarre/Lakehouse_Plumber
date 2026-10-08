Custom DataSource (custom_datasource)
=====================================

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: custom_datasource``.

Reads through a user-supplied PySpark ``DataSource`` class. Fields live under
``source:``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_custom
     type: load
     source:
       type: custom_datasource
       module_path: "sources/my_source.py"
       custom_datasource_class: MyDataSource
       options:
         endpoint: "${api_endpoint}"
     target: v_custom

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
     - Must be ``custom_datasource``.
   * - ``module_path``
     - string
     - Yes
     - —
     - Path to the ``.py`` DataSource module.
   * - ``custom_datasource_class``
     - string
     - Yes
     - —
     - Name of the ``DataSource`` class to register and use.
   * - ``options``
     - mapping
     - No
     - ``{}``
     - Passed to the reader as ``.option(...)`` calls.

``readMode`` defaults to ``stream``; ``batch`` is also supported. The class's
``name()`` classmethod return value is used as the ``.format(...)`` name.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/custom-datasource>` or
:doc:`choose another load source </reference/actions/load>`.
