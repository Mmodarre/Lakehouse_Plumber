Python transform (python)
=========================

Calls a function in a project Python module and emits a ``@dp.temporary_view``.
The whole module file is copied to ``generated/<pipeline>/custom_python_functions/``
and imported as ``from custom_python_functions.<module> import <function>``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: transform_enrich
     type: transform
     transform_type: python
     source: v_orders
     target: v_orders_enriched
     module_path: "transforms/enrich.py"
     function_name: enrich
     parameters:
       lookup: regions

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
     - string / list
     - Yes
     - —
     - Input view name(s); ``None`` is rejected. A list passes a ``List[DataFrame]`` to the function.
   * - ``module_path``
     - string
     - Yes
     - —
     - Path to a ``.py`` file, relative to project root.
   * - ``function_name``
     - string
     - Yes
     - —
     - Name of the function to call.
   * - ``parameters``
     - dict
     - No
     - ``{}``
     - Passed to the function as its ``parameters`` argument.
   * - ``readMode``
     - string
     - No
     - ``batch``
     - ``batch`` or ``stream``; controls ``spark.read`` vs ``spark.readStream`` on each source.

Function signature: single source ``def fn(df, spark, parameters)``; list source
``def fn(dataframes, spark, parameters)``. A source is required. To create a
DataFrame without an input view, use :doc:`a Python load </reference/actions/load/python>`.

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/python`
- :doc:`All transform actions </reference/actions/transform>`
