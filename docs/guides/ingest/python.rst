Read data with a Python function
================================

Use a Python load when a function should return the source DataFrame. The
function receives ``spark`` and a parameters dictionary. For a PySpark
``DataSource`` class instead, use :doc:`custom-datasource`.

Create the loader
-----------------

Create ``loaders/orders.py`` relative to the project root:

.. code-block:: python

   def get_df(spark, parameters):
       return spark.read.table(parameters["table"])

Declare the action
------------------

Add this action to a :doc:`flowgroup </reference/config/flowgroups>`:

.. code-block:: yaml

   - name: load_orders
     type: load
     source:
       type: python
       module_path: loaders/orders.py
       function_name: get_df
       parameters:
         table: "${catalog}.${bronze_schema}.orders"
     target: v_orders

Follow it with transforms or a write action that consumes ``v_orders``.
Run ``lhp validate --env dev`` and ``lhp generate --env dev``; in a bundle
project add ``-pc config/pipeline_config.yaml``. LHP copies the module into the
generated pipeline's ``custom_python_functions`` directory and imports the
function. The function reads data when the deployed pipeline runs.

The function controls batch or streaming reads; action ``readMode`` does not
change its implementation. For complete options and common action fields, see
:doc:`/reference/actions/load/python`.
