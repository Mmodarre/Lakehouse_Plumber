Substitutions & Secrets
=======================

.. meta::
   :description: Environment substitutions, local variables, secret management, and file substitution support in Lakehouse Plumber.

Summary
-------

LakehousePlumber uses multiple substitution syntaxes, each resolved at a different
stage of the generation pipeline. The table below shows all forms, their scope,
and the processing order.

.. list-table:: Substitution Types
   :header-rows: 1
   :widths: 20 25 25 30

   * - Syntax
     - Name
     - Scope
     - Defined In
   * - ``%{var}``
     - Local variable
     - Flowgroup
     - ``variables:`` section in flowgroup YAML
   * - ``{{ param }}``
     - Template parameter
     - Template
     - ``template_parameters:`` in flowgroup; consumed by Jinja2 template
   * - ``${token}``
     - Environment token
     - Global / per-environment
     - ``substitutions/<env>.yaml``
   * - ``${secret:scope/key}``
     - Secret reference
     - Global / per-environment
     - ``substitutions/<env>.yaml`` (scope aliases); resolved to ``dbutils.secrets.get()``

**Processing order:**

1. ``%{var}`` — Local variables are resolved first, within the flowgroup
2. ``{{ param }}`` — Template parameters are expanded via Jinja2
3. ``${token}`` — Environment tokens are substituted from the env file
4. ``${secret:scope/key}`` — Secret references are converted to secure ``dbutils.secrets.get()`` calls

Each phase only processes its own syntax and passes all other forms through untouched,
so tokens from later phases can safely appear in earlier contexts (e.g., ``${catalog}``
inside a ``%{var}`` value).

Environment Configuration
-------------------------

Tokens wrapped in ``${token}`` are replaced at generation time using files under
``substitutions/<env>.yaml``. This enables environment-specific configurations
while keeping pipeline definitions portable.

**Example substitution file:**

.. code-block:: yaml
   :caption: substitutions/dev.yaml
   :linenos:
   :emphasize-lines: 10-15

   # Environment-specific tokens
   dev:
     catalog: dev_catalog
     bronze_schema: bronze
     silver_schema: silver
     landing_path: /mnt/dev/landing
     checkpoint_path: /mnt/dev/checkpoints

   # Secret configuration
   secrets:
     default_scope: dev_secrets
     scopes:
       database_secrets: dev_db_secrets
       storage_secrets: dev_azure_secrets
       api_secrets: dev_external_apis


Local Variables
---------------

**Local variables** allow you to define reusable values within a single flowgroup, reducing repetition and improving maintainability. They are resolved **before** templates and environment substitutions.

**Syntax:** ``%{variable_name}``

**Key Features:**

- **Flowgroup-scoped**: Variables are only accessible within the flowgroup where they're defined
- **Inline substitution**: Supports patterns like ``prefix_%{var}_suffix``
- **Strict validation**: Undefined variables cause immediate errors with clear messages
- **Processed first**: Resolved before templates, presets, and environment substitutions

**Example:**

.. code-block:: yaml
   :caption: pipelines/customer_bronze.yaml
   :linenos:
   :emphasize-lines: 4-7,12,17,20

   pipeline: acmi_edw_bronze
   flowgroup: customer_pipeline

   variables:
     entity: customer
     source_table: customer_raw
     target_table: customer

   actions:
     - name: "load_%{entity}_raw"
       type: load
       source:
         type: delta
         database: "${catalog}.${raw_schema}"
         table: "%{source_table}"
       target: "v_%{entity}_raw"

     - name: "write_%{entity}_bronze"
       type: write
       source: "v_%{entity}_cleaned"
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: "%{target_table}"

.. seealso::
   For complete details on local variables, see :doc:`templates_reference`.

Secret Management
-----------------

**Secret references** use the ``${secret:scope/key}`` syntax and are converted to
secure ``dbutils.secrets.get()`` calls in generated Python code. LHP validates
scope aliases and collects every secret used by the pipeline, making security
reviews and approvals easier.

**Secret reference formats:**

- ``${secret:scope_alias/key}`` - Uses specific scope alias (resolved to actual Databricks scope)
- ``${secret:key}`` - Uses default_scope if configured

.. note::
   Scope aliases (like ``database_secrets``) are mapped to actual Databricks secret scope
   names (like ``dev_db_secrets``) in the substitution file. This provides flexibility
   to use different scope names across environments while keeping pipeline definitions portable.


File Substitution Support
-------------------------

.. versionadded:: Latest

LakehousePlumber now supports substitutions in external files, providing the same environment-specific flexibility for Python functions and SQL files that you have in YAML configurations.

**Supported File Types:**

================== ==================================================
File Type          Where Used
================== ==================================================
**Python Files**   • Snapshot CDC ``source_function`` files
                   • Python transform ``module_path`` files
                   • Custom datasource ``module_path`` files
**SQL Files**      • SQL load actions with ``sql_path``
                   • SQL transform actions with ``sql_path``
================== ==================================================

**Example Python Function with Substitutions:**

.. code-block:: python
   :caption: py_functions/customer_snapshot.py
   :linenos:
   :emphasize-lines: 4-5,10

   from typing import Optional, Tuple
   from pyspark.sql import DataFrame

   catalog = "${catalog}"
   schema = "${bronze_schema}"

   def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
       if latest_version is None:
           df = spark.sql(f"""
               SELECT * FROM {catalog}.{schema}.customers
               WHERE snapshot_id = 1
           """)
           return (df, 1)
       return None

**Example SQL File with Substitutions:**

.. code-block:: text
   :caption: sql/customer_metrics.sql
   :linenos:
   :emphasize-lines: 4-6

   SELECT
       customer_id,
       customer_name,
       '${environment}' as source_env
   FROM ${catalog}.${bronze_schema}.customers
   WHERE created_date >= '${cutoff_date}'

**Secret Support in Files:**

Both Python and SQL files support secret substitutions with the same syntax as YAML:

.. code-block:: python
   :caption: Example with secrets

   # Environment token
   api_endpoint = "${api_base_url}"

   # Secret reference
   api_key = "${secret:api_keys/service_key}"
   db_password = "${secret:database/password}"

**Processing Behavior:**

- **Tokens and secrets** are processed before the file content is used
- **Python files** have substitutions applied before import management
- **SQL files** have substitutions applied before query execution
- **Backward compatible** - files without substitution variables work unchanged
- **Same syntax** as YAML substitutions for consistency

**Example pipeline with secrets:**

.. code-block:: yaml
   :caption: pipelines/customer_ingestion/external_load.yaml
   :linenos:
   :emphasize-lines: 9-12

   pipeline: customer_ingestion
   flowgroup: external_load

   actions:
     - name: load_from_postgres
       type: load
       source:
         type: jdbc
         url: "jdbc:postgresql://${secret:database_secrets/host}:5432/customers"
         user: "${secret:database_secrets/username}"
         password: "${secret:database_secrets/password}"
         driver: "org.postgresql.Driver"
         table: "customers"
       target: v_customers_raw

**Generated Python code:**

.. code-block:: python
   :caption: Generated DLT code with secure secret handling
   :linenos:
   :emphasize-lines: 6-8

   @dp.temporary_view()
   def v_customers_raw():
       """Load from external database"""
       df = spark.read \
           .format("jdbc") \
           .option("url", f"jdbc:postgresql://{dbutils.secrets.get(scope='dev_db_secrets', key='host')}:5432/customers") \
           .option("user", f"{dbutils.secrets.get(scope='dev_db_secrets', key='username')}") \
           .option("password", f"{dbutils.secrets.get(scope='dev_db_secrets', key='password')}") \
           .option("driver", "org.postgresql.Driver") \
           .option("dbtable", "customers") \
           .load()

       return df


Substitution Syntax
-------------------

LakehousePlumber supports multiple substitution syntaxes for different purposes:

**Local Variables (Flowgroup-scoped):** ``%{variable}``

.. code-block:: yaml

   variables:
     entity: customer

   actions:
     - name: "load_%{entity}_raw"
       target: "v_%{entity}_raw"

**Environment Substitution:** ``${token}``

.. code-block:: yaml

   catalog: ${my_catalog}
   table: ${catalog}.${schema}.customers

**Secret References:** ``${secret:scope/key}``

.. code-block:: yaml

   password: ${secret:database/db_password}

**Template Parameters:** ``{{ parameter }}``

.. code-block:: yaml

   use_template: my_template
   template_parameters:
     table_name: customer
   # In template: table: "{{ table_name }}"

.. note::
   **Syntax Distinction:**

   - ``%{var}`` = Local variable (flowgroup-scoped)
   - ``${token}`` = Environment substitution
   - ``${secret:scope/key}`` = Secret reference
   - ``{{ parameter }}`` = Template parameter (Jinja2)

.. warning::
   **Legacy syntax:** The bare ``{token}`` form (without ``$``) is still supported for
   backward compatibility but is deprecated. In external Python files (transforms,
   batch handlers, custom datasources, snapshot CDC functions, custom sinks), the
   ``{token}`` pattern directly collides with Python f-string syntax — if a Python
   runtime variable like ``{catalog}`` in ``f"SELECT * FROM {catalog}.{schema}.table"``
   matches a substitution token name, it will be silently replaced at generation time,
   breaking your code. The ``${token}`` syntax avoids this entirely because ``${}`` is
   not valid Python f-string syntax. Use ``${token}`` in all new configurations.

.. note::
   **Processing Order:**

   1. **Local variables** (``%{var}``) are resolved first within the flowgroup
   2. **Template parameters** (``{{ }}``) are resolved when templates are applied
   3. **Environment substitutions** (``${ }``) are resolved at generation time
   4. **Secret references** (``${secret:}``) are converted to ``dbutils.secrets.get()`` calls

.. warning::
   **Python Code Context:** When using LHP substitution tokens inside external Python
   files (batch handlers, Python transforms, custom datasources, snapshot CDC functions,
   custom sinks), you **must** use ``${}`` syntax. LHP applies substitution to these
   files at generation time, and the legacy ``{token}`` pattern matches Python f-string
   variables.

   .. code-block:: python
      :caption: Correct — LHP tokens use ${}, Python variables use {}

      # ${catalog} is replaced by LHP at generation time
      default_catalog = "${catalog}"

      # {table} is a Python runtime variable — safe because it has no $ prefix
      spark.sql(f"SELECT * FROM {default_catalog}.{table}")

   .. code-block:: python
      :caption: Dangerous — {catalog} collides with LHP substitution

      def my_transform(df, spark, parameters):
          catalog = parameters.get("catalog", "main")
          # If 'catalog' is also a substitution token, LHP replaces {catalog}
          # at generation time, breaking this f-string!
          return spark.sql(f"SELECT * FROM {catalog}.{schema}.lookup")
