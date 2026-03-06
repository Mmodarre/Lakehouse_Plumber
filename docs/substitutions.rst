Substitutions & Secrets
=======================

.. meta::
   :description: Environment substitutions, local variables, secret management, and file substitution support in Lakehouse Plumber.

Environment Configuration
-------------------------

Tokens wrapped in ``{token}`` or ``${token}`` are replaced at generation time
using files under ``substitutions/<env>.yaml``. This enables environment-specific
configurations while keeping pipeline definitions portable.

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
         database: "{catalog}.{raw_schema}"  # Environment tokens still work!
         table: "%{source_table}"
       target: "v_%{entity}_raw"

     - name: "write_%{entity}_bronze"
       type: write
       source: "v_%{entity}_cleaned"
       write_target:
         type: streaming_table
         database: "{catalog}.{bronze_schema}"
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

   catalog = "{catalog}"
   schema = "{bronze_schema}"

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
       '{environment}' as source_env
   FROM {catalog}.{bronze_schema}.customers
   WHERE created_date >= '{cutoff_date}'

**Secret Support in Files:**

Both Python and SQL files support secret substitutions with the same syntax as YAML:

.. code-block:: python
   :caption: Example with secrets

   # Environment token
   api_endpoint = "{api_base_url}"

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

**Environment Substitution (Preferred):** ``${token}``

.. code-block:: yaml

   catalog: ${my_catalog}
   table: ${catalog}.${schema}.customers

**Environment Substitution (Legacy):** ``{token}``

.. code-block:: yaml

   catalog: {my_catalog}
   table: {catalog}.{schema}.customers

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
   - ``${token}`` = Environment substitution (preferred)
   - ``{token}`` = Environment substitution (legacy, backward compatible)
   - ``${secret:scope/key}`` = Secret reference
   - ``{{ parameter }}`` = Template parameter (Jinja2)

   The ``${}`` syntax is preferred for environment substitution because:

   - It's visually distinct from Python f-string syntax
   - It avoids confusion when tokens appear in SQL or Python strings
   - It clearly differentiates from local variables (``%{}``) and template parameters (``{{ }}``)

.. note::
   **Processing Order:**

   1. **Local variables** (``%{var}``) are resolved first within the flowgroup
   2. **Template parameters** (``{{ }}``) are resolved when templates are applied
   3. **Environment substitutions** (``{ }`` and ``${ }``) are resolved at generation time
   4. **Secret references** (``${secret:}``) are converted to ``dbutils.secrets.get()`` calls

.. warning::
   **Python Code Context:** When using substitution tokens inside Python code
   (e.g., in batch handlers or Python transform files), always use ``${}``
   syntax to avoid conflicts with Python f-strings and SQL placeholders.

   .. code-block:: python
      :caption: Correct usage in Python files

      # Use ${} for LHP substitution (replaced at generation time)
      table = "${catalog}.${schema}.customers"

      # Then use Python f-string for runtime formatting
      spark.sql(f"SELECT * FROM {table}")

   .. code-block:: python
      :caption: Incorrect usage (causes SQL syntax errors)

      # DON'T use {} in non-f-strings - generates invalid SQL
      spark.sql("""
          SELECT * FROM {catalog}.{schema}.customers
      """)
      # After substitution: SELECT * FROM {acme_catalog}.{bronze}.customers
      # This is INVALID SQL!
