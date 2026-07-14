Substitutions
=============

.. meta::
   :description: Reference for LHP substitutions — the substitutions/<env>.yaml file structure, the global and per-environment token maps, the secrets block, and the four token syntaxes.

``substitutions/<env>.yaml`` supplies the values LHP substitutes into ``${...}``
tokens when generating a pipeline for environment ``<env>`` (selected by
``lhp generate --env <env>``). Recognized top-level keys::

   global:              # tokens shared across every environment
     <token>: <value>
   <env>:               # dev, staging, prod, ... — one block per environment
     <token>: <value>
   secrets:             # secret-scope configuration
     default_scope: <scope>
     scopes:
       <alias>: <databricks_scope>

Top-level keys
--------------

.. list-table::
   :header-rows: 1
   :widths: 22 16 62

   * - Key
     - Type
     - Description
   * - ``<env>``
     - mapping
     - Token map for one environment. The key matches the ``--env`` value (e.g. ``dev``, ``staging``, ``prod``). Only the block matching the active environment is loaded.
   * - ``global``
     - mapping
     - Token map applied to every environment. A token also present in the active ``<env>`` block takes its environment-block value.
   * - ``secrets``
     - mapping
     - Secret-scope configuration (``default_scope`` and ``scopes``). See below.

Token maps
----------

Each ``<env>`` and ``global`` block maps token names to values. A name used in
a ``${name}`` reference resolves to the value here. Values may be strings,
numbers, or booleans (coerced to strings); nested mappings and lists are
preserved for structured tokens.

Two reserved tokens are provided automatically and need not be declared:

.. list-table::
   :header-rows: 1
   :widths: 24 34 42

   * - Token
     - Default
     - Override
   * - ``workspace_env``
     - The active ``--env`` value.
     - ``WORKSPACE_ENV`` OS environment variable.
   * - ``logical_env``
     - The active ``--env`` value.
     - ``LOGICAL_ENV`` OS environment variable.

.. code-block:: yaml

   global:
     landing_volume: /Volumes/shared/landing

   dev:
     catalog: dev_catalog
     bronze_schema: bronze
     silver_schema: silver

   prod:
     catalog: prod_catalog
     bronze_schema: bronze
     silver_schema: silver

secrets
-------

The top-level ``secrets`` block configures how ``${secret:...}`` references
resolve. Both fields are optional.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``default_scope``
     - string
     - No
     - —
     - Scope used for the ``${secret:key}`` form (no scope segment). Required only if that form is used.
   * - ``scopes``
     - mapping
     - No
     - —
     - Alias-to-scope map. An alias used in ``${secret:alias/key}`` resolves to the mapped Databricks secret-scope name; an unmapped scope is passed through unchanged.

.. code-block:: yaml

   secrets:
     default_scope: dev_secrets
     scopes:
       database_secrets: dev_db_secrets
       storage_secrets: dev_azure_secrets

Token syntax
------------

.. list-table::
   :header-rows: 1
   :widths: 30 26 44

   * - Syntax
     - Name
     - Defined in
   * - ``%{local_var}``
     - Local variable
     - ``variables:`` block of the flowgroup YAML (flowgroup-scoped).
   * - ``{{ template_param }}``
     - Template parameter
     - ``template_parameters:`` of the flowgroup; expanded by the Jinja2 template.
   * - ``${env_token}``
     - Environment token
     - ``global`` / ``<env>`` block of ``substitutions/<env>.yaml``.
   * - ``${secret:scope/key}``
     - Secret reference
     - Resolved to a ``dbutils.secrets.get()`` call. Use ``${secret:key}`` to fall back to ``default_scope``.

The bare ``{token}`` form (no ``$``) is deprecated; use ``${token}``. Resolution
order across these forms is covered in the substitutions concept page.

.. code-block:: yaml

   variables:
     entity: customer

   actions:
     - name: "load_%{entity}"
       type: load
       source:
         type: delta
         catalog: "${catalog}"
         schema: "${bronze_schema}"
         table: "%{entity}"
       target: "v_%{entity}"

     - name: load_jdbc
       type: load
       source:
         type: jdbc
         url: "jdbc:postgresql://host:5432/db"
         user: "${secret:database_secrets/user}"
         password: "${secret:database_secrets/password}"
         driver: org.postgresql.Driver
         table: public.customers
       target: v_customers_raw
