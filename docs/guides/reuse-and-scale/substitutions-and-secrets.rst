=======================================
Reuse one flowgroup across environments
=======================================

.. meta::
   :description: Ship one Lakehouse Plumber flowgroup to dev, staging, and prod unchanged — declare the values that change per environment as substitution tokens, keep credentials out of source with secret references, and let LHP resolve both at generation time.

A pipeline that reads from a dev database and writes to a dev catalog has to
read from a prod database and write to a prod catalog too. The logic is the
same; the catalog names, the connection host, and the credentials are not.

You could keep two copies — ``orders_dev.yaml`` and ``orders_prod.yaml`` — and
hand-edit the catalog, the host, and the password in each. Now every change has
to be made twice, and the two files drift: a column added in dev, a flag flipped
in prod, and the pipeline that "worked in dev" fails in prod. Or you write the
flowgroup **once**, mark the values that change with tokens, and let a small
per-environment file carry the difference. That is the idea on every page:
**declare your ETL, don't hand-write it** — and here, don't hand-copy it per
environment either.

Let's take one flowgroup that pulls the orders table out of an operational
database and land it in the bronze layer, then ship that same file to dev and
prod — with the catalog, the database host, and the credentials all supplied
from outside the flowgroup.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. To *run* the
generated pipeline you would also need the database credentials stored in a
Databricks secret scope, but you do not need them to generate: the flowgroup
references a secret by name, and generation resolves that reference into a
lookup call, never into a value.

Declare the flowgroup once
==========================

A pipeline in Lakehouse Plumber is a **flowgroup**: a short YAML file describing
a sequence of **actions**. This one has two — a ``load`` that reads the orders
table over JDBC, and a ``write`` that lands it as a bronze table. Nothing in it
names an environment.

Create ``pipelines/orders_ingest.yaml``:

.. literalinclude:: ../../_fixtures/guide_reuse_substitutions/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml

Three different kinds of placeholder appear here, and the difference between them
is the whole point:

- ``%{entity}`` and ``%{source_table}`` are **local variables**, declared in the
  ``variables:`` block. They are the same in every environment — orders are
  orders everywhere — and they exist so a repeated value is written once.
- ``${catalog}``, ``${bronze_schema}``, ``${db_host}``, and ``${db_name}`` are
  **environment tokens**. These are the values that change between dev and prod.
- ``${secret:sales_db/username}`` and ``${secret:sales_db/password}`` are
  **secret references**. The flowgroup names a logical scope and a key; it never
  holds the credential itself.

The four-tier resolution order
==============================

Lakehouse Plumber resolves those placeholders in a fixed order, and knowing the
order is what lets you reason about the output:

1. ``%{local_var}`` — **local variables**, resolved first, scoped to this one
   flowgroup. ``%{entity}`` becomes ``orders`` before anything else runs.
2. ``{{ template_param }}`` — **template parameters**, expanded when a flowgroup
   uses a template. This flowgroup uses none; templates are covered in the
   templates guide.
3. ``${env_token}`` — **environment tokens**, substituted from
   ``substitutions/<env>.yaml`` at generation time.
4. ``${secret:scope/key}`` — **secret references**, turned into a
   ``dbutils.secrets.get(...)`` call in the generated Python, so the value is
   fetched at run time.

Each layer resolves before the next one sees the YAML, and its scope widens as
you go down: a local variable belongs to one flowgroup, an environment token to
one environment, and a secret is only read when the pipeline runs. Because local
variables resolve first, put a value in ``variables:`` only when it is the same
everywhere — a value that differs per environment belongs in an environment
token, or it will be frozen at parse time and the flowgroup will not promote.

Supply the per-environment values
==================================

The environment tokens and the secret scope both come from a per-environment
substitutions file. Create ``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_reuse_substitutions/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

Values under ``dev:`` fill the environment tokens. The ``global:`` section holds
values shared by every environment — the database name is ``sales`` no matter
where you run — so you declare them once instead of repeating them in each file.
The ``secrets:`` block maps the *logical* scope name you wrote in the flowgroup —
``sales_db`` — onto the *actual* workspace scope ``dev_sales_db_secrets``, so
``${secret:sales_db/username}`` resolves to a lookup against
``dev_sales_db_secrets``.

.. important::

   Substitution files are committed to version control, so a real password in
   one is a leak the moment you commit. That is why credentials live behind
   ``${secret:scope/key}`` and the substitution file stores only a scope
   *alias*, never the secret. The value stays in the Databricks secret scope and
   is fetched at run time.

Now add the production environment. It has the same tokens as dev with different
values, and the same logical ``sales_db`` scope pointed at the production
secret scope. Create ``substitutions/prod.yaml``:

.. literalinclude:: ../../_fixtures/guide_reuse_substitutions/substitutions/prod.yaml
   :language: yaml
   :caption: substitutions/prod.yaml

The flowgroup does not change. The difference between dev and prod is entirely
contained in these two small files.

Generate for dev
=================

Compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  0 files
   ✓ validate (0.33s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.39s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and secret reference and checks the actions
before you commit to generating; ``generate --env dev`` writes the Python for
the dev environment. Open ``generated/dev/bronze_ingest/orders_ingest.py`` —
this is the entire output:

.. literalinclude:: ../../_fixtures/guide_reuse_substitutions/generated/dev/bronze_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_ingest.py
   :emphasize-lines: 23-32, 45

Every placeholder is gone. ``%{entity}`` resolved to ``orders`` (the view is
``v_orders_raw``, the function is ``orders``). The ``${db_host}`` and
``${db_name}`` tokens filled in the JDBC URL as
``jdbc:postgresql://sales-db-dev.internal:5432/sales``. Each
``${secret:sales_db/...}`` reference became a
``dbutils.secrets.get(scope="dev_sales_db_secrets", key=...)`` call — the
password is never written into the file. And the target name resolved to
``dev_catalog.bronze.orders``.

Ship the same flowgroup to prod
===============================

Generate again, this time for prod — the flowgroup file untouched:

.. code-block:: console

   $ lhp generate --env prod
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.40s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Diff the two generated files and you can see exactly what an environment change
costs — and what it does not touch:

.. code-block:: console

   $ diff generated/dev/bronze_ingest/orders_ingest.py \
          generated/prod/bronze_ingest/orders_ingest.py
   23c23
   <         .option("url", "jdbc:postgresql://sales-db-dev.internal:5432/sales")
   ---
   >         .option("url", "jdbc:postgresql://sales-db-prod.internal:5432/sales")
   25c25
   <             "user", dbutils.secrets.get(scope="dev_sales_db_secrets", key="username")
   ---
   >             "user", dbutils.secrets.get(scope="prod_sales_db_secrets", key="username")
   29c29
   <             dbutils.secrets.get(scope="dev_sales_db_secrets", key="password"),
   ---
   >             dbutils.secrets.get(scope="prod_sales_db_secrets", key="password"),
   45c45
   <     name="dev_catalog.bronze.orders",
   ---
   >     name="prod_catalog.bronze.orders",

Two 54-line files, and they differ in exactly five lines: the database host, the
two secret scopes, and the target catalog. Every one of those differences was
declared in the substitution file — none of them came from editing the
flowgroup. Any difference between the environments is one you can point at, which
is what makes a diff across environments worth reading.

What you just did
=================

You wrote one 40-line flowgroup and generated it for two environments. Each
compiled to a 54-line Lakeflow file, and the two differ in exactly the five lines
your substitution files control. **The flowgroup source is byte-identical across
every environment** — the catalog, the host, and the credentials all came from
outside it, and no PySpark came from you at all.

The payoff scales the way you want it to: a third environment is one more small
substitution file, not another copy of the pipeline. Add ``staging.yaml`` and
``lhp generate --env staging`` produces the staging build from the same source —
so a fleet of environments stays one flowgroup, and drift between them can only
come from a small, reviewable YAML file.

What's next
===========

- **Audit the resolved tokens before you generate.** ``lhp substitutions --env
  dev`` prints every token visible to an environment, including inherited
  ``global`` values, so you catch a typo or a missing token before it becomes a
  generation error.
- **Push tokens into your SQL and Python files.** The same ``${...}`` and
  ``${secret:...}`` syntax works inside ``.sql`` and ``.py`` files referenced by
  an action, not only in YAML — so an inline query or a Python transform can be
  environment-specific too.
- **See every rule.** Recursive token expansion, the default-scope form
  ``${secret:key}``, and the exact precedence edge cases are listed in the
  substitutions and project-configuration reference.
