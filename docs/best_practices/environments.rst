Environments
============

.. meta::
   :description: Reasoning behind LHP environment management — substitutions, the four-tier resolution order, secret references, and per-environment overrides.

For the step-by-step procedure for writing substitution files, see
:doc:`../substitutions`. This page explains why LHP separates environment
concerns the way it does and what that buys you.

Why environment-agnostic configs matter
---------------------------------------

LHP's central environment-promotion claim is that the same YAML configs
deploy to dev, staging, and prod. Only the substitution file changes.
The generated Python is environment-specific because the substituted
values differ, but the YAML source you commit is identical.

The reason this matters is that environment drift is the most common
shape of production bug. A pipeline works in dev because the dev
catalog has a slightly different schema, or the dev table happens to
have a column that prod does not, or a flag was flipped manually in
prod six months ago. When the YAML is identical across environments,
those drifts can only come from a substitution token — and the
substitution file is a small, reviewable YAML document.

The other shape of environment bug is the reverse: a change works in
prod and fails in dev. With environment-agnostic configs, you can run
``lhp generate --env dev`` and ``lhp generate --env prod`` side by side
and diff the outputs. Any difference is intentional.

The four-tier substitution order
--------------------------------

LHP resolves :term:`substitution <Substitution>` syntaxes in a specific order: :term:`local
variables <Local variable>`, then template parameters, then :term:`environment tokens <Environment token>`, then
secret references. The order is not arbitrary — it reflects the
lifecycle of each kind of substitution.

.. list-table::
   :header-rows: 1
   :widths: 20 25 55

   * - Syntax
     - Scope
     - Resolved when
   * - ``%{local_var}``
     - One FlowGroup
     - YAML parse time, before template expansion
   * - ``{{ template_param }}``
     - One template instance
     - Template expansion time
   * - ``${env_token}``
     - One environment
     - After template expansion, before code generation
   * - ``${secret:scope/key}``
     - Runtime
     - Generated as ``dbutils.secrets.get(...)`` call

The earliest layer (local variables) has the narrowest scope — one
FlowGroup. The latest layer (secrets) has the widest — the value is
only retrieved when the pipeline runs in Databricks. Each layer
resolves before the next sees the YAML, so a local variable can be
used as a template parameter, which can produce text containing an
environment token, which can include a secret reference.

The case difference between ``${SCREAMING_SNAKE_CASE}`` for env
tokens and ``%{lower_snake_case}`` for local variables is intentional:
a reader can tell at a glance which resolution layer applies, without
remembering syntax details. This pays off in PR review, where you want
to spot a mistake by reading.

Local variables are for FlowGroup-scoped repetition
---------------------------------------------------

When the same value — usually a table name, a schema, or a path
segment — appears multiple times within one FlowGroup, define it as a
local variable instead of repeating it:

.. code-block:: yaml
   :caption: Local variables compress repetition

   variables:
     entity: orders
     source_schema: raw
   actions:
     - name: load_%{entity}
       source:
         type: delta
         catalog: "${BRONZE_CATALOG}"
         database: "%{source_schema}"
         table: "%{entity}"

The local variable does not change between environments — ``orders``
is ``orders`` everywhere. The environment token (``${BRONZE_CATALOG}``)
captures what does change. Mixing the two is the common mistake: people
put environment-varying values in local variables, then discover the
FlowGroup cannot be promoted across environments because the
``variables:`` block hard-codes a dev value.

The rule is: if the value differs between dev and prod, it is an
environment token. If it is the same everywhere but repeats inside one
FlowGroup, it is a local variable.

The ``global`` section eliminates duplication
---------------------------------------------

Substitution files support a ``global`` section whose values apply to
every environment. Environment-specific sections override globals:

.. code-block:: yaml
   :caption: Shape of a substitution file

   global:
     catalog_prefix: main
     storage_account: companylake

   dev:
     catalog: "${catalog_prefix}_dev"
     landing_path: "abfss://landing@${storage_account}.dfs.core.windows.net/dev"

   prod:
     catalog: "${catalog_prefix}_prod"
     landing_path: "abfss://landing@${storage_account}.dfs.core.windows.net/prod"

LHP supports recursive token expansion — a token can reference another
token, up to ten iterations. The combination of recursive expansion and
the ``global`` section means most substitution files end up small and
diff-friendly. A change to the storage account name affects one line.

A standard medallion token set keeps the substitution files predictable
across projects:

.. code-block:: yaml

   global:
     bronze_catalog: "${catalog_prefix}_bronze"
     silver_catalog: "${catalog_prefix}_silver"
     gold_catalog: "${catalog_prefix}_gold"
     landing_path_base: "abfss://landing@${storage_account}.dfs.core.windows.net"

The same token names across projects means the same FlowGroup template
works in different repos.

Why secret literals stay out of substitution files
--------------------------------------------------

Substitution files are committed to version control. A secret literal
in one of them is a leak — even if the file is committed to a private
repo, every developer who clones the repo can read the secret, and
nothing prevents the value from showing up in a backup, a Slack paste,
or a deleted-but-recoverable branch.

LHP's ``${secret:scope/key}`` syntax solves this. The substitution file
contains a reference, not a value. LHP converts the reference into a
``dbutils.secrets.get(scope="scope", key="key")`` call in the generated
Python. The actual secret is stored in a Databricks secret scope and
retrieved at pipeline runtime. The version-controlled artifact contains
only the indirection.

.. warning::

   Any secret literal in a substitution file is leaked the moment the
   file is committed. There is no "private substitution file" — they all
   ship to version control as part of the project. Always use
   ``${secret:scope/key}``.

The substitution file can declare a default scope and named scope
aliases so references stay readable:

.. code-block:: yaml

   secrets:
     default_scope: prod-secrets
     scopes:
       data-vault: data-vault-prod

   jdbc_password: "${secret:default/db_password}"
   vault_token: "${secret:data-vault/api_token}"

Per-environment overrides for behaviour, not just values
--------------------------------------------------------

Substitution tokens cover most environment differences — catalog names,
schemas, storage paths, alert email addresses. The cases that need
different *behaviour* per environment (different DQE expectations in
dev versus prod, for example) usually have to be modelled through
presets or template parameters that take a token as input.

The reason is that LHP keeps substitution resolution textual: it
replaces tokens with values. It does not flip flags or skip actions.
If a pipeline needs to drop bad rows in prod and only warn in dev, the
``failureAction`` in the expectations file must come from a token, and
the substitution file picks the value per environment.

.. code-block:: yaml

   # expectations file
   - name: valid_order_id_not_null
     constraint: "order_id IS NOT NULL"
     failureAction: "${BRONZE_DQE_ACTION}"   # warn in dev, drop in prod

This is the canonical pattern: parameterise behaviour the same way you
parameterise values. The substitution file then captures the whole
environment difference in one place.

Auditing the available tokens
-----------------------------

Before writing FlowGroups in a fresh project, run
``lhp substitutions --env dev`` to dump the resolved token set. The
command prints every token visible to ``--env dev``, including
inherited globals and recursively expanded values. The most common
class of bug — an unresolved-token error at generation time — comes
from a typo or a missing token. ``lhp substitutions`` surfaces both
before you write the FlowGroup that uses them.

Anti-patterns
-------------

**Secrets in substitution files.** Leaks to version control. Use
``${secret:scope/key}`` syntax.

**Hardcoded catalog or schema names in YAML.** Breaks environment
promotion. The whole point of the substitution layer is to push these
out of FlowGroup YAML. Use ``${BRONZE_CATALOG}.${schema}.${table}``.

**Local variables for environment-varying values.** ``%{var}`` is
flowgroup-scoped and resolved at parse time, before LHP sees which
environment is in play. Putting a dev value in a local variable
silently breaks prod generation.

**Different YAML configs per environment.** If you find yourself
maintaining ``orders_dev.yaml`` and ``orders_prod.yaml`` with mostly
the same content, the difference belongs in the substitution file, not
in duplicate sources.

See also
--------

- :doc:`../substitutions` for the procedural how-to and full syntax
  reference.
- :doc:`../configure_bundles` for how environment substitutions
  integrate with Databricks Asset Bundle targets.
- :doc:`governance` for operational-metadata patterns that include
  ``pipeline_id`` and ``pipeline_run_id`` for cross-environment audit.
