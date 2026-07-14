==========================================
One flowgroup, every environment
==========================================

.. meta::
   :description: The mental model behind Lakehouse Plumber substitutions — write a flowgroup once, keep it identical across dev, staging, and prod, and resolve everything environment-specific from a per-environment file through four widening tiers of tokens.

A Lakehouse Plumber flowgroup is written once and stays identical across dev,
staging, and prod. The logic of a pipeline — read this source, reshape these
rows, write that table — does not change when you promote it. What changes is
the surroundings: which catalog it writes to, which host it reads from, which
credential it uses. Keeping those two things apart is the whole point of
substitutions. Everything environment-specific is a **token**, and every token
resolves from a per-environment file when you generate. The flowgroup you commit
carries the shape of the pipeline; the substitution file carries the
environment.

This is the same idea that runs through the rest of Lakehouse Plumber —
**declare your ETL, don't hand-write it** — applied to promotion: declare the
values that vary, don't hand-copy the pipeline for each place it runs.

The alternative is config drift
===============================

The obvious way to run one pipeline in two places is to keep two copies —
``orders_dev.yaml`` and ``orders_prod.yaml`` — and hand-edit the catalog, the
host, and the password in each. It works on the first day. Then every change has
to be made twice, and the two files drift apart: a column added in dev, a flag
flipped in prod, a fix applied to one copy and forgotten in the other. The
pipeline that "worked in dev" fails in prod, and the failure hides in a diff
between two files that were supposed to be identical.

Environment drift is the most common shape of production bug, and a copy per
environment manufactures it. When the flowgroup is one file, that whole class of
bug closes: a difference between dev and prod can only come from the
substitution file, which is a small, reviewable YAML document. Generate for two
environments, diff the output, and every difference is one you declared and can
point at.

Four tiers, widening outward
============================

"Environment-specific" is not one thing, so Lakehouse Plumber does not resolve
it with one mechanism. It resolves four kinds of placeholder, in a fixed order,
each with a wider scope than the last:

1. ``%{local_var}`` — **local variables**, scoped to one flowgroup. Resolved
   first, at parse time, before anything else runs. For a value that repeats
   inside a single flowgroup and is the same in every environment.
2. ``{{ template_param }}`` — **template parameters**, scoped to one instance of
   a template. Expanded when a flowgroup stamps out a template. For the pieces
   that vary between uses of one shared pattern.
3. ``${env_token}`` — **environment tokens**, scoped to one environment.
   Substituted from ``substitutions/<env>.yaml`` at generation time. For the
   values that actually change between dev, staging, and prod.
4. ``${secret:scope/key}`` — **secret references**, scoped to the secret store.
   Turned into a ``dbutils.secrets.get(...)`` call in the generated Python, so
   the value is fetched when the pipeline runs — never written into a file.

Read that list top to bottom and the scope widens at each step: one flowgroup,
one template instance, one environment, the workspace's secret store. The order
is not cosmetic. Each layer resolves before the next one sees the YAML, so the
output of one can feed the next: a local variable can sit inside a template
parameter, a template parameter can expand into text that contains an
environment token, and an environment token can resolve to a string that holds a
secret reference. Resolving them in any other order would break that chaining.

The order also tracks when each value becomes known. A local variable is fixed
the moment the flowgroup is parsed. A template parameter is fixed when the
template is stamped. An environment token is fixed when you choose ``--env`` at
generate time. A secret is not fixed at generate time at all — the generated
code carries a lookup, and the value arrives at run time. Parse-time facts
resolve first, run-time secrets last.

Choosing the tier is choosing the scope
=======================================

Because the tiers differ by scope, picking the right one is a question about
where a value belongs, not about syntax:

- The value is the same everywhere but repeats inside one flowgroup — a table
  name reused across three actions. That is a **local variable**. It resolves at
  parse time, so it must not depend on the environment; a dev value frozen into
  ``variables:`` will not promote to prod.
- The value differs between dev and prod — a catalog, a schema, a storage path,
  an alert address. That is an **environment token**, and it lives in the
  substitution file.
- The value is a credential. That is a **secret reference**. A literal password
  in a substitution file is a leak the moment the file is committed, because
  substitution files ship to version control with the rest of the project. The
  reference keeps the value in the Databricks secret scope and puts only the
  indirection under version control.
- The value is one knob in a pattern reused across many flowgroups. That is a
  **template parameter**, filled in each time the template is used.

The mistake the model guards against is putting an environment-varying value in
the wrong tier — most often a local variable, which is resolved before Lakehouse
Plumber even knows which environment is in play. The rule that falls out of the
scope ladder: if a value differs between environments, it is an environment token
or a secret; if it is constant but repeated, it is a local variable or a template
parameter.

.. note::

   Always write environment tokens as ``${token}``. The bare single-brace form
   ``{token}`` is deprecated: it collides with Python f-string syntax inside the
   ``.py`` and ``.sql`` files a flowgroup can reference, where ``{name}`` is an
   ordinary runtime variable. The ``${...}`` form is unambiguous because ``${}``
   is not valid f-string syntax.

The model to keep
=================

Hold on to the four-tier ladder, narrowest scope to widest:

1. ``%{local_var}`` — one flowgroup.
2. ``{{ template_param }}`` — one template instance.
3. ``${env_token}`` — one environment.
4. ``${secret:scope/key}`` — the secret store, resolved at run time.

A flowgroup written against that ladder promotes across every environment
untouched, and the only thing that changes from dev to prod is a small file you
can read in one sitting. For the mechanics — writing a substitution file,
generating for an environment, and reading the diff across environments — see
the Substitutions and Secrets guide. For the exhaustive precedence rules,
recursive token expansion, and the default-scope secret form, see the
substitutions reference.
