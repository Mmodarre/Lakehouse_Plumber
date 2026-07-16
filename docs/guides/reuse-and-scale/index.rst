===============
Reuse and scale
===============

.. meta::
   :description: Remove duplication from Lakehouse Plumber pipelines, then multiply them — substitutions and secrets for environments, presets and templates for patterns, blueprints for whole fleets.

Once a pattern repeats, stop copying it. Lakehouse Plumber has a **ladder of
reuse tools**, each removing a different grain of duplication — reach for the
smallest one that fits. The :doc:`Choosing a reuse tool </concepts/presets-templates-blueprints>`
concept page explains the ladder; these guides are the mechanics.

- :doc:`substitutions-and-secrets` — one flowgroup, every environment; keep
  credentials out of your YAML.
- :doc:`templates` — one parametrized pattern, many tables.
- :doc:`blueprints` — one blueprint, whole pipelines across tenants and regions.
- :doc:`multi-flowgroup` — compose one pipeline from many flowgroups.

.. toctree::
   :maxdepth: 1
   :hidden:

   Substitutions & secrets <substitutions-and-secrets>
   Templates <templates>
   Blueprints <blueprints>
   Multi-flowgroup pipelines <multi-flowgroup>
