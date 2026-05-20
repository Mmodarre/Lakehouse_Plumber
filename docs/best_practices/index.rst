Best Practices
==============

.. meta::
   :description: Reasoning behind recommended Lakehouse Plumber (LHP) practices for project layout, environments, performance, governance, and testing.

The advice on these pages explains why certain Lakehouse Plumber (LHP) patterns
hold up at scale and others do not. The recommendations apply whether the
project has three :term:`FlowGroups <FlowGroup>` or three thousand — scale exposes more pain, but
the underlying reasoning does not change.

If you want to ship a pipeline first and come back to the reasoning, follow
:doc:`../quickstart` and return here when something starts to feel awkward.

How the pages divide
--------------------

Each subpage answers a different "why" question. Pick the one that matches the
decision in front of you.

:doc:`project_structure`
    Where files go, what gets named what, and why mono-repos with hundreds of
    FlowGroups still stay readable. Covers directory layout, subdirectory
    support per file type, prefix-based naming for the flat directories
    (templates, presets, substitutions), and the FlowGroup-Pipeline-Action
    organisational model.

:doc:`environments`
    Why LHP separates configuration that varies per environment from
    configuration that does not. Covers the substitution syntax tiers
    (``%{local}`` -> ``{{ template }}`` -> ``${env}`` -> ``${secret:...}``),
    the ``global`` section, environment promotion as a pure-YAML operation,
    and the reasoning behind never putting secret literals in
    ``substitutions/``.

:doc:`performance`
    When streaming tables are the right shape and when materialized views
    are, and how dependency resolution turns FlowGroup ordering into a
    non-decision. Covers the cost models behind common write-target
    choices and the levers LHP exposes for tuning generation and runtime.

:doc:`governance`
    How LHP's outputs map onto Unity Catalog and Databricks Asset Bundles —
    catalog naming, table comments, operational metadata columns, and the
    bundle resource files that turn FlowGroups into deployable units.
    Covers the audit-trail patterns that pay off most when something breaks
    in production.

:doc:`testing`
    Why declarative test actions catch a different class of bug than
    expectations, the CI layering that gives the fastest feedback, dry-run
    baselines as the LHP equivalent of snapshot testing, and the E2E
    pattern the LHP repo itself uses to lock down generator behaviour
    across releases.

How to read these pages
-----------------------

The pages are explanations, not procedures. They cover the reasoning behind
choices LHP nudges you toward, and they cite trade-offs you have to make
yourself. When a page describes a working pattern, it links to the
corresponding how-to for the step-by-step version.

A reader who already knows the pattern can skim. A reader debating two
approaches gets the trade-off written out. A reader new to LHP should start
with :doc:`../quickstart` and :doc:`../architecture`; these pages assume you
already know what a FlowGroup is.

Anti-patterns are listed inline alongside the pattern they violate, not
collected on a separate page. The fix is always next to the failure mode.

.. toctree::
   :maxdepth: 1
   :titlesonly:
   :hidden:

   project_structure
   environments
   performance
   governance
   testing
