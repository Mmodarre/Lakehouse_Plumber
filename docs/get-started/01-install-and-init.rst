=======================
Install and initialize
=======================

.. meta::
   :description: Install Lakehouse Plumber and scaffold your first project in two commands.

Two commands and you're ready to write pipelines.

Install
=======

Lakehouse Plumber is a Python package. Install it with pip:

.. code-block:: bash

   pip install lakehouse-plumber

That gives you the ``lhp`` command.

Create a project
================

``lhp init`` scaffolds a project into the **current directory** — so make a
folder for it and initialize from inside:

.. code-block:: bash

   mkdir first_pipeline
   cd first_pipeline
   lhp init first_pipeline --no-bundle

``--no-bundle`` keeps this first project minimal; you'll add Databricks bundle
packaging later, when you deploy.

What LHP created
================

The scaffold is a working project skeleton. Two folders matter first:

- ``pipelines/`` — your **flowgroups**: the YAML that describes each pipeline.
- ``substitutions/`` — per-environment values (``dev.yaml``, ``prod.yaml``, …)
  that fill the ``${...}`` tokens in your flowgroups.

``lhp.yaml`` at the root holds project-level settings, and everything LHP
generates lands in ``generated/`` — readable Python you never edit by hand.

Next
====

You have an empty project. Time to put a pipeline in it — see
:doc:`02-your-first-pipeline`.
