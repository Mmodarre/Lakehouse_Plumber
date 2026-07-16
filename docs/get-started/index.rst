===========
Get Started
===========

.. meta::
   :description: Learn Lakehouse Plumber by running its sample project — scaffold a complete TPC-H medallion pipeline, configure it, generate the Lakeflow code, and deploy it to Databricks.

Learn Lakehouse Plumber the way you'll use it: with a real project in front of
you. This course scaffolds the **sample project** LHP ships — a complete
medallion pipeline (bronze → silver → gold) over the ``samples.tpch`` dataset in
every Unity Catalog workspace — then walks you through reading it, configuring
it, generating its code, and deploying it to Databricks.

.. toctree::
   :maxdepth: 1

   01-install-and-scaffold
   02-configure-the-project
   03-explore-the-pipelines
   04-generate-and-inspect
   05-deploy-and-run
   06-explore-lhp-web
   07-where-to-next

The full course
===============

You'll work through:

1. **Install and scaffold** — get the ``lhp`` command and scaffold the sample
   project with ``lhp init --sample``.
2. **Configure the project** — tour the four config files (``lhp.yaml``,
   substitutions, the pipeline config, ``databricks.yml``) and set the two values
   the sample needs.
3. **Explore the pipelines** — read the flowgroups: cloudfiles and delta
   ingestion, a four-transform silver chain, CDC and snapshot-CDC dimensions, a
   gold materialized view.
4. **Generate and inspect** — run ``lhp generate`` and read the Lakeflow Python
   it produces.
5. **Deploy and run** — ship the bundle to a workspace, run the job, and re-run
   it to watch incremental ingestion and CDC advance.
6. **Explore LHP Web** — open the same project in the local web IDE: the pipeline
   as a graph, actions as forms, generation live in the browser.
7. **Where to next** — start your own project, and find the features the sample
   didn't cover.

Every YAML file, command output, and generated Python snippet on these pages is
**fixture-backed**: it comes from the real sample project and is regenerated on
every docs build, so nothing here can drift from what Lakehouse Plumber actually
emits.
