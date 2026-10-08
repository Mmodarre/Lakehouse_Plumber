Compose flowgroups
==================

Split a growing pipeline into flowgroups with clear inputs and outputs.
Flowgroups with the same ``pipeline`` value contribute to the same generated
pipeline. Dependencies come from what actions read and write, with
``depends_on`` available for dependencies LHP cannot infer.

.. toctree::
   :maxdepth: 1

   Build a pipeline from several flowgroups </guides/reuse-and-scale/multi-flowgroup>
   Flowgroups and dependencies </concepts/flowgroups-and-dependencies>

Look up :doc:`the file syntax </reference/config/flowgroups>` or
:doc:`explicit dependency rules </reference/dependency-analysis>`.
To turn cross-pipeline dependencies into scheduled jobs, see :doc:`/develop/jobs`.
