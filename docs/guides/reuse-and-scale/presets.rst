Apply shared defaults with a preset
===================================

Use a preset when several actions share reader options or table properties.
Start with a working flowgroup from :doc:`/build/first-pipeline`.

Create the shared settings
--------------------------

.. literalinclude:: ../../_fixtures/reuse_presets/presets/bronze_defaults.yaml
   :language: yaml
   :caption: presets/bronze_defaults.yaml

``load_actions.cloudfiles`` targets the load action's ``source`` mapping;
``write_actions.streaming_table`` targets its ``write_target`` mapping.
The preset name inside the file is the name used by a flowgroup.

Apply it to a flowgroup
-----------------------

.. literalinclude:: ../../_fixtures/reuse_presets/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml

Validate and generate, then inspect the reader options and table properties in
``generated/dev/bronze_ingest/orders_ingest.py``:

.. code-block:: bash

   lhp validate --env dev
   lhp generate --env dev

In a bundle project, add ``-pc config/pipeline_config.yaml`` to both commands.

In the current resolver, preset values win conflicts in load-source and
write-target mappings. A flowgroup's list of preset names is applied in order;
later presets win. Read :doc:`/reference/config/presets` before combining layers,
especially for transform or flowgroup defaults, whose behaviour differs.

When the whole action sequence repeats, continue to :doc:`templates`.
