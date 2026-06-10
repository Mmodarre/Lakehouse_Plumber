Editor setup
============

.. meta::
   :description: Configure VS Code, Cursor, and other LSP-aware editors for Lakehouse Plumber YAML authoring with schema validation, completions, and hover documentation.

Configure your editor so it validates Lakehouse Plumber (LHP) YAML against the
bundled JSON schemas and offers completions and hover docs on every key. This page
covers VS Code (auto-configured by ``lhp init``), Cursor, and any editor that
speaks the YAML Language Server protocol.

For background on why LHP ships JSON schemas, see :doc:`architecture`.

Use the auto-configured setup from ``lhp init``
-----------------------------------------------

``lhp init`` writes a ready-to-use VS Code configuration into every new project.
You do not need to do anything else if you scaffolded the project with the CLI.

Run the initializer from an empty directory:

.. code-block:: bash

   mkdir my_lakehouse && cd my_lakehouse
   lhp init my_lakehouse

The command creates the project layout and, alongside it, a ``.vscode/`` folder:

.. code-block:: text
   :caption: .vscode/ contents after ``lhp init``

   .vscode/
     ├── settings.json          # Maps YAML files to JSON schemas
     └── schemas/
         ├── flowgroup.schema.json
         ├── template.schema.json
         ├── preset.schema.json
         ├── project.schema.json
         ├── substitution.schema.json
         ├── blueprint.schema.json
         └── instance.schema.json

Open the project in VS Code, install the **YAML** extension by Red Hat if you do
not already have it, and editing any file under ``pipelines/``, ``presets/``,
``templates/``, or ``substitutions/`` triggers schema validation and IntelliSense.

.. note::

   The schemas in ``.vscode/schemas/`` are copies pinned to the LHP version that
   ran ``lhp init``. Run ``lhp init`` in a sibling directory and copy the updated
   files when you upgrade LHP, or follow the manual steps below to refresh them.

Configure VS Code in an existing project
----------------------------------------

If your project predates ``lhp init`` or you removed ``.vscode/``, recreate the
configuration in four steps.

1. Install the YAML extension
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install **YAML** by Red Hat (extension ID ``redhat.vscode-yaml``) from the
Extensions marketplace. The extension ships the YAML Language Server that VS Code
uses to validate against JSON Schema.

2. Copy the schemas into your project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LHP keeps the canonical schemas under ``src/lhp/schemas/`` in the source tree
and bundles them in the installed package. Copy them into ``.vscode/schemas/``:

.. code-block:: bash

   mkdir -p .vscode/schemas
   python -c "from importlib.resources import files; import shutil; \
     src = files('lhp.schemas'); dest = '.vscode/schemas'; \
     [shutil.copy(src / name, dest) for name in [ \
       'flowgroup.schema.json', 'template.schema.json', \
       'preset.schema.json', 'project.schema.json', \
       'substitution.schema.json', 'blueprint.schema.json', \
       'instance.schema.json']]"

This pulls the schemas straight from the installed ``lhp`` package, so they match
the CLI version you are using.

3. Create ``.vscode/settings.json``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Write the schema-to-glob mapping that ``lhp init`` would have generated:

.. code-block:: json
   :caption: .vscode/settings.json

   {
     "yaml.schemas": {
       "./schemas/flowgroup.schema.json": [
         "pipelines/**/*.yaml",
         "pipelines/**/*.yml"
       ],
       "./schemas/template.schema.json": [
         "templates/**/*.yaml",
         "templates/**/*.yml"
       ],
       "./schemas/preset.schema.json": [
         "presets/**/*.yaml",
         "presets/**/*.yml"
       ],
       "./schemas/project.schema.json": [
         "lhp.yaml",
         "lhp.yml"
       ],
       "./schemas/substitution.schema.json": [
         "substitutions/**/*.yaml",
         "substitutions/**/*.yml"
       ],
       "./schemas/blueprint.schema.json": [
         "blueprints/**/*.yaml",
         "blueprints/**/*.yml"
       ],
       "./schemas/instance.schema.json": [
         "instances/**/*.yaml",
         "instances/**/*.yml"
       ]
     },
     "yaml.format.enable": true,
     "yaml.validate": true,
     "yaml.completion": true,
     "yaml.hover": true,
     "files.associations": {
       "*.yaml": "yaml",
       "*.yml": "yaml"
     }
   }

The glob patterns mirror LHP's project layout. If you keep FlowGroups in a custom
directory, add it to the relevant ``yaml.schemas`` entry.

4. Reload the window
~~~~~~~~~~~~~~~~~~~~

Run **Developer: Reload Window** from the command palette (``Ctrl+Shift+P`` or
``Cmd+Shift+P``). VS Code rereads ``settings.json`` and binds the schemas.

Configure Cursor and other LSP editors
--------------------------------------

Cursor uses the same YAML Language Server as VS Code, so the ``.vscode/``
configuration above works without modification. Install the YAML extension from
the Open VSX registry and reload.

For other editors (Neovim with ``yaml-language-server``, JetBrains IDEs, Helix,
Zed), point the language server's ``yaml.schemas`` setting at the same files:

.. code-block:: yaml
   :caption: Generic yaml-language-server config

   yaml.schemas:
     ./.vscode/schemas/flowgroup.schema.json:
       - pipelines/**/*.yaml
     ./.vscode/schemas/template.schema.json:
       - templates/**/*.yaml
     ./.vscode/schemas/preset.schema.json:
       - presets/**/*.yaml
     ./.vscode/schemas/project.schema.json:
       - lhp.yaml
     ./.vscode/schemas/substitution.schema.json:
       - substitutions/**/*.yaml

JetBrains IDEs (PyCharm, IntelliJ) configure JSON Schema mappings under
**Settings -> Languages & Frameworks -> Schemas and DTDs -> JSON Schema
Mappings**. Add one mapping per schema file with the file pattern from the table
above.

Verify IntelliSense is working
------------------------------

Run this two-minute check to confirm the editor binds the schemas.

1. Open a file under ``pipelines/``, for example ``pipelines/bronze/customers.yaml``.
2. Start a new line at the top level and type ``pi``. The completion popup must
   suggest ``pipeline`` with the description "Pipeline name".
3. Add an invalid action type:

   .. code-block:: yaml

      pipeline: bronze
      flowgroup: customers
      actions:
        - name: load_data
          type: not_a_real_type

   VS Code must underline ``not_a_real_type`` and report that the value is not one
   of ``load``, ``transform``, ``write``, or ``test``.
4. Hover over ``actions``. The tooltip must show the property description from
   ``flowgroup.schema.json``.

If all four signals appear, the editor is configured correctly.

Troubleshoot a silent setup
---------------------------

If completions or validation do not appear, work through these checks in order:

* Confirm the YAML extension is enabled. Disabling and re-enabling it forces a
  language-server restart.
* Open ``.vscode/settings.json`` and verify the relative paths under
  ``yaml.schemas`` resolve. VS Code resolves them against the workspace root, so
  ``./schemas/flowgroup.schema.json`` only works when the schemas live in
  ``.vscode/schemas/`` (the leading ``./`` is workspace-relative, not file-relative).
* Reload the window with **Developer: Reload Window** after any edit to
  ``settings.json``.
* Check the **YAML** output channel (**View -> Output**, then pick "YAML" from the
  dropdown) for schema-load errors.

.. tip::

   Refresh your schemas after every LHP upgrade. Step 2 of the manual setup
   reads schemas from the installed package, so re-running that one-liner picks
   up new keys and enum values added in the release.

See also
--------

* :doc:`quickstart` — build your first pipeline with completions guiding the way.
* :doc:`architecture` — why LHP ships JSON schemas and how they relate to the
  Pydantic models the CLI validates against.
* :doc:`requirements` — system prerequisites including supported Python versions
  and Databricks workspace setup.
