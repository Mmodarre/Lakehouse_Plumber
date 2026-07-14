====================================================
Get YAML autocomplete and validation in your editor
====================================================

.. meta::
   :description: Point VS Code, Cursor, or any YAML LSP editor at Lakehouse Plumber's bundled JSON schemas so flowgroup YAML gets autocomplete, hover docs, and inline validation as you type.

A flowgroup is plain YAML, and plain YAML lets you write anything: ``type: laod``
instead of ``load``, a ``write_taget`` key that silently does nothing, a required
field left out. Nothing catches it while you type. You find out when you run
``lhp validate`` and read the error — or later, when ``lhp generate`` writes the
wrong Python.

Lakehouse Plumber ships a JSON schema for every file kind it reads. Point your
editor at those schemas and the editor becomes the first check: it completes keys
as you type, describes each field on hover, and underlines a bad action ``type``
the moment you write it — before the CLI ever runs. That is what this page buys
you: catch the typo while the cursor is still on it, not after a round trip
through the CLI.

Let's wire an editor to the schemas ``lhp init`` already put in your project.

What lhp init already set up
============================

Every project scaffolded with ``lhp init`` carries LHP's schemas with it, under
``.vscode/schemas/``:

.. code-block:: text
   :caption: .vscode/schemas/ after ``lhp init``

   .vscode/
     └── schemas/
         ├── flowgroup.schema.json       # your flowgroups under pipelines/
         ├── template.schema.json        # templates under templates/
         ├── preset.schema.json          # presets under presets/
         ├── project.schema.json         # lhp.yaml
         └── substitution.schema.json    # substitutions under substitutions/

``lhp init`` copies these five schemas straight from the installed ``lhp``
package, so they match the version of LHP you scaffolded with. ``flowgroup.schema.json``
is the one you lean on most — it describes every action key you can write under
``pipelines/``.

What ``lhp init`` does *not* do is tell the editor which schema applies to which
files. That mapping is the one step you add.

Point VS Code at the schemas
============================

Install the **YAML** extension by Red Hat (extension ID ``redhat.vscode-yaml``)
from the Extensions marketplace. It ships the YAML Language Server that reads JSON
Schema.

Then map each schema to the files it validates. Create ``.vscode/settings.json``:

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
       ]
     },
     "yaml.validate": true,
     "yaml.completion": true,
     "yaml.hover": true,
     "files.associations": {
       "*.yaml": "yaml",
       "*.yml": "yaml"
     }
   }

The ``yaml.schemas`` block is the load-bearing part: each key is a schema file
(resolved against the workspace root, which is why the path starts with
``./schemas/``), and each value is the glob of files that schema validates. The
globs mirror LHP's project layout, so a file under ``pipelines/`` is checked
against the flowgroup schema, ``lhp.yaml`` against the project schema, and so on.
If you keep flowgroups in a directory other than ``pipelines/``, add it to the
``flowgroup.schema.json`` entry.

Reload the editor after you save: run **Developer: Reload Window** from the
command palette (``Cmd+Shift+P`` or ``Ctrl+Shift+P``). VS Code rereads
``settings.json`` and binds the schemas.

.. note::

   A source or editable install of LHP also drops a ready-made
   ``.vscode/settings.json`` with this mapping, so check whether the file already
   exists before you write your own. Either way, the ``yaml.schemas`` block above
   is exactly what wires the editor to the schemas.

.. note::

   The schemas in ``.vscode/schemas/`` are copies pinned to the LHP version that
   ran ``lhp init``. After you upgrade LHP, refresh them so completions include
   the release's new keys and enum values — the simplest way is to run
   ``lhp init`` in an empty throwaway directory and copy its ``.vscode/schemas/``
   over yours.

Point any YAML LSP editor at the schemas
========================================

Cursor uses the same YAML Language Server as VS Code, so the ``.vscode/settings.json``
above works unchanged — install the YAML extension from the Open VSX registry and
reload.

For editors driven by ``yaml-language-server`` directly (Neovim, Helix, Zed),
give the language server the same schema-to-glob mapping, pointing at the schema
files LHP put in ``.vscode/schemas/``:

.. code-block:: yaml
   :caption: yaml-language-server schema mapping

   yaml.schemas:
     ./.vscode/schemas/flowgroup.schema.json:
       - pipelines/**/*.yaml
     ./.vscode/schemas/project.schema.json:
       - lhp.yaml
     ./.vscode/schemas/substitution.schema.json:
       - substitutions/**/*.yaml

JetBrains IDEs (PyCharm, IntelliJ) configure the same thing under **Settings ->
Languages & Frameworks -> Schemas and DTDs -> JSON Schema Mappings**: add one
mapping per schema file, each with the file glob it validates.

Confirm it works
================

Open a flowgroup under ``pipelines/`` and try three things:

1. On a new top-level line, type ``pi``. The completion popup offers ``pipeline``,
   described as "Name of the pipeline this flowgroup belongs to" — that text comes
   straight from ``flowgroup.schema.json``.
2. Give an action an invalid type:

   .. code-block:: yaml

      pipeline: bronze
      flowgroup: customers
      actions:
        - name: load_customers
          type: not_a_real_type

   The editor underlines ``not_a_real_type`` and reports that the value must be
   one of ``load``, ``transform``, ``write``, or ``test``.
3. Hover over ``actions``. The tooltip shows the field's description from the
   schema.

If all three signals appear, the schemas are bound. From here on, every flowgroup
you write is checked as you type — the mistake that used to surface as an
``lhp validate`` error now surfaces under your cursor, one keystroke after you
make it.

What's next
===========

- **Write your first flowgroup with completions guiding you.** The Get Started
  course walks through authoring a pipeline end to end; with the schemas bound,
  every key you type is offered and validated.
- **See every file kind LHP validates.** LHP also ships schemas for blueprints
  and instances in the ``lhp`` package; if you author those files, copy the
  matching schema into ``.vscode/schemas/`` and add a ``yaml.schemas`` entry for
  it. The configuration reference lists every file kind and every field in each.
- **Let the CLI have the final word.** The editor is the fast pre-check; it does
  not replace ``lhp validate``, which checks the same field definitions plus the
  cross-field rules a JSON schema cannot express. Author with schema completions,
  then validate before you generate.
