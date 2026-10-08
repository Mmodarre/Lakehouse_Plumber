# Onboard an agent to Lakehouse Plumber

You are helping a user set up a local Lakehouse Plumber (LHP) project. Complete the setup in their workspace, using an isolated Python environment, then load the LHP authoring skill. This guide works as instructions for any coding agent with terminal and filesystem access.

Describe LHP as a **modern ETL framework** for Databricks Spark Declarative Pipelines. Teach its core model as load actions → transform actions → write actions, with test actions checking the data alongside the flow. Native Python supports inspection and debugging; it is an implementation detail, not the product category.

## Check where you can work

First identify the intended project location and your actual execution access. Databricks Genie Code operates inside the Databricks workspace; do not assume it can access the user's laptop, a local terminal, or a persistent Python environment. Distinguish the workspace project folder from temporary compute storage.

If you have a suitable shell, filesystem, and persistent project directory, follow the setup below there. If those tools are unavailable, ask the user which local terminal or development environment they will use and guide them through the same commands. Wait for their output to verify each stage; do not report that installation or scaffolding happened merely because you supplied commands. Do not automatically substitute notebook-scoped package installation or an ephemeral compute directory for the requested project setup.

If you cannot retrieve this guide or a referenced skill, ask the user to paste its contents or provide an accessible location. Do not invent the missing instructions.

## 1. Establish the project choice

Ask the user together, before scaffolding:

- **Sample or empty project?** The sample is a TPC-H example with ingestion, transformations, and outputs to explore. The empty project provides the structure for their own pipelines.
- **What project name and directory?** Offer the current directory if suitable. Explain that `lhp init NAME` creates files in the current directory; NAME does not create a directory.

Wait for the sample/empty answer. Do not silently choose a sample. Reuse a choice already given in the conversation. Inspect the destination before changing it. If `lhp.yaml` already exists, treat it as an existing project and ask whether to onboard there or create a different project; never remove it to force scaffolding. If other files would conflict, choose a separate directory with the user.

## 2. Prepare an isolated Python environment

Inspect the operating system, shell, available Python interpreters, and whether `uv` is installed. LHP requires Python 3.11 or newer; prefer a compatible installed Python, with 3.12 as a conservative default for a new environment. Check the package's current Python requirement if installation reports a conflict.

Create and enter the chosen project directory. Reuse an appropriate existing project environment; do not overwrite an existing `.venv` or modify an unrelated environment.

If `uv` is available, use it:

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python lakehouse-plumber
```

`uv` can obtain the requested Python if it is missing. Respect the user's environment/network constraints. On Windows, the environment interpreter is `.venv\Scripts\python.exe`; use that path with `uv pip install --python`.

Otherwise use Python's built-in `venv`:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install lakehouse-plumber
```

First verify that the selected `python3` meets LHP's Python requirement. On Windows PowerShell, for example:

```powershell
py -3.12 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install lakehouse-plumber
```

If neither a usable Python nor `uv` is available, explain the missing prerequisite and help the user install one through the appropriate official instructions. Do not install LHP into system Python or use `sudo pip`. Respect any environment-specific approval requirements.

For subsequent commands use the environment's CLI executable explicitly: `.venv/bin/lhp` on macOS/Linux or `& .\.venv\Scripts\lhp.exe` in PowerShell. Activation is optional; do not assume it persists between agent tool calls. The commands below use the macOS/Linux spelling—adapt the executable path to the user's OS.

## 3. Check the installed CLI and scaffold

```sh
.venv/bin/lhp --version
.venv/bin/lhp init --help
```

Substitute the chosen project name for `PROJECT_NAME`, passing it as a single safely quoted argument. Run from the selected project directory.

For the sample:

```sh
.venv/bin/lhp init PROJECT_NAME --sample
```

For an empty project:

```sh
.venv/bin/lhp init PROJECT_NAME
```

The current CLI enables bundle scaffolding by default. `--sample` cannot be combined with `--no-bundle`. The sample may initialize a local Git repository; if the user does not want this, use the documented `--no-git` option. Never run both scaffold commands against the same destination. Check the installed command help before using options that differ across releases.

After scaffolding, verify that `lhp.yaml` and the expected project directories exist. Ensure `.venv/` is ignored by Git, preserving existing ignore rules.

## 4. Install and load the authoring skill

From the project root, inspect the installed skill commands:

```sh
.venv/bin/lhp skill --help
```

The current command is **`lhp skill install`**, not `lhp skill add`:

```sh
.venv/bin/lhp skill install
.venv/bin/lhp skill status
```

If the skill is already installed, check its status. Use the documented update command if needed, preserving user modifications and reporting any conflict rather than forcing an overwrite.

The current installer writes `.claude/skills/lhp/SKILL.md` and its references, and adds a routing block to `CLAUDE.md`. This is the CLI's Claude Code integration. Other agents can use the same Markdown: open the installed `SKILL.md` now and read the references needed for the next task. Do not claim the installer configured Codex, Gemini, or Copilot's native skill discovery. If persistence across sessions is needed, use the active agent's supported project instructions to point at the installed skill, preserving existing instructions.

For Genie Code, do not claim `lhp skill install` registered a native Genie skill. Once the installed LHP skill and its references are accessible in the Databricks workspace, read them for the current task. If the user wants native discovery, use Genie Code's **Open skills folder** setting to locate their user skills folder and copy the complete installed `lhp` skill directory there, preserving relative references and existing user changes. Verify its `SKILL.md` and references before reporting registration. Use a user-scoped skill; do not publish a workspace-wide skill as part of onboarding. Follow the current [Genie Code skill instructions](https://docs.databricks.com/aws/en/genie-code/skills) for the workspace UI and supported locations.

Follow the loaded skill for authoring work. In particular, inspect the project's shared presets and templates before writing new pipeline definitions, keep environment-specific settings in substitutions, and validate configuration before generating Python.

## 5. Hand back a verified local project

Report:

- Project path and whether the user chose the sample or empty scaffold.
- Python/environment method and installed LHP version.
- Authoring-skill location and status, including whether it has been read in this conversation.
- How to run the environment's `lhp` executable again.
- The next concrete step: configure the workspace/catalog and inspect the sample, or describe the first pipeline to build.

Installation and scaffolding do not mean a pipeline has been deployed. Do not report successful validation/generation unless those commands actually ran successfully. A new scaffold still needs project-specific configuration; inspect its README and generated configuration before proposing commands. Do not invent Databricks credentials, catalog names, or deployment results, and do not deploy as part of this onboarding request.

## References

- [LHP documentation](https://mmodarre.github.io/Lakehouse_Plumber/)
- [LHP source](https://github.com/Mmodarre/Lakehouse_Plumber)
- [uv environments](https://docs.astral.sh/uv/pip/environments/)
- [Python virtual environments](https://docs.python.org/3/library/venv.html)
- [Genie Code execution context](https://docs.databricks.com/aws/en/notebooks/code-assistant)
- [Genie Code skills](https://docs.databricks.com/aws/en/genie-code/skills)
