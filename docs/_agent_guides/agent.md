# Build with Lakehouse Plumber

Copy the complete prompt below into your coding agent. It contains its own setup instructions and does not require the agent to open this page.

---

Help me build with Lakehouse Plumber (LHP). Install and use the lhp development skill using the instructions below, then continue my project request. If I have not described a project yet, ask what pipeline I want to build after setup.

# Set up LHP and its bundled skill

Perform routine setup yourself using your available tools. Preserve existing work
and project pins. Ask only for information or actions you cannot resolve yourself;
give exact instructions when I must act. Respect the environment's approval settings.

## 1. Identify the execution environment

Identify your agent host and interface, available terminal/notebook and file tools,
and where Python actually runs. Check that environment's OS, architecture, Python
version, pip and persistent project location. The underlying model name does not
identify the agent host. Agent browsing, compute downloads and pip access are
separate capabilities; test the route you need instead of assuming internet access.

LHP requires Python 3.11 or newer. Reuse a suitable isolated environment or create
one for desktop work. In Databricks, use notebook-scoped installation and available
compute. If execution, Python or compute is unavailable, guide me through the
specific missing step, then verify the result. Do not install into system Python
or assume your remote environment is my laptop.

## 2. Check the existing installation

In the selected interpreter, check the installed `lakehouse-plumber` version,
package import and `lhp skill export --name lhp` into a temporary
folder. Verify the selected skill's `SKILL.md` and its linked local resources.
A version number alone is insufficient: an earlier test build can have the same
version without this skill selector or curriculum.
Genie also needs `SkillFacade.install_genie_skill`. Reuse a working version
**0.9.3 or newer**; do not upgrade it just because a newer release exists. An older
version, broken import or missing required capability needs repair or installation.

## 3. Obtain a suitable package when needed

First test the configured pip source with a real download into a temporary folder:
use `pip download --no-deps --no-cache-dir --only-binary=:all:` with
`--dest <temporary-folder>` and the quoted requirement `"lakehouse-plumber>=0.9.3"`,
through the selected interpreter or notebook pip tool. Substitute a real folder.
An installed package or cached result does not establish package-source access.

If the configured source cannot supply a suitable release, try public PyPI using
`--index-url https://pypi.org/simple` for that operation, where permitted. Do not
change persistent index settings. An internal index offering only older versions
is insufficient. Distinguish unavailable releases from authentication, network,
Python compatibility, permissions and dependency failures; do not retry indefinitely.

Install the retrieved wheel and its dependencies in the selected environment.
Check dependencies separately: downloading LHP with `--no-deps` is not a complete
installation. If dependencies cannot be retrieved, continue to the offline route.

## 4. Fall back to a complete offline bundle

If pip cannot provide a usable installation, inspect the assets at:
[https://github.com/Mmodarre/Lakehouse_Plumber/releases/latest](https://github.com/Mmodarre/Lakehouse_Plumber/releases/latest)

Select the **complete offline ZIP**, not the standalone LHP wheel, for this
fallback. It contains LHP, both skills and their references, all pinned CLI/Genie
dependencies, and `install.py`. Python and pip must already be present; LHP Web's
optional dependencies are excluded. Use this bundle when pip has no package-source
access, dependencies cannot be retrieved, or the available indexes have no suitable
LHP release. Do not ask me to collect dependency wheels individually.

Choose a release at least 0.9.3 matching the **environment running Python**,
which may differ from my computer or browser. Match its OS, CPU architecture and
Python major/minor version. Published offline bundles target **CPython 3.12** on
Linux x86_64/ARM64, macOS Apple Silicon and Windows x86_64. Names follow
`lhp-offline-VERSION-SYSTEM-ARCH-py312.zip`:
`SYSTEM` is `linux`, `darwin` (macOS) or `windows`; `ARCH` is `x86_64` (Intel/AMD)
or `arm64`; `py312` means Python 3.12. Inspect the actual assets and manifest;
do not substitute another platform or Python version if a match is missing.
For Genie, inspect Databricks compute, not the computer running my browser.
If the offline target uses another Python version, help me select an available
Python 3.12 interpreter or compatible Databricks compute before installation.
If that is unavailable, or the platform is unsupported (including Intel macOS),
explain that no matching official offline bundle is provided and pause until a
compatible environment or custom bundle is available. Do not change system Python.

Retrieve compatible files yourself when your tools can. Only ask me to transfer
files when you cannot. **For every coding agent**, a download handoff must include:

1. **Target and file:** the detected execution OS, architecture and Python version,
   the exact asset filename to download, and why it matches. If a required detail
   cannot be detected, ask for it instead of guessing.
2. **Clickable links:** show the full HTTPS release-page URL above in your reply.
   When assets are verified, also show their full, clickable download URLs and
   the published checksum-file URL. Do not merely say "download from GitHub".
   If you cannot inspect assets, state that availability is unverified, give the
   matching filename pattern and ask me to share the asset list if needed.
   Never invent a release, filename or download URL.
3. **Where to put it and what to share:** give the appropriate instructions below
   for the complete ZIP and its published checksum file. A connected computer can
   download them for transfer into the offline environment. Do not leave me to
   infer the next step.

For **Genie Code or another agent installing into Databricks**, direct me to
**Workspace Files**: open **Workspace**, choose a writable personal or project
folder, open its menu, select **Import > File**, and upload the complete ZIP.
Explain that Workspace import extracts ZIPs, so guide me through verifying its
published checksum before import. Ask me to **paste the imported folder's workspace
link** in this conversation. Resolve that link with available tools, verify the
corresponding `/Workspace/...` path, and locate `manifest.json` and `install.py`
inside it. If you cannot resolve the link, ask me to right-click the folder and
choose **Copy URL/path > Full path**. A browser link is not a pip filesystem path.
Do not direct me to create or use a Unity Catalog volume for this setup. If
Workspace Files is unavailable, explain the specific blocker and establish an
accessible alternative with me.

For **Codex, Claude Code, Copilot or another agent using local Python**, ask me to
download the selected ZIP and checksum into a folder on my machine and **share the
ZIP's full path**. Verify and extract it yourself when accessible. If the agent
runs in a container, remote host or cloud workspace, a laptop path may not be accessible: give the host's
supported upload or copy steps, then ask for the destination path. Match the
package to that execution environment, even when my laptop uses another OS.

If no suitable release exists, explicitly say there is no compatible published
offline ZIP at the release link and ask whether I can supply a compatible offline
bundle. Do not substitute an older release or recommend the standalone wheel as a
complete offline installation. Pause installation until a suitable file is
available. When I supply a file, link or path, continue
from there rather than repeating failed downloads or requiring a published
release. Verify the archive checksum before extraction, then verify its manifest
and files and run `install.py` with the target interpreter. The installer uses only
the bundled wheels and blocks network access during installation.

If I explicitly supply a standalone wheel, including an unpublished test build,
check its metadata and checksum and establish that its dependencies are installed
or locally available before using it. Otherwise explain that the complete offline
bundle is needed. The LHP wheel itself is universal (`py3-none-any`); its dependencies
are why the complete ZIP must match the target platform.

Verify the installed version, imports and required capabilities before continuing.
Restart notebook Python only if needed, then repeat verification. Retain the exact
version and installation source for later sessions.

## 5. Install and read the bundled skill

The selected `lhp` skill and its references are inside the installed
package. Install only this skill for this request; do not load the other skill's
entrypoint. Both skills use the same maintained technical reference material.
Do not require a separate download, plugin or LHP Web installation.

For **Genie Code**, run in authenticated notebook Python:

```python
from lhp.api import SkillFacade
result = SkillFacade().install_genie_skill(skill_name="lhp")
print(result.install_dir)
```

This uses the authenticated workspace user and installs into
`/Users/<user>/.assistant/skills/lhp` (filesystem: `/Workspace/Users/...`). Reuse a
compatible Databricks SDK or install the package's optional `genie` extra from the
same package source. The complete offline bundle already includes it. Do not
assume a subprocess inherits notebook authentication. If SDK installation is
unavailable but workspace file tools work,
export the skill locally and copy its complete contents to that workspace folder.

For **desktop coding agents**, use `lhp skill export --name lhp --output <skill-directory>`
in the selected environment. Default to project-local installation:
`.agents/skills/lhp` for Codex, `.claude/skills/lhp` for Claude Code, and
`.github/skills/lhp` for Copilot. Respect an explicitly configured location. For
other agents, use a verified supported location or export into the project and
read the files explicitly. Do not infer native discovery from a directory name.

Verify the exported files. Preserve differing existing skills. If installation
is blocked, read the packaged skill directly where your tools permit:

```python
from importlib.resources import files
root = files("lhp.resources.skills.lhp")
print(root.joinpath("SKILL.md").read_text())
```

When reading package resources directly, common technical references live under
`lhp.resources.skills.lhp.references`; portable export assembles the selected skill
into one self-contained folder.

Read the selected `SKILL.md` now and follow its relevant local references. Native skill discovery
is a separate check; request a new chat only when necessary, with a resume prompt.

## 6. Continue my original request

Briefly report the verified environment, LHP version, skill location and project
location, then follow the selected skill's workflow. During installation, handle
routine installation without unnecessary confirmations, then respect the selected
skill's interaction rules. Offer deployment and pipeline execution separately;
report only results actually verified. Public documentation access is not required.
