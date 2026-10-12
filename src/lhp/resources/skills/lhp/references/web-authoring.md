# Optional LHP Web authoring

LHP Web is an optional local authoring interface over the same project files.
Use this interface when requested; the CLI and API operate on the same project.
The offline installation archives intentionally omit the `webapp` extra and its
dependencies. Do not try to download those dependencies without package access.

In an environment with access to the required packages, install
`lakehouse-plumber[webapp]` pinned to the same LHP version, then run `lhp web --help`
and `lhp web` from the project root. Read the actual startup output for the URL.
The interface supports graph/form editing and validation/generation over the
project. Show how an edit changes the underlying YAML; it is still the same LHP
project and generation contract.

Do not assume that a server launched on Databricks compute is reachable from the
user's browser. Genie users can use CLI/API tools. Explain
the optional desktop workflow instead of inventing a proxy, exposing a server or
claiming the interface is available. Stop any server you started when finished.
