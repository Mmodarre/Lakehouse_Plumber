# Field help catalogs

These versioned JSON catalogs provide reviewed guidance for the local frontend.
They are authored content; do not regenerate explanations by scraping paragraphs
or copying terse schema labels.

Each `{kind}.json` has `version: 1` and an `entries` array. Entries have a stable
`id`, `summary`, `sources` and `bindings`, plus optional `details`, `choices`,
`examples`, `unsetBehavior`, `constraints` and `relatedHelpIds`. A binding has a
`path` and optional `subtype`; action subtypes match the registry using
`load:cloudfiles`, `write:streaming_table`, etc. Configuration paths are relative
to the same schema roots as the forms. `*` matches an array index or user-defined
map key. An empty path provides action-level overview help.

Explain the decision, input format and consequence. Put an essential requirement
in `summary`; put examples and optional detail in the expanded help. Explain
omission separately from an effective inherited value. Never infer a runtime
default merely from the visible control or an enum's first option. Reuse an entry
across bindings only when their meanings and defaults are the same. Subtype help
must take precedence over generic action help.

`examples[].yaml` is valid YAML, not dotted path notation. Field examples are
fragments; examples labelled **Complete action example** additionally pass the
production action model and type-specific validators. File references in those
examples describe companion project files and do not claim the files already
exist in every project. Examples containing credentials use secret references.

For edits:

1. Inspect the linked local reference, relevant guide, and parser/generator when
   they disagree. Correct misleading documentation or schema descriptions in the
   same change. Add the source file to `sources` and a stable anchor when useful.
2. Keep IDs stable. Add concrete subtype/path bindings, including nested fields.
3. Run `python scripts/check_field_help.py`. It checks catalog structure, source
   references, links, examples, duplicate bindings and source-review drift.
4. Review every affected entry after a source document changes. Only after that
   review run `python scripts/check_field_help.py --refresh-sources` and commit
   the updated source-review manifest.
5. Run `pytest tests/test_field_help_catalog.py` and the frontend
   `field-help-catalog.test.ts` coverage guard. The guard enumerates the real
   action registry, including nested one-of branches and expectation rows.

A passing coverage check does not prove the prose is useful: review its meaning
in the running form, including defaults, subtype changes and viewer mode. Catalog
loading is local and category-based; it must not fetch remote documentation or
add the full catalog to the frontend entry chunk.

The embedded catalog is reviewed against the checked local sources recorded in
`source-review.json`. Documentation links are supplementary: rendered RST links
use the site's current documentation, while packaged reference Markdown links
point to the `release/V0.9.2` source branch. The shipped help remains available
without fetching either external site.
