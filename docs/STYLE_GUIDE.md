# LHP Documentation Style Guide

This style guide is read by every writer and reviewer subagent before authoring or
evaluating LHP documentation pages. Follow every rule. Reviewers cite rule numbers.

The rendered docs site uses reStructuredText (RST) with Sphinx and the Furo theme.
This file is Markdown because it documents RST conventions; it is excluded from the
Sphinx build via `conf.py` `exclude_patterns`. Do not add `:doc:` or `:ref:` links
inside this file — refer to other documents by plain prose (for example, "see
`quickstart.rst`" or "see CHANGELOG.md").

## Diátaxis purity (rules 1–8)

1. **Tutorial = learning.** Tutorials lead a beginner through a single guided path
   to a working result. Never branch, never explain alternatives, never link out
   mid-flow. [Diátaxis]
2. **How-to = task.** How-to pages answer "how do I do X?" for a reader who already
   knows what X is. Skip background; jump to the steps. [Diátaxis]
3. **Reference = facts.** Reference pages describe the API surface — options,
   defaults, types, error codes — exhaustively and without narrative. [Diátaxis]
4. **Explanation = understanding.** Explanation pages discuss why the system behaves
   as it does. They may speculate, contrast alternatives, and use diagrams. They
   never contain step-by-step instructions. [Diátaxis]
5. **Do not mix modes on one page**, with one exception. A page is exactly one
   of tutorial, how-to, reference, or explanation. Reference pages MAY begin
   with a "Concept" section (What/When/minimum example) before the reference
   body; this is the only sanctioned cross-mode pattern. All other mode mixing
   requires splitting the page. [Diátaxis, User-resolved]
6. **Tutorial litmus:** does it work end-to-end for a first-time user with no prior
   LHP knowledge? If not, it is a how-to. [Diátaxis]
7. **How-to litmus:** does the title fit the pattern "How to <verb> <object>"? If
   the page covers more than one task, split it. [Diátaxis, User-resolved]
8. **Reference litmus:** would a reader scan it for a specific value rather than
   read it linearly? If they would read it, move the narrative parts into
   explanation — except for the Concept intro permitted under rule #5. [Diátaxis]

## Voice and tense (rules 9–14)

9. **Second person, always.** Address the reader as "you". Never write "we",
   "let's", "our", or "the user" in any page. [Google, User-resolved]
10. **Imperative mood for instructions.** Write "Run `lhp validate`", not "You can
    run `lhp validate`" or "The user runs `lhp validate`". [Google, Microsoft]
11. **Present tense.** Describe what the system does, not what it will do or did do.
    "LHP generates a Python file" beats "LHP will generate a Python file". [Google]
12. **Active voice.** Name the actor. "LHP resolves substitutions" beats
    "Substitutions are resolved". [Google, Microsoft]
13. **No marketing language.** Avoid "powerful", "seamless", "robust", "simply",
    "easily". State the capability and let the reader judge. [Microsoft]
14. **No hedging.** Drop "basically", "essentially", "just", "simply", and "of
    course". They add no information and sound condescending. [Microsoft]

## RST structural conventions (rules 15–22)

15. **Heading characters: exactly `=`, `-`, `~`, `^` for h1 through h4.** No
    overlines. H5 and deeper are not allowed — restructure or split the page if you
    need a fifth level. [User-resolved, Sphinx]
16. **One h1 per page**, matching the file's purpose. Place it at line 1 with the
    underline immediately below. [Sphinx]
17. **Every page starts with a `.. meta:: :description:` block** in the first ten
    lines, giving a one-sentence summary for search engines and the in-app docs
    search. [Sphinx, Google]
18. **Use `.. code-block:: <language>` for all code samples.** Never use
    indented-only literal blocks. Add `:caption:` when the snippet is a named file
    and `:linenos:` only when prose later refers to specific line numbers. [Sphinx]
19. **Use `.. mermaid::` for diagrams** so they render as text in source and as
    SVG in HTML. Avoid embedded PNG screenshots of diagrams — they age badly and
    fail accessibility. [Sphinx, Microsoft]
20. **Admonition budget: at most three of `.. note::`, `.. warning::`, or
    `.. tip::` per page combined.** Prefer prose. If you reach four, you are
    using admonitions as emphasis — rewrite. [Furo, User-resolved]
21. **Use `.. seealso::` for cross-references at the foot of a section** when one
    or two related pages deserve a pointer. Reserve a dedicated "See also" h2 for
    page-level link clusters. [Sphinx]
22. **Mark new and changed behavior with `.. versionadded::` and
    `.. versionchanged::`** directly under the affected heading. Cite the LHP
    version, not a date. [Sphinx]

## Naming and terminology (rules 23–28)

23. **Substitution syntax is `${token}`, never `{token}`.** The bare-braces form is
    deprecated. The only non-`$` braces syntax is `%{local_var}` for local
    variables. [User-resolved]
24. **Product names: spell them out, then capitalize correctly.** Use "Lakehouse
    Plumber (LHP)" on first mention per page, then "LHP" thereafter. Write
    "Databricks Lakeflow Declarative Pipelines" in full at first use; "DLT" is the
    deprecated name and only appears in migration notes. [User-resolved, Microsoft]
25. **Capitalize Databricks feature names exactly as Databricks does:** "Auto
    Loader" (two words, both capitalized), "Unity Catalog", "Asset Bundle",
    "Delta Live Tables" (legacy). [Microsoft]
26. **LHP domain terms are capitalized concepts, not proper nouns:** "FlowGroup",
    "Action", "Pipeline", "Preset", "Template". Use them as defined in
    `concepts.rst`; do not invent synonyms. [User-resolved]
27. **Never abbreviate at first use.** Spell out "Databricks Asset Bundle (DAB)",
    "Change Data Capture (CDC)", "directed acyclic graph (DAG)" the first time a
    page mentions them. [Microsoft]
28. **File paths and CLI flags appear in `double backticks` in RST.** Treat
    `pipelines/`, `substitutions/dev.yaml`, `--env`, and `lhp generate` as code,
    not prose. [Sphinx]

## Code examples (rules 29–31)

29. **Examples must be runnable.** A reader who copy-pastes the snippet and runs
    it (with documented prerequisites) gets the documented result. No
    pseudo-code, no `...` placeholders unless explicitly marked as such. [Google]
30. **Minimal context, maximum completeness.** Show every line needed to make the
    example work — required imports, the full YAML block, the exact CLI command —
    but omit unrelated configuration. [Google, Microsoft]
31. **Use `:caption:` for any code block tied to a real file** so readers know
    where the snippet belongs. Pair `:linenos:` only with prose that references
    specific lines (for example, "line 7 declares the streaming source"). [Sphinx]

## Cross-linking (rule 32)

32. **Diátaxis reverse-links:** Explanation pages place one "If you want to do X,
    see `<how-to>`" near the top. How-to pages place links to the corresponding
    explanation **and** reference at the bottom. Reference pages place links to
    relevant how-tos at the bottom. Tutorials do not need reverse-links. [Diátaxis]

Additional cross-link rules:

- Use `:doc:` for whole-page references and `:ref:` for in-page anchors. [Sphinx]
- Use `:term:` only after the glossary is stable; until then, link to the
  definition's page with `:doc:`. [Sphinx]
- Never write "click here" or "this link" as link text. The link text must
  describe the destination — "see the configuration reference", not "see
  [here](...)". [Microsoft]

## Length and scope (rules 33–37)

33. **How-to pages are at most 1500 words.** If a how-to exceeds this, the task
    is probably two tasks — split it. [User-resolved]
34. **Tutorial pages are at most 2000 words.** A tutorial longer than this is
    likely a multi-page learning path; restructure it as such. [Diátaxis,
    User-resolved]
35. **Reference pages have no fixed word ceiling** — they scale with the API
    surface. Use tables and consistent headings so readers can scan. [Diátaxis]
36. **No marketing pages.** Every page must serve one of the four Diátaxis modes.
    "Why LHP" prose belongs in `index.rst` or `concepts.rst`, never as a standalone
    page. [Diátaxis, User-resolved]
37. **One task per how-to.** A how-to titled "Configure Bundles and Deploy and
    Monitor" is three how-tos. Split by the smallest verb-object unit the reader
    might search for. [User-resolved]

## Accessibility (rules 38–40)

38. **Every image and diagram has `:alt:` text** describing the content in one
    sentence. For Mermaid diagrams, prefer the source itself as accessibility —
    screen readers can read the graph definition. [Microsoft]
39. **No color-only signals.** If a status indicator uses red or green, pair it
    with an icon or word ("error", "ok"). Colorblind readers and grayscale prints
    must still convey the same meaning. [Microsoft]
40. **Descriptive link text.** Link text must make sense out of context. Never
    "click here", "this link", "more". A reader scanning a page of links must
    know each destination. [Microsoft, Google]

## Changelog and meta (rules 41–42)

41. **Changelog follows Keep a Changelog 1.1.0.** A single rolling page at
    `docs/changelog.rst` uses a MyST `include` of the repository-root
    `CHANGELOG.md`. Releases use `## [version] — YYYY-MM-DD` headers and group
    entries under `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`,
    `Security`. [KaC, MyST, User-resolved]
42. **Docs and skill-MD drift is accepted by design.** When factual content
    changes, the writer updates both the user-facing RST page and the
    AI-targeted skill MD in the same patch, but voice may differ — `docs/`
    targets humans and skill MDs target agents. Reviewers check fact parity,
    not wording parity. [User-resolved]

## Reference Concept intro (rule 43)

43. **Reference pages MAY open with a Concept section.** Under the rule #5
    exception, a Reference page may precede its reference body with a brief
    "Concept" section answering What / When / minimum example. The Concept
    section is a tutorial-style on-ramp for first-time readers; the reference
    body that follows is the page's canonical pure-reference content. Apply
    these constraints:

    - **Length cap:** the Concept section is ~150 words combined across all
      three parts (What, When, minimum example). If it grows past that, the
      content belongs in a separate explanation or how-to page.
    - **What:** one paragraph defining the page's topic in plain language. No
      option tables, no flag lists.
    - **When:** one short paragraph or decision aid for when to use this vs.
      adjacent topics. Pull from `decisions.rst` if a relevant matrix exists.
    - **Minimum example:** the smallest working snippet (typically 5–20 lines
      of YAML or Python). Every line must serve the example; no placeholders.
    - **Required hand-off to the reference body:** end the Concept section
      with an inline jump or open the reference body under an h2 clearly
      labelled "Reference", a sub-type name, or an equivalent heading that
      signals the mode switch.
    - **Voice:** the Concept section follows the same voice rules as the
      rest of the page (rules 9–14). It is not an excuse for marketing prose.

    Pages that have no first-time-reader audience (pure catalogs like the
    glossary, changelog, errors reference, and API reference) skip the Concept
    section entirely. [Diátaxis, User-resolved]

## Litmus tests

Reviewers apply these checks before approving a page.

**Tutorial:**

- Does a first-time reader with no prior LHP knowledge reach a working result by
  following only this page?
- Is there exactly one path through the page, with no "alternatively" branches?
- Does every step have a verifiable success signal (file appears, command exits
  zero, output matches)?

**How-to:**

- Does the title fit "How to <verb> <object>" or a close equivalent?
- Could a reader who already knows what they want skip the introduction and start
  at step one?
- Does the page cover exactly one task, with related tasks linked rather than
  inlined?

**Reference:**

- Can a reader find a specific option, default, or error code in under fifteen
  seconds via the page's table of contents or section headings?
- Is every option's type, default, and accepted values documented?
- Is narrative ("you might want to…") absent or minimal?

**Explanation:**

- Does the page answer "why?" rather than "how?"
- Are step-by-step instructions absent (those belong in a how-to)?
- Does the page link out to relevant how-tos near the top so action-oriented
  readers can leave?

## Voice-anchor excerpts

These excerpts demonstrate the target tone. Match this register.

From `quickstart.rst`:

> "Build your first Lakehouse Plumber pipeline in about ten minutes. This
> walk-through uses the `samples.tpch.customer_sample` table that ships with
> every Databricks workspace, so you do not need to upload data first."

From `architecture.rst`:

> "Lakehouse Plumber keeps a small **state file** under `.lhp_state.json` that
> maps generated Python files to their source YAML. It records checksums and
> dependency links so that future `lhp generate` runs can re-process only new
> or stale FlowGroups."

From `configure_bundles.rst`:

> "This how-to walks you through enabling Databricks Asset Bundle (DAB)
> integration in your LHP project: `databricks.yml` setup, generation,
> deployment commands, and the environment-specific configuration pattern most
> teams adopt for prod."
