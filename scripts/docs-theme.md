# LHP documentation theme

The approved [Superdesign draft, version 4](https://p.superdesign.dev/draft/7e4e3c78-9946-440f-b8d0-10129b4c6c6f) is implemented as local Furo template overrides, CSS and JavaScript. The existing RST sources, URLs and toctree remain authoritative. New sections appear through the normal toctree; there is no fixed list of navigation groups in the theme.

## Build and preview

From the repository root, in an isolated Python environment:

```sh
pip install -e . -r docs/requirements.txt
sphinx-build -W --keep-going -b html docs docs/_build/html
python3 -m http.server 8767 --directory docs/_build/html
```

Open http://localhost:8767/. Check the homepage, Install and scaffold, a nested reference page and Search, in light and dark themes and on mobile.

The theme uses Furo's native search index, theme preference, heading outline and page navigation. Custom code adds the header, mobile navigation and agent dialog. Logos, fonts and agent artwork are served locally; font and agent licences accompany the assets. Documentation illustrations are local PNG assets with descriptive alternative text and full-size links. The existing analytics configuration is retained.

## Browser smoke test

The browser check is separate from the application frontend dependencies:

```sh
npm install --prefix /tmp/lhp-docs-browser playwright@1.63.0
/tmp/lhp-docs-browser/node_modules/.bin/playwright install chromium
NODE_PATH=/tmp/lhp-docs-browser/node_modules node scripts/docs_theme_smoke.cjs
```

Set `DOCS_BASE_URL` to test another HTTP server, including a deployment under a subpath. The check covers seven viewport widths, four real pages, navigation preservation, active-page scrolling, native search on desktop and mobile, code copying, persisted theme changes, all ten agent prompt choices, dialog focus and mobile navigation. The full Sphinx build also runs in the existing documentation CI workflow.

## Shared agent guides

`docs/_agent_guides/agent.md` and `learn.md` are the canonical setup and learning instructions. Both sites use the raw GitHub URLs under `main/docs/_agent_guides/`. Sphinx additionally publishes byte-identical mirrors at the documentation root through `html_extra_path`.

The companion website branch is `docs/shared-agent-guides` in the adjacent `lhp_dot_dev` repository. It changes its prompts and agent page to these repository URLs; the old website `/agent.md` and `/learn.md` become compatibility pointers.

Publication order matters: merge the guides into Lakehouse_Plumber's **main** branch before deploying the website link changes. A merge into `release/V0.9.3` alone does not make the raw-main URLs available. Neither the branch nor a local preview publishes the guides.

## Review scope

The implementation is based on the current `release/V0.9.3`, including its merged documentation reorganisation. Existing in-progress work in the original checkout was preserved. PR creation requires the user's final confirmation.

## Documentation diagrams

The three former Mermaid diagrams and the quarantine recovery illustration are
local generated PNGs. Select any diagram to open its full-resolution image;
alternative text and adjacent prose carry the explanation without the image.
Their warm canvas stays the same in both reading themes.

See [the diagram audit](docs-diagrams/diagram-audit.md) for implementation
evidence, the proposed visual backlog, and the page-level coverage table.
Generation prompts and Superdesign asset/draft records are kept alongside it.
Additional concepts and the homepage capability overview await user approval.
