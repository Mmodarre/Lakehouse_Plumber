/**
 * Monaco Editor worker configuration.
 *
 * Must be imported before any Monaco usage (first import in main.tsx)
 * to prevent the default CDN loader from kicking in.
 *
 * Uses the ESM entry point to import only the core editor API + the
 * specific language contributions we need, avoiding 8MB+ of unused
 * workers (TypeScript, CSS, HTML).
 */
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api'
import { loader } from '@monaco-editor/react'
// Language contributions — Monarch tokenizers (main thread, no workers)
import 'monaco-editor/esm/vs/basic-languages/yaml/yaml.contribution'
import 'monaco-editor/esm/vs/basic-languages/python/python.contribution'
import 'monaco-editor/esm/vs/basic-languages/sql/sql.contribution'
import 'monaco-editor/esm/vs/basic-languages/markdown/markdown.contribution'
import 'monaco-editor/esm/vs/basic-languages/shell/shell.contribution'
import 'monaco-editor/esm/vs/basic-languages/ini/ini.contribution'

// JSON language service (uses its own worker for validation)
import 'monaco-editor/esm/vs/language/json/monaco.contribution'

// Workers
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'
import jsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker'
// monaco-yaml ships its own language-server worker. Per the monaco-yaml Vite
// recipe this is imported via the `?worker` query so Vite emits it as a
// dedicated worker chunk (the package has no `exports` map, so the
// `yaml.worker` subpath resolves directly to its `yaml.worker.js` file).
import yamlWorker from 'monaco-yaml/yaml.worker?worker'

import { fetchSchema, type SchemaKind } from '../api/schemas'

self.MonacoEnvironment = {
  getWorker(_workerId: string, label: string) {
    if (label === 'json') return new jsonWorker()
    // monaco-yaml registers its language under the 'yaml' worker label.
    if (label === 'yaml') return new yamlWorker()
    return new editorWorker()
  },
}

// Use locally-bundled Monaco instead of CDN
loader.config({ monaco })

// ── Shell-matching editor themes ────────────────────────
//
// `editor.background` is the exact hex equivalent of the app's `--card`
// token (light `oklch(1 0 0)` / dark `oklch(0.205 0.011 258)`), so the
// editor surface sits seamlessly inside a `bg-card` panel in both themes.
// Selection tints derive from `--primary` (light `oklch(0.54 0.19 259)` /
// dark `oklch(0.66 0.16 257)`).

const LIGHT_PRIMARY = '#1968da'
const DARK_PRIMARY = '#4c92f2'

monaco.editor.defineTheme('lhp-light', {
  base: 'vs',
  inherit: true,
  rules: [],
  colors: {
    'editor.background': '#ffffff',
    'editor.selectionBackground': `${LIGHT_PRIMARY}2e`,
    'editor.inactiveSelectionBackground': `${LIGHT_PRIMARY}14`,
    'editor.selectionHighlightBackground': `${LIGHT_PRIMARY}1f`,
  },
})

monaco.editor.defineTheme('lhp-dark', {
  base: 'vs-dark',
  inherit: true,
  rules: [],
  colors: {
    'editor.background': '#14171c',
    'editor.selectionBackground': `${DARK_PRIMARY}40`,
    'editor.inactiveSelectionBackground': `${DARK_PRIMARY}1f`,
    'editor.selectionHighlightBackground': `${DARK_PRIMARY}2e`,
  },
})

/** Monaco theme name for a resolved app theme. */
export function monacoThemeFor(resolved: 'light' | 'dark'): 'lhp-light' | 'lhp-dark' {
  return resolved === 'dark' ? 'lhp-dark' : 'lhp-light'
}

/**
 * Per-kind `fileMatch` globs mapping the LHP project layout to its canonical
 * schema.
 *
 * Monaco models are created by `@monaco-editor/react` via `monaco.Uri.parse(path)`
 * where `path` is the project-relative file path (no scheme). With non-strict
 * parsing Monaco coerces this to the `file` scheme and absolutises the path, so
 * a model path like `pipelines/01_bronze/orders.yaml` becomes the URI
 * `file:///pipelines/01_bronze/orders.yaml`.
 *
 * The yaml-language-server (in the worker) matches `fileMatch` patterns as
 * end-anchored substrings of that URI string, with `*` matching across path
 * separators. The patterns below are therefore written to match the trailing
 * project-relative portion of the URI:
 *   - `pipelines/` flowgroups may be nested arbitrarily, hence the `**` segment.
 *   - `lhp.yaml` lives at the project root, so an exact filename suffix suffices.
 */
const SCHEMA_FILE_MATCH: Record<SchemaKind, string[]> = {
  flowgroup: ['pipelines/**/*.yaml', 'pipelines/**/*.yml'],
  preset: ['presets/*.yaml', 'presets/*.yml'],
  template: ['templates/**/*.yaml', 'templates/**/*.yml'],
  substitution: ['substitutions/*.yaml', 'substitutions/*.yml'],
  project: ['lhp.yaml', 'lhp.yml'],
}

let yamlConfigured = false

/**
 * Configure monaco-yaml lazily, after the editor has bootstrapped.
 *
 * This is intentionally NOT run at module import time — it is invoked once the
 * first editor mounts (see the editor modals' `onMount`). Schemas are fetched
 * from the backend ourselves (so `enableSchemaRequest` is left off) and passed
 * inline. Any failure degrades gracefully: YAML validation is simply skipped
 * and the editor remains fully usable.
 */
export async function setupMonacoYaml(): Promise<void> {
  if (yamlConfigured) return
  yamlConfigured = true

  try {
    const { configureMonacoYaml } = await import('monaco-yaml')

    const kinds = Object.keys(SCHEMA_FILE_MATCH) as SchemaKind[]
    const settled = await Promise.allSettled(kinds.map((kind) => fetchSchema(kind)))

    const schemas = settled.flatMap((result, i) => {
      const kind = kinds[i]
      if (result.status === 'rejected') {
        console.warn(
          `monaco-yaml: failed to fetch '${kind}' schema; YAML validation disabled for ${SCHEMA_FILE_MATCH[kind].join(', ')}`,
          result.reason,
        )
        return []
      }
      return [
        {
          // `uri` only identifies the schema for hover sources; it does not
          // trigger a network request because the schema is supplied inline.
          uri: `lhp://schemas/${kind}.schema.json`,
          fileMatch: SCHEMA_FILE_MATCH[kind],
          // The backend serves the raw packaged JSON Schema document, so the
          // fetched value IS the schema (not a wrapper around it).
          schema: result.value,
        },
      ]
    })

    if (schemas.length === 0) {
      console.warn('monaco-yaml: no schemas available; YAML validation disabled')
    }

    configureMonacoYaml(monaco, {
      enableSchemaRequest: false,
      schemas,
    })
  } catch (err) {
    // Reset so a later mount can retry, and degrade gracefully.
    yamlConfigured = false
    console.warn('monaco-yaml: configuration failed; YAML validation disabled', err)
  }
}
