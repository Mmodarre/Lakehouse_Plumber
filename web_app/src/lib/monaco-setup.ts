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

import { loadSchemaCached, type SchemaKind } from '../api/schemas'

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

// ── Shell-matching editor themes (Lakehouse palette — TOKEN MAP) ───────────
//
// `editor.background` is the exact hex of the app's `--card` token
// (light `#FFFFFF` / dark `#22262E`), so the editor surface sits
// seamlessly inside a `bg-card` panel in both themes. The accent (= `--primary`,
// light `#E8552F` / dark `#FF6F4E`) drives the cursor and selection tints.
// Token colors mirror the `--syntax-*` rows (YAML keys/strings/nums/comments and
// SQL keywords), so highlighting reads as one system with the shell.

const LIGHT_PRIMARY = '#E8552F'
const DARK_PRIMARY = '#FF6F4E'

const LIGHT_TOKEN_RULES = [
  { token: 'comment', foreground: '8A857C', fontStyle: 'italic' },
  { token: 'type', foreground: '1F5FA8' }, // YAML keys
  { token: 'tag', foreground: '1F5FA8' },
  { token: 'string', foreground: '2E7D46' },
  { token: 'number', foreground: 'B4630F' },
  { token: 'keyword', foreground: 'B4630F' }, // YAML true/false/null
  { token: 'delimiter', foreground: '6B675F' },
  { token: 'operators', foreground: '6B675F' },
  { token: 'keyword.sql', foreground: '6D28D9' },
  { token: 'operator.sql', foreground: '6B675F' },
  { token: 'string.sql', foreground: '2E7D46' },
  { token: 'number.sql', foreground: 'B4630F' },
  { token: 'comment.sql', foreground: '8A857C', fontStyle: 'italic' },
]

const DARK_TOKEN_RULES = [
  { token: 'comment', foreground: '6E7681', fontStyle: 'italic' },
  { token: 'type', foreground: '8AB4F8' }, // YAML keys
  { token: 'tag', foreground: '8AB4F8' },
  { token: 'string', foreground: '9ECE8A' },
  { token: 'number', foreground: 'E0A66B' },
  { token: 'keyword', foreground: 'E0A66B' }, // YAML true/false/null
  { token: 'delimiter', foreground: '9A968E' },
  { token: 'operators', foreground: '9A968E' },
  { token: 'keyword.sql', foreground: 'B99BF0' },
  { token: 'operator.sql', foreground: '9A968E' },
  { token: 'string.sql', foreground: '9ECE8A' },
  { token: 'number.sql', foreground: 'E0A66B' },
  { token: 'comment.sql', foreground: '6E7681', fontStyle: 'italic' },
]

monaco.editor.defineTheme('lhp-light', {
  base: 'vs',
  inherit: true,
  rules: LIGHT_TOKEN_RULES,
  colors: {
    'editor.background': '#FFFFFF',
    'editor.foreground': '#1A1B1E',
    'editorLineNumber.foreground': '#94908A',
    'editorLineNumber.activeForeground': '#6B675F',
    'editorCursor.foreground': LIGHT_PRIMARY,
    'editor.selectionBackground': `${LIGHT_PRIMARY}2e`,
    'editor.inactiveSelectionBackground': `${LIGHT_PRIMARY}14`,
    'editor.selectionHighlightBackground': `${LIGHT_PRIMARY}1f`,
  },
})

monaco.editor.defineTheme('lhp-dark', {
  base: 'vs-dark',
  inherit: true,
  rules: DARK_TOKEN_RULES,
  colors: {
    'editor.background': '#22262E',
    'editor.foreground': '#ECEAE6',
    'editorLineNumber.foreground': '#7C786F',
    'editorLineNumber.activeForeground': '#9A968E',
    'editorCursor.foreground': DARK_PRIMARY,
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
 *   - `config/` pipeline/job configs are name-prefixed files directly under
 *     `config/` (e.g. `config/pipeline_config_dev.yaml`), hence the `*` after
 *     the prefix. `monitoring_job_config*` files are job-config-shaped, so
 *     they share the `job_config` schema (and cannot collide with the
 *     `config/job_config*` patterns — the `config/` segment anchors the prefix).
 */
const SCHEMA_FILE_MATCH: Record<SchemaKind, string[]> = {
  flowgroup: ['pipelines/**/*.yaml', 'pipelines/**/*.yml'],
  preset: ['presets/*.yaml', 'presets/*.yml'],
  template: ['templates/**/*.yaml', 'templates/**/*.yml'],
  substitution: ['substitutions/*.yaml', 'substitutions/*.yml'],
  project: ['lhp.yaml', 'lhp.yml'],
  pipeline_config: ['config/pipeline_config*.yaml', 'config/pipeline_config*.yml'],
  job_config: [
    'config/job_config*.yaml',
    'config/job_config*.yml',
    'config/monitoring_job_config*.yaml',
    'config/monitoring_job_config*.yml',
  ],
  tags_file: ['uc_tags/**/*.yaml', 'uc_tags/**/*.yml'],
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
    const settled = await Promise.allSettled(kinds.map((kind) => loadSchemaCached(kind)))

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
