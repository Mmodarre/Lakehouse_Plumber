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

self.MonacoEnvironment = {
  getWorker(_workerId: string, label: string) {
    if (label === 'json') return new jsonWorker()
    return new editorWorker()
  },
}

// Use locally-bundled Monaco instead of CDN
loader.config({ monaco })
