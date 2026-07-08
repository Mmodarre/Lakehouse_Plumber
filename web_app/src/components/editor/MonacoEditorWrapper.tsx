import { useRef, useImperativeHandle, forwardRef } from 'react'
import Editor from '@monaco-editor/react'
import type { OnMount, Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { monacoThemeFor, setupMonacoYaml } from '../../lib/monaco-setup'
import { useThemeStore } from '../../store/themeStore'

/** Marker owner used for our YAML syntax-error markers. Kept distinct from
 * monaco-yaml's own schema markers so we only ever clear what we set. */
const YAML_MARKER_OWNER = 'lhp-yaml'

const EXT_TO_LANGUAGE: Record<string, string> = {
  yaml: 'yaml',
  yml: 'yaml',
  py: 'python',
  sql: 'sql',
  json: 'json',
  md: 'markdown',
  txt: 'plaintext',
  ddl: 'sql',
  cfg: 'ini',
  ini: 'ini',
  toml: 'ini',
  sh: 'shell',
  bash: 'shell',
}

function getLanguage(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  return EXT_TO_LANGUAGE[ext] ?? 'plaintext'
}

/** A single YAML syntax-error location (1-based, Monaco-compatible). */
export interface YamlSyntaxMarker {
  line: number
  column: number
  message: string
}

export interface MonacoEditorHandle {
  getValue: () => string
  /** Programmatically replace the editor buffer (e.g. reloading from disk).
   * Resets the dirty signal to `false` since this is not a user edit. No-op if
   * the editor is not mounted. */
  setValue: (value: string) => void
  /** Set our `lhp-yaml` error markers on the current model. Replaces any
   * previously-set markers of the same owner. No-op if the editor is not
   * mounted. */
  setYamlMarkers: (markers: YamlSyntaxMarker[]) => void
  /** Clear all `lhp-yaml` error markers from the current model. */
  clearYamlMarkers: () => void
}

interface Props {
  path: string
  content: string
  readOnly?: boolean
  onDirtyChange?: (dirty: boolean) => void
  onSave?: () => void
  /** Called once the Monaco editor instance is mounted (after the imperative
   * handle is usable) — e.g. to re-apply markers on a fresh model. */
  onEditorMount?: () => void
}

const MonacoEditorWrapper = forwardRef<MonacoEditorHandle, Props>(function MonacoEditorWrapper(
  { path, content, readOnly = false, onDirtyChange, onSave, onEditorMount },
  ref,
) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)
  // True while a programmatic setValue is in flight. Monaco fires
  // onDidChangeModelContent SYNCHRONOUSLY from setValue, which would
  // otherwise surface as a spurious user-edit dirty signal.
  const suppressChangeRef = useRef(false)
  // Subscribing keeps open editors live: a theme flip re-renders with the
  // other `lhp-*` theme name and @monaco-editor/react calls setTheme for us.
  const resolvedTheme = useThemeStore((s) => s.resolved)

  useImperativeHandle(ref, () => ({
    getValue: () => editorRef.current?.getValue() ?? '',
    setValue: (value: string) => {
      const ed = editorRef.current
      if (!ed) return
      // Suppress the synchronous change event for the duration of the call —
      // this is a programmatic reload, not a user edit — then signal clean so
      // the host can cancel any pending capture of the replaced text.
      suppressChangeRef.current = true
      try {
        ed.setValue(value)
      } finally {
        suppressChangeRef.current = false
      }
      onDirtyChange?.(false)
    },
    setYamlMarkers: (markers: YamlSyntaxMarker[]) => {
      const monacoInstance = monacoRef.current
      const model = editorRef.current?.getModel()
      if (!monacoInstance || !model) return
      monacoInstance.editor.setModelMarkers(
        model,
        YAML_MARKER_OWNER,
        markers.map((m) => ({
          severity: monacoInstance.MarkerSeverity.Error,
          message: m.message,
          // Backend sends 1-based line/column; Monaco markers are 1-based.
          // Highlight from the reported position to end-of-line so the
          // squiggle is visible even when only a point is known.
          startLineNumber: m.line,
          startColumn: m.column,
          endLineNumber: m.line,
          endColumn: m.column + 1,
        })),
      )
    },
    clearYamlMarkers: () => {
      const monacoInstance = monacoRef.current
      const model = editorRef.current?.getModel()
      if (!monacoInstance || !model) return
      monacoInstance.editor.setModelMarkers(model, YAML_MARKER_OWNER, [])
    },
  }))

  const handleMount: OnMount = (ed, monacoInstance) => {
    editorRef.current = ed
    monacoRef.current = monacoInstance
    ed.focus()

    // Lazily wire monaco-yaml (schema validation) on first editor mount.
    // Idempotent and self-guarding; failures degrade gracefully.
    void setupMonacoYaml()

    // Signal dirty on any content change — except programmatic setValue
    // (see suppressChangeRef), which is a reload rather than a user edit.
    ed.onDidChangeModelContent(() => {
      if (suppressChangeRef.current) return
      onDirtyChange?.(true)
    })

    // Capture Ctrl+S / Cmd+S — prevents browser save dialog
    ed.addCommand(monacoInstance.KeyMod.CtrlCmd | monacoInstance.KeyCode.KeyS, () => {
      onSave?.()
    })

    onEditorMount?.()
  }

  return (
    <Editor
      defaultValue={content}
      path={path}
      language={getLanguage(path)}
      theme={monacoThemeFor(resolvedTheme)}
      loading={null}
      onMount={handleMount}
      options={{
        readOnly,
        domReadOnly: readOnly,
        contextmenu: !readOnly,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
        foldingStrategy: 'indentation',
        guides: { indentation: true },
        wordWrap: 'on',
        tabSize: 2,
        fontSize: 13,
        lineNumbers: 'on',
        renderLineHighlight: 'line',
        padding: { top: 12 },
      }}
    />
  )
})

export default MonacoEditorWrapper
