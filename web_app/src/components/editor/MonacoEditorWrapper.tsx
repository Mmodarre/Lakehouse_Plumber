import { useRef, useImperativeHandle, forwardRef } from 'react'
import Editor from '@monaco-editor/react'
import type { OnMount, Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { setupMonacoYaml } from '../../lib/monaco-setup'

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
}

const MonacoEditorWrapper = forwardRef<MonacoEditorHandle, Props>(function MonacoEditorWrapper(
  { path, content, readOnly = false, onDirtyChange, onSave },
  ref,
) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)

  useImperativeHandle(ref, () => ({
    getValue: () => editorRef.current?.getValue() ?? '',
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

    // Signal dirty on any content change
    ed.onDidChangeModelContent(() => {
      onDirtyChange?.(true)
    })

    // Capture Ctrl+S / Cmd+S — prevents browser save dialog
    ed.addCommand(monacoInstance.KeyMod.CtrlCmd | monacoInstance.KeyCode.KeyS, () => {
      onSave?.()
    })
  }

  return (
    <Editor
      defaultValue={content}
      path={path}
      language={getLanguage(path)}
      theme="vs-dark"
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
