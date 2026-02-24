import { useRef, useImperativeHandle, forwardRef } from 'react'
import Editor from '@monaco-editor/react'
import type { OnMount } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'

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

export interface MonacoEditorHandle {
  getValue: () => string
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

  useImperativeHandle(ref, () => ({
    getValue: () => editorRef.current?.getValue() ?? '',
  }))

  const handleMount: OnMount = (ed, monacoInstance) => {
    editorRef.current = ed
    ed.focus()

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
