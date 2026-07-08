import { forwardRef, useImperativeHandle, useRef } from 'react'
import { DiffEditor } from '@monaco-editor/react'
import type { DiffOnMount } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { monacoThemeFor } from '../../lib/monaco-setup'
import { useThemeStore } from '../../store/themeStore'

export interface DiffEditorHandle {
  /** Current text of the editable (right/"mine") side. */
  getModifiedValue: () => string
}

interface Props {
  /** Read-only left side (the on-disk version). */
  original: string
  /** Editable right side (the user's version — merge target). */
  modified: string
  language: string
}

/** Theme-aware Monaco diff editor used by the conflict-resolution dialog:
 * original (disk) is read-only, modified (mine) is editable so the user can
 * merge manually before re-saving. */
const DiffEditorWrapper = forwardRef<DiffEditorHandle, Props>(function DiffEditorWrapper(
  { original, modified, language },
  ref,
) {
  const diffRef = useRef<editor.IStandaloneDiffEditor | null>(null)
  // Subscribing keeps the diff live across a theme flip (same pattern as
  // MonacoEditorWrapper).
  const resolvedTheme = useThemeStore((s) => s.resolved)

  useImperativeHandle(ref, () => ({
    getModifiedValue: () =>
      diffRef.current?.getModifiedEditor().getValue() ?? modified,
  }))

  const handleMount: DiffOnMount = (ed) => {
    diffRef.current = ed
  }

  return (
    <DiffEditor
      original={original}
      modified={modified}
      language={language}
      theme={monacoThemeFor(resolvedTheme)}
      loading={null}
      onMount={handleMount}
      options={{
        originalEditable: false,
        readOnly: false,
        renderSideBySide: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
        wordWrap: 'on',
        fontSize: 13,
        lineNumbers: 'on',
      }}
    />
  )
})

export default DiffEditorWrapper
