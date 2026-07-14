import { lazy, Suspense } from 'react'
import type { RefObject } from 'react'
import { Loader2, Lock } from 'lucide-react'
import type { EditorBuffer } from '../../../store/workspaceStore'
import { Badge } from '../../ui/badge'
import { Button } from '../../ui/button'
import type { MonacoEditorHandle } from '../../editor/MonacoEditorWrapper'

// ── YamlView — the single-Monaco editor body (§6.4) ──────────
//
// Recomposed from components/workspace/WorkspaceEditor.tsx's editor body: the
// SINGLE mounted Monaco keyed by the active buffer path (model disposed on
// switch), plus the read-only / load-failed / not-yet-created banners and a
// compact save bar. All editor machinery (content capture, ⌘S save via
// useWorkspaceSave, conflict + yaml_error wiring, restore prompt) lives in the
// parent CenterArea and reaches Monaco through `editorRef` — this view is pure
// presentation over one buffer.

const MonacoEditorWrapper = lazy(() => import('../../editor/MonacoEditorWrapper'))

function EditorSkeleton() {
  return (
    <div className="flex flex-1 items-center justify-center bg-card">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-border border-t-muted-foreground" />
    </div>
  )
}

interface YamlViewProps {
  buffer: EditorBuffer
  editorRef: RefObject<MonacoEditorHandle | null>
  isReadOnly: boolean
  /** Number of dirty buffers across the workspace (drives "Save All"). */
  dirtyCount: number
  anySaving: boolean
  onDirtyChange: (dirty: boolean) => void
  onSave: () => void
  onSaveAll: () => void
  onEditorMount: () => void
  onRetryLoad: (path: string) => void
}

export function YamlView({
  buffer,
  editorRef,
  isReadOnly,
  dirtyCount,
  anySaving,
  onDirtyChange,
  onSave,
  onSaveAll,
  onEditorMount,
  onRetryLoad,
}: YamlViewProps) {
  const loadFailed = buffer.loadFailed

  return (
    <div className="flex h-full min-h-0 flex-col bg-card">
      {/* Compact save bar (relocated from the old editor tab-strip row). */}
      <div className="flex items-center justify-end gap-2 border-b border-border bg-sidebar px-3 py-1">
        {isReadOnly ? (
          <Badge
            variant="outline"
            className="h-5 rounded-sm px-1.5 text-2xs text-muted-foreground"
          >
            <Lock className="size-2.5" aria-hidden="true" />
            Read Only
          </Badge>
        ) : (
          <Button
            size="xs"
            onClick={onSave}
            disabled={!buffer.isDirty || buffer.isSaving || loadFailed}
          >
            {buffer.isSaving && <Loader2 className="animate-spin" aria-hidden="true" />}
            {buffer.isSaving ? 'Saving...' : 'Save'}
          </Button>
        )}
        {dirtyCount > 1 && (
          <Button variant="outline" size="xs" onClick={onSaveAll} disabled={anySaving}>
            Save All ({dirtyCount})
          </Button>
        )}
      </div>

      {loadFailed && (
        <div className="flex items-center gap-3 border-b border-border bg-muted px-4 py-2 text-xs">
          <span className="text-destructive">
            This file failed to load — editing is disabled so an empty buffer can't overwrite
            it.
          </span>
          <Button variant="outline" size="xs" onClick={() => onRetryLoad(buffer.path)}>
            Retry
          </Button>
        </div>
      )}
      {!buffer.exists && !buffer.loading && !loadFailed && (
        <div className="border-b border-border bg-muted px-4 py-2 text-xs text-muted-foreground">
          This file doesn't exist yet. Save to create it.
        </div>
      )}

      {!buffer.loading ? (
        <div className="min-h-0 flex-1">
          <Suspense fallback={<EditorSkeleton />}>
            <MonacoEditorWrapper
              key={buffer.path}
              ref={editorRef}
              path={buffer.path}
              content={buffer.content}
              readOnly={isReadOnly || loadFailed}
              onDirtyChange={onDirtyChange}
              onSave={onSave}
              onEditorMount={onEditorMount}
            />
          </Suspense>
        </div>
      ) : (
        <EditorSkeleton />
      )}
    </div>
  )
}
