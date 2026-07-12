import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ExternalLink, Loader2 } from 'lucide-react'
import { toast } from 'sonner'
import { useQueryClient } from '@tanstack/react-query'
import { ApiError } from '@/api/client'
import { fetchFileContentWithMeta, writeFile } from '@/api/files'
import { errorMessage } from '@/lib/errors'
import { deleteActionField, setActionField } from '@/lib/flowgroup-doc'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '@/components/ui/dialog'
import type { MonacoEditorHandle } from '../editor/MonacoEditorWrapper'
import type { CodeLanguage } from './codeFields'
import type { DesignerMutator } from './useDesignerWrite'

const MonacoEditorWrapper = lazy(() => import('../editor/MonacoEditorWrapper'))

// ── CodeModal — Monaco editor for SQL/Python/expectations ────
//
// Reuses the editor-modal pattern (shadcn Dialog + lazy Monaco). Two backings:
//   • inline — the value is code embedded in the YAML; Save writes it back
//     through `commit` (the same write-through path as any field edit), so an
//     empty body deletes the key (delete-on-clear).
//   • file — the value is a project-relative path; the modal loads its content
//     and Save PUTs it via the files API with If-Match. "Open as file tab"
//     hands the file to the docked workspace editor instead.
//
// Both writes respect the enforcing dirty-buffer guard: when `readOnly`, the
// editor is read-only and Save is hidden (validation may still run, but the
// designer never mutates while the flowgroup file has unsaved text edits).

export type CodeTarget =
  | {
      backing: 'inline'
      /** Canvas node id (mutator address) + field path for the write-back. */
      actionId: string
      path: YamlPath
      title: string
      language: CodeLanguage
      initialValue: string
    }
  | { backing: 'file'; title: string; filePath: string }

interface CodeModalProps {
  target: CodeTarget | null
  /** Editing is blocked (dirty text buffer / unresolved conflict). */
  readOnly: boolean
  /** Inline write-through (no-op when read-only; see useDesignerWrite). */
  commit: (mutator: DesignerMutator) => void
  /** Open a file ref as a docked workspace buffer, then close the modal. */
  openAsFile: (path: string) => void
  onClose: () => void
}

/** Unique-per-open synthetic model path: keeps the modal's Monaco model
 * isolated from any docked editor of the same file (no shared-model binding). */
let MODEL_SEQ = 0

function basename(path: string): string {
  return path.split('/').pop() ?? path
}

function extFor(target: CodeTarget): string {
  if (target.backing === 'file') {
    const ext = basename(target.filePath).split('.').pop()
    return ext !== undefined && ext !== '' ? ext.toLowerCase() : 'txt'
  }
  return target.language === 'python' ? 'py' : 'sql'
}

export function CodeModal({ target, readOnly, commit, openAsFile, onClose }: CodeModalProps) {
  if (target === null) return null
  const key =
    target.backing === 'file'
      ? `file:${target.filePath}`
      : `inline:${target.actionId}:${target.path.join('.')}`
  return (
    <CodeModalBody
      key={key}
      target={target}
      readOnly={readOnly}
      commit={commit}
      openAsFile={openAsFile}
      onClose={onClose}
    />
  )
}

interface CodeModalBodyProps extends Omit<CodeModalProps, 'target'> {
  target: CodeTarget
}

function CodeModalBody({ target, readOnly, commit, openAsFile, onClose }: CodeModalBodyProps) {
  const queryClient = useQueryClient()
  const editorRef = useRef<MonacoEditorHandle>(null)
  const [dirty, setDirty] = useState(false)
  const [saving, setSaving] = useState(false)

  const isFile = target.backing === 'file'
  const ext = extFor(target)
  const editorPath = useMemo(() => `.lhp-designer/buffer-${(MODEL_SEQ += 1)}.${ext}`, [ext])

  // Inline: the value is in hand. File: fetch content + fresh etag once.
  const [initial, setInitial] = useState<string | null>(
    target.backing === 'inline' ? target.initialValue : null,
  )
  const [etag, setEtag] = useState<string | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)

  useEffect(() => {
    if (target.backing !== 'file') return
    let cancelled = false
    fetchFileContentWithMeta(target.filePath)
      .then((res) => {
        if (cancelled) return
        setInitial(res.content)
        setEtag(res.etag)
      })
      .catch((err: unknown) => {
        if (!cancelled) setLoadError(errorMessage(err, 'Failed to load the file'))
      })
    return () => {
      cancelled = true
    }
  }, [target])

  const handleOpenChange = useCallback(
    (open: boolean) => {
      if (!open) onClose()
    },
    [onClose],
  )

  const handleSave = useCallback(async () => {
    if (readOnly) return
    const value = editorRef.current?.getValue() ?? ''

    if (target.backing === 'inline') {
      const { actionId, path } = target
      commit((doc) => {
        if (value.trim() === '') deleteActionField(doc, actionId, path)
        else setActionField(doc, actionId, path, value)
      })
      onClose()
      return
    }

    setSaving(true)
    try {
      const res = await writeFile(target.filePath, value, etag)
      queryClient.setQueryData(['file-content', target.filePath], {
        content: value,
        etag: res.etag ?? null,
      })
      toast.success(`Saved ${basename(target.filePath)}`)
      onClose()
    } catch (err) {
      if (err instanceof ApiError && err.status === 412) {
        toast.error('This file changed on disk. Close and reopen it to edit the latest version.')
      } else {
        toast.error(errorMessage(err, 'Failed to save the file'))
      }
    } finally {
      setSaving(false)
    }
  }, [readOnly, target, commit, onClose, etag, queryClient])

  const langLabel = ext.toUpperCase()

  return (
    <Dialog open onOpenChange={handleOpenChange}>
      <DialogContent
        showCloseButton
        className="flex h-[85vh] flex-col gap-0 overflow-hidden bg-card p-0 sm:max-w-4xl"
      >
        <div className="border-b border-border px-5 py-3">
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            {isFile ? 'Edit file' : 'Edit inline code'}
          </span>
          <DialogTitle className="flex items-center gap-2 truncate text-sm font-semibold text-foreground">
            {target.title}
            <span className="rounded-sm border border-border px-1 font-mono text-2xs font-normal text-muted-foreground">
              {langLabel}
            </span>
          </DialogTitle>
          <DialogDescription className="mt-0.5 truncate font-mono text-2xs text-muted-foreground">
            {isFile && target.backing === 'file'
              ? target.filePath
              : 'Saved into the flowgroup YAML'}
          </DialogDescription>
        </div>

        {loadError !== null ? (
          <div className="px-5 py-4 text-xs text-destructive">{loadError}</div>
        ) : initial === null ? (
          <div className="flex items-center gap-2 px-5 py-6 text-xs text-muted-foreground">
            <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />
            Loading the file...
          </div>
        ) : (
          <div className="min-h-0 flex-1 bg-card">
            <Suspense
              fallback={
                <div className="flex h-full items-center justify-center">
                  <Loader2 className="size-5 animate-spin text-muted-foreground" aria-hidden="true" />
                </div>
              }
            >
              <MonacoEditorWrapper
                ref={editorRef}
                path={editorPath}
                content={initial}
                readOnly={readOnly}
                onDirtyChange={setDirty}
                onSave={() => void handleSave()}
              />
            </Suspense>
          </div>
        )}

        <div className="flex items-center gap-2 border-t border-border px-5 py-3">
          {isFile && target.backing === 'file' && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                openAsFile(target.filePath)
                onClose()
              }}
            >
              <ExternalLink aria-hidden="true" />
              Open as file tab
            </Button>
          )}
          <div className="flex-1" />
          {readOnly ? (
            <span className="text-2xs text-muted-foreground">
              Editing is disabled while this file has unsaved edits in the text editor.
            </span>
          ) : (
            <>
              <Button variant="ghost" size="sm" onClick={onClose}>
                Cancel
              </Button>
              <Button
                size="sm"
                onClick={() => void handleSave()}
                disabled={!dirty || saving || initial === null}
              >
                {saving && <Loader2 className="animate-spin" aria-hidden="true" />}
                Save
              </Button>
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
