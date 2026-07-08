import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react'
import { CloudDownload, CloudUpload, GitMerge, Loader2 } from 'lucide-react'
import { fetchFileContentWithMeta } from '../../api/files'
import { errorMessage } from '../../lib/errors'
import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '../ui/dialog'
import type { DiffEditorHandle } from './DiffEditorWrapper'

const DiffEditorWrapper = lazy(() => import('./DiffEditorWrapper'))

// ── ConflictDialog — 412 stale-write resolution ──────────────
//
// Opened from the persistent stale-file toast after a save returns 412.
// Fetches the current on-disk version (content + fresh ETag) and offers:
//   • Keep mine  — re-PUT my content using the FRESH etag (never the stale
//     one, so a further concurrent change still 412s — no double-clobber);
//   • Take disk  — replace my buffer with the disk version;
//   • Merge      — side-by-side Monaco diff (disk read-only, mine editable),
//     then save the merged right side with the fresh etag.

/** One unresolved 412 conflict (per buffer path). */
export interface ConflictInfo {
  path: string
  /** The content whose save was rejected (the user's version). */
  mine: string
  /** Monaco language id for the diff view. */
  language: string
}

interface ConflictDialogProps {
  conflict: ConflictInfo | null
  onCancel: () => void
  /** Re-save `content` with the freshly fetched on-disk etag. */
  onKeepMine: (path: string, content: string, freshEtag: string | null) => void
  /** Replace the buffer with the on-disk version. */
  onTakeTheirs: (path: string, content: string, etag: string | null) => void
}

interface TheirsState {
  content: string
  etag: string | null
}

export function ConflictDialog({
  conflict,
  onCancel,
  onKeepMine,
  onTakeTheirs,
}: ConflictDialogProps) {
  if (!conflict) return null
  // Keyed by path so switching to a different conflict remounts the body,
  // resetting its fetched/merge state via the useState initializers.
  return (
    <ConflictDialogBody
      key={conflict.path}
      conflict={conflict}
      onCancel={onCancel}
      onKeepMine={onKeepMine}
      onTakeTheirs={onTakeTheirs}
    />
  )
}

interface ConflictDialogBodyProps extends ConflictDialogProps {
  conflict: ConflictInfo
}

function ConflictDialogBody({
  conflict,
  onCancel,
  onKeepMine,
  onTakeTheirs,
}: ConflictDialogBodyProps) {
  const [theirs, setTheirs] = useState<TheirsState | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [merging, setMerging] = useState(false)
  const diffRef = useRef<DiffEditorHandle>(null)

  const path = conflict.path

  // Fetch the current on-disk version once per conflict. The cancelled guard
  // drops a response that resolves after the dialog unmounted.
  useEffect(() => {
    let cancelled = false
    fetchFileContentWithMeta(path)
      .then((res) => {
        if (!cancelled) setTheirs(res)
      })
      .catch((err: unknown) => {
        if (!cancelled) setLoadError(errorMessage(err, 'Failed to load the on-disk version'))
      })
    return () => {
      cancelled = true
    }
  }, [path])

  const handleOpenChange = useCallback(
    (open: boolean) => {
      if (!open) onCancel()
    },
    [onCancel],
  )

  const filename = conflict.path.split('/').pop() ?? conflict.path

  return (
    <Dialog open onOpenChange={handleOpenChange}>
      <DialogContent
        showCloseButton
        className={
          merging
            ? 'flex h-[85vh] flex-col gap-0 overflow-hidden bg-card p-0 sm:max-w-6xl'
            : 'gap-0 overflow-hidden p-0 sm:max-w-lg'
        }
      >
        <div className="border-b border-border px-5 py-3">
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Conflict
          </span>
          <DialogTitle className="truncate text-sm font-semibold text-foreground">
            {filename} changed on disk
          </DialogTitle>
          <DialogDescription className="mt-0.5 truncate font-mono text-2xs text-muted-foreground">
            {conflict.path}
          </DialogDescription>
        </div>

        {loadError ? (
          <div className="px-5 py-4 text-xs text-destructive">{loadError}</div>
        ) : !theirs ? (
          <div className="flex items-center gap-2 px-5 py-6 text-xs text-muted-foreground">
            <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />
            Loading the on-disk version...
          </div>
        ) : merging ? (
          <>
            <div className="grid grid-cols-2 border-b border-border text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
              <span className="px-5 py-1.5">On disk</span>
              <span className="px-5 py-1.5">Mine (editable)</span>
            </div>
            <div className="min-h-0 flex-1 bg-card">
              <Suspense
                fallback={
                  <div className="flex h-full items-center justify-center">
                    <Loader2 className="size-5 animate-spin text-muted-foreground" aria-hidden="true" />
                  </div>
                }
              >
                <DiffEditorWrapper
                  ref={diffRef}
                  original={theirs.content}
                  modified={conflict.mine}
                  language={conflict.language}
                />
              </Suspense>
            </div>
            <div className="flex items-center justify-end gap-2 border-t border-border px-5 py-3">
              <Button variant="ghost" size="sm" onClick={() => setMerging(false)}>
                Back
              </Button>
              <Button
                size="sm"
                onClick={() =>
                  onKeepMine(
                    conflict.path,
                    diffRef.current?.getModifiedValue() ?? conflict.mine,
                    theirs.etag,
                  )
                }
              >
                Save merged
              </Button>
            </div>
          </>
        ) : (
          <>
            <p className="px-5 pt-4 text-xs text-muted-foreground">
              This file was modified on disk after you opened it. Your save was
              rejected to avoid overwriting those changes.
            </p>
            <div className="space-y-2 px-5 py-4">
              <Button
                variant="outline"
                size="sm"
                className="w-full justify-start"
                onClick={() => onKeepMine(conflict.path, conflict.mine, theirs.etag)}
              >
                <CloudUpload aria-hidden="true" />
                Keep my version
                <span className="ml-auto text-2xs font-normal text-muted-foreground">
                  overwrite the disk copy
                </span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="w-full justify-start"
                onClick={() => onTakeTheirs(conflict.path, theirs.content, theirs.etag)}
              >
                <CloudDownload aria-hidden="true" />
                Take disk version
                <span className="ml-auto text-2xs font-normal text-muted-foreground">
                  discard my changes
                </span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="w-full justify-start"
                onClick={() => setMerging(true)}
              >
                <GitMerge aria-hidden="true" />
                Merge manually
                <span className="ml-auto text-2xs font-normal text-muted-foreground">
                  side-by-side diff
                </span>
              </Button>
            </div>
            <div className="flex items-center justify-end border-t border-border px-5 py-3">
              <Button variant="ghost" size="sm" onClick={onCancel}>
                Cancel
              </Button>
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  )
}
