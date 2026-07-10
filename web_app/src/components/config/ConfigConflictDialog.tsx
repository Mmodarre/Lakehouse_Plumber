import { CloudDownload, CloudUpload, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '@/components/ui/dialog'

// ── ConfigConflictDialog — 412 stale-save resolution ─────────
//
// Config-form sibling of the workspace editor's ConflictDialog (same
// wording and option layout), deliberately NOT a reuse of that component:
// the workspace dialog owns raw text buffers, fetches the disk version
// itself, and offers a Monaco merge view — none of which applies to a
// form editing a parsed handle. Here both resolutions are single
// useConfigFile actions:
//   • Reload from disk — discard my form changes (useConfigFile.reload);
//   • Overwrite — re-save my version with a freshly fetched etag
//     (useConfigFile.overwrite; a further concurrent change still 412s).

export interface ConfigConflictDialogProps {
  /** Path in conflict; `null` renders nothing (dialog closed). */
  path: string | null
  /** A resolution is in flight — disables both options. */
  saving: boolean
  /** Discard form changes and adopt the disk version. */
  onReload: () => void
  /** Save my version again over the disk version. */
  onOverwrite: () => void
  /** Dismiss without resolving (Save stays rejected until resolved). */
  onCancel: () => void
}

export function ConfigConflictDialog({
  path,
  saving,
  onReload,
  onOverwrite,
  onCancel,
}: ConfigConflictDialogProps) {
  if (path === null) return null
  const filename = path.split('/').pop() ?? path

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onCancel()
      }}
    >
      <DialogContent showCloseButton className="gap-0 overflow-hidden p-0 sm:max-w-lg">
        <div className="border-b border-border px-5 py-3">
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Conflict
          </span>
          <DialogTitle className="truncate text-sm font-semibold text-foreground">
            {filename} changed on disk
          </DialogTitle>
          <DialogDescription className="mt-0.5 truncate font-mono text-2xs text-muted-foreground">
            {path}
          </DialogDescription>
        </div>

        <p className="px-5 pt-4 text-xs text-muted-foreground">
          This file was modified on disk after you opened it. Your save was rejected to avoid
          overwriting those changes.
        </p>
        <div className="space-y-2 px-5 py-4">
          <Button
            variant="outline"
            size="sm"
            className="w-full justify-start"
            disabled={saving}
            onClick={onReload}
          >
            <CloudDownload aria-hidden="true" />
            Reload from disk
            <span className="ml-auto text-2xs font-normal text-muted-foreground">
              discard my form changes
            </span>
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="w-full justify-start"
            disabled={saving}
            onClick={onOverwrite}
          >
            {saving ? <Loader2 className="animate-spin" aria-hidden="true" /> : <CloudUpload aria-hidden="true" />}
            Overwrite
            <span className="ml-auto text-2xs font-normal text-muted-foreground">
              save my version again
            </span>
          </Button>
        </div>
        <div className="flex items-center justify-end border-t border-border px-5 py-3">
          <Button variant="ghost" size="sm" onClick={onCancel}>
            Cancel
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
