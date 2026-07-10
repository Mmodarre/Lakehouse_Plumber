import { FileWarning } from 'lucide-react'
import { Button } from '@/components/ui/button'

// ── ExternalChangeBanner — file changed on disk while dirty ──
//
// Non-modal notice rendered into ConfigPageShell's `banner` slot when
// useConfigFile.externalChange is set (SSE reported a disk change while
// unsaved form edits exist). "Reload" adopts the disk version and
// discards local edits; "Keep my changes" dismisses the banner — a later
// Save will then hit 412 and open the ConfigConflictDialog.

export interface ExternalChangeBannerProps {
  /** Adopt the on-disk version, discarding local edits (useConfigFile.reload). */
  onReload: () => void
  /** Dismiss and keep editing (useConfigFile.keepMine). */
  onKeep: () => void
}

export function ExternalChangeBanner({ onReload, onKeep }: ExternalChangeBannerProps) {
  return (
    <div
      role="status"
      className="flex flex-wrap items-center gap-2 border-b border-warning/30 bg-warning/10 px-6 py-2"
    >
      <FileWarning className="size-3.5 shrink-0 text-warning" aria-hidden="true" />
      <span className="text-xs text-foreground">This file changed on disk.</span>
      <div className="ml-auto flex items-center gap-2">
        <Button type="button" variant="outline" size="xs" onClick={onReload}>
          Reload
        </Button>
        <Button type="button" variant="ghost" size="xs" onClick={onKeep}>
          Keep my changes
        </Button>
      </div>
    </div>
  )
}
