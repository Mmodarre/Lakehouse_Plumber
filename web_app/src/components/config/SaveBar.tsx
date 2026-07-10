import { useCallback } from 'react'
import { FileCode2, Loader2, Save } from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { fetchFileContentWithMeta } from '../../api/files'
import { errorMessage } from '../../lib/errors'
import { useWorkspaceStore } from '../../store/workspaceStore'

// ── SaveBar — the Config forms' pinned footer ────────────────
//
// Rendered into ConfigPageShell's `footer` slot. Owns no file state: the
// page wires it to useConfigFile (`dirty`/`saving`/`save`) and the form
// supplies `errorCount` (blocking config-model issues; Save is disabled
// while any exist). "Open raw YAML" opens the same file as a workspace
// editor buffer — the escape hatch for anything the form cannot express.

export interface SaveBarProps {
  /** Project-relative file this bar saves; null disables both actions. */
  path: string | null
  /** Unsaved form edits exist (useConfigFile.dirty). */
  dirty: boolean
  /** A save is in flight (useConfigFile.saving). */
  saving: boolean
  /** Blocking validation errors; Save is disabled while > 0. */
  errorCount: number
  /** Trigger the save (useConfigFile.save). */
  onSave: () => void
}

export function SaveBar({ path, dirty, saving, errorCount, onSave }: SaveBarProps) {
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const setActiveBuffer = useWorkspaceStore((s) => s.setActive)

  // Mirrors the file-browser open flow: focus the buffer when it is
  // already open, otherwise fetch content + etag and seed a new buffer.
  const openRawYaml = useCallback(async () => {
    if (path === null) return
    if (useWorkspaceStore.getState().buffers.some((b) => b.path === path)) {
      setActiveBuffer(path)
      return
    }
    try {
      const { content, etag } = await fetchFileContentWithMeta(path)
      openBuffer(path, { content, etag, exists: true })
    } catch (err) {
      toast.error(errorMessage(err, 'Failed to open the file'))
    }
  }, [path, openBuffer, setActiveBuffer])

  return (
    <div className="flex shrink-0 items-center gap-2 border-t border-border bg-card px-6 py-2.5">
      {dirty && (
        <Badge variant="secondary" className="rounded-sm px-1.5 text-2xs">
          Unsaved changes
        </Badge>
      )}
      {errorCount > 0 && (
        <Badge variant="destructive" className="rounded-sm px-1.5 text-2xs">
          {errorCount} {errorCount === 1 ? 'error' : 'errors'}
        </Badge>
      )}
      <div className="ml-auto flex items-center gap-2">
        <Button
          type="button"
          variant="ghost"
          size="sm"
          disabled={path === null}
          onClick={() => void openRawYaml()}
          title={path ? `Open ${path} in the editor` : undefined}
        >
          <FileCode2 aria-hidden="true" />
          Open raw YAML
        </Button>
        <Button
          type="button"
          size="sm"
          disabled={!dirty || errorCount > 0 || saving}
          onClick={onSave}
        >
          {saving ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <Save aria-hidden="true" />
          )}
          Save
        </Button>
      </div>
    </div>
  )
}
