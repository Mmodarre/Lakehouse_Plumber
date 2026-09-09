import { focusInvalidWorkspaceDraft } from '@/workspace/editorCommands'
import { markTemplatePreviewsStale } from '@/store/templatePreviewStore'
import { useCallback, useState } from 'react'
import type { RefObject } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { writeFile, IF_MATCH_CREATE_ONLY } from '../../api/files'
import { ApiError } from '../../api/client'
import { useRunController, useRunStore } from '../../store/runStore'
import { isReadOnlyPath, useWorkspaceStore } from '../../store/workspaceStore'
import { captureWorkspaceEditors } from '../../workspace/editorCommands'
import { useLayoutStore } from '../../store/layoutStore'
import { useDocumentStore } from '../../store/documentStore'
import {
  isYamlPath,
  derivePipelineFromYaml,
  startScopedValidate,
} from '../editor/yamlSaveSupport'
import type { MonacoEditorHandle } from '../editor/MonacoEditorWrapper'
import type { ConflictInfo } from '../editor/ConflictDialog'

// ── useWorkspaceSave — the buffer save pipeline ──────────────
//
// Carries every load-bearing save behaviour of the retired editor modals:
//   • ETag optimistic concurrency (If-Match from the buffer, refreshed from
//     every successful response);
//   • 412 stale-write → persistent per-path toast (id `stale:${path}`) whose
//     action opens the ConflictDialog; dismissed on any successful save or
//     reload of that path;
//   • YAML save side-effects: yaml_error → Monaco markers (active buffer
//     only) + synthetic Problems issue; clean → clear both, derive the
//     pipeline and start a scoped validate run;
//   • query invalidations (['files'] always; flowgroup-topology keys when a
//     new YAML file is first created);
//   • read-only path guard.

interface SaveOverrides {
  /** Save this text instead of the buffer content (conflict keep-mine/merge). */
  validate?: boolean
  contentOverride?: string
  /** Use this etag instead of the buffer's (fresh on-disk etag from the
   * ConflictDialog — never the stale one, so no double-clobber). */
  etagOverride?: string | null
}

interface UseWorkspaceSaveArgs {
  editorRef: RefObject<MonacoEditorHandle | null>
  /** Mirror of the currently-mounted buffer path (kept by WorkspaceEditor). */
  activePathRef: RefObject<string | null>
  /** Sync the live editor's text into the store (called before saves). */
  captureActive: () => void
  activeDocumentPathRef?: RefObject<string | null>
}

export interface WorkspaceSaveApi {
  saveBuffer: (path: string, overrides?: SaveOverrides) => Promise<boolean>
  saveActive: () => Promise<boolean>
  saveAllDirty: (options?: { validate?: boolean }) => Promise<boolean>
  conflict: ConflictInfo | null
  cancelConflict: () => void
  resolveKeepMine: (path: string, content: string, freshEtag: string | null) => void
  resolveTakeTheirs: (path: string, content: string, etag: string | null) => void
}

export function useWorkspaceSave({
  editorRef,
  activePathRef,
  captureActive,
  activeDocumentPathRef = activePathRef,
}: UseWorkspaceSaveArgs): WorkspaceSaveApi {
  const queryClient = useQueryClient()
  const runController = useRunController()
  const setSyntheticSyntaxIssue = useRunStore((s) => s.setSyntheticSyntaxIssue)
  const [conflict, setConflict] = useState<ConflictInfo | null>(null)

  const saveBuffer = useCallback(
    async (path: string, overrides?: SaveOverrides): Promise<boolean> => {
      if (focusInvalidWorkspaceDraft()) { toast.error('Correct this value or press Escape to restore it before saving.'); return false }
      const store = useWorkspaceStore.getState()
      const buffer = store.buffers.find((b) => b.path === path)
      if (!buffer || buffer.isSaving) return false
      const filename = path.split('/').pop() ?? path
      if (isReadOnlyPath(path) || useLayoutStore.getState().viewerMode) {
        toast.error('This file is read-only')
        return false
      }
      if (buffer.loading || buffer.loadFailed) {
        // The buffer never received its on-disk content (fetch pending or
        // failed) — writing it would clobber the real file with ''.
        toast.error(`${filename} has not loaded — retry loading before saving`)
        return false
      }

      const content = overrides?.contentOverride ?? buffer.content
      const etag =
        overrides && 'etagOverride' in overrides ? (overrides.etagOverride ?? null) : buffer.etag
      const isYaml = isYamlPath(path)
      const wasNew = buffer.isNew && !buffer.exists

      // Apply an explicit merge at submission, so later typing has a clear baseline.
      if (overrides?.contentOverride !== undefined) {
        if (activePathRef.current === path) editorRef.current?.setValue(content)
        store.updateContent(path, content)
      }
      store.setSaving(path, true)
      try {
        const res = await writeFile(path, content, buffer.exists || (overrides && 'etagOverride' in overrides) ? etag : IF_MATCH_CREATE_ONLY)
        captureWorkspaceEditors()
        if (activePathRef.current === path) captureActive()
        // A successful save supersedes any lingering stale-file warning.
        toast.dismiss(`stale:${path}`)
        useWorkspaceStore.getState().setEtagAndBaseline(path, res.etag ?? null, content)
        // Re-anchor the entity document's parse handle to the just-saved text
        // (risk 11): no-op for paths documentStore hasn't open + echo-suppressed
        // in the common case; genuinely re-parses on a merged / kept-mine save
        // whose text differs from the handle's last projection.
        const current = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
        if (current) useDocumentStore.getState().reparse(path, current.content)

        if (isYaml && res.yaml_error) {
          // Write persisted but the YAML is unparseable: marker + Problems
          // entry, and skip auto-validate.
          if (activePathRef.current === path) {
            editorRef.current?.setYamlMarkers([res.yaml_error])
          }
          setSyntheticSyntaxIssue(path, res.yaml_error)
          toast.error(`Saved ${filename} with YAML syntax error`)
        } else {
          if (isYaml) {
            if (activePathRef.current === path) editorRef.current?.clearYamlMarkers()
            setSyntheticSyntaxIssue(path, null)
            const pipeline = derivePipelineFromYaml(content) ?? undefined
            if (overrides?.validate !== false) startScopedValidate(runController, pipeline)
          }
          toast.success(`Saved ${filename}`)
        }

        queryClient.invalidateQueries({ queryKey: ['files'] })
        markTemplatePreviewsStale()
        if (path.startsWith('templates/')) {
          void queryClient.invalidateQueries({ queryKey: ['templates'] })
          void queryClient.invalidateQueries({ queryKey: ['template'] })
        }
        if (wasNew && isYaml) {
          // First save of a new flowgroup YAML changes the project topology
          // (the retired create-modal's post-create invalidations). Refetch the
          // inspection caches so the new file appears in the tree/lists — but do
          // NOT invalidate the graph keys: under serve-stale a brand-new
          // flowgroup misses the graph cache, so that would force an ungated
          // cold rebuild and leave a freshly-rebuilt-graph + stale-badge state.
          // The file watcher's graph-stale mark drives the badge; the user's
          // Refresh owns the single rebuild. (Mirrors CreateFlowgroupDialog.)
          queryClient.invalidateQueries({ queryKey: ['flowgroups'] })
          queryClient.invalidateQueries({ queryKey: ['pipelines'] })
        }
        return true
      } catch (err) {
        if (err instanceof ApiError && err.status === 412) {
          const language = buffer.language
          // Stable per-path id: repeated failed saves REPLACE the toast
          // instead of stacking; save/reload of the path dismisses it by id.
          toast.error(`${filename} changed on disk since you opened it`, {
            id: `stale:${path}`,
            duration: Infinity,
            action: {
              label: 'Resolve',
              onClick: () => {
                // Read "mine" at CLICK time, not failed-save time: the toast
                // can sit for minutes while the user keeps editing, and a
                // frozen snapshot would silently discard post-412 edits on
                // keep-mine. Flush the live editor first when it's this path.
                if (activePathRef.current === path) captureActive()
                const current = useWorkspaceStore
                  .getState()
                  .buffers.find((b) => b.path === path)
                setConflict({ path, mine: current?.content ?? content, language })
              },
            },
          })
        } else {
          const message = err instanceof ApiError ? err.message : 'Failed to save file'
          toast.error(message)
        }
        return false
      } finally {
        useWorkspaceStore.getState().setSaving(path, false)
      }
    },
    [editorRef, activePathRef, captureActive, queryClient, runController, setSyntheticSyntaxIssue],
  )

  const saveActive = useCallback(async (): Promise<boolean> => {
    const path = activeDocumentPathRef.current
    if (!path) return false
    captureWorkspaceEditors()
    captureActive()
    const buffer = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
    if (buffer?.exists && !buffer.isDirty && !buffer.loading && !buffer.loadFailed) return true
    return saveBuffer(path)
  }, [activeDocumentPathRef, captureActive, saveBuffer])

  const saveAllDirty = useCallback(async (options?: { validate?: boolean }): Promise<boolean> => {
    captureWorkspaceEditors()
    captureActive()
    const dirtyPaths = useWorkspaceStore.getState().buffers.filter((b) => b.isDirty).map((b) => b.path)
    let success = true
    for (const path of dirtyPaths) {
      if (!await saveBuffer(path, options)) success = false
    }
    return success && !useWorkspaceStore.getState().buffers.some((b) => b.isDirty)
  }, [captureActive, saveBuffer])

  const cancelConflict = useCallback(() => setConflict(null), [])

  const resolveKeepMine = useCallback(
    (path: string, content: string, freshEtag: string | null) => {
      setConflict(null)
      void saveBuffer(path, { contentOverride: content, etagOverride: freshEtag })
    },
    [saveBuffer],
  )

  const resolveTakeTheirs = useCallback(
    (path: string, content: string, etag: string | null) => {
      setConflict(null)
      // Reload succeeded — the stale-file warning for this path is resolved.
      toast.dismiss(`stale:${path}`)
      useWorkspaceStore.getState().replaceBuffer(path, content, etag)
      // Re-anchor the entity document to the reloaded on-disk text (risk 11).
      useDocumentStore.getState().reparse(path, content)
      if (activePathRef.current === path) editorRef.current?.setValue(content)
    },
    [editorRef, activePathRef],
  )

  return {
    saveBuffer,
    saveActive,
    saveAllDirty,
    conflict,
    cancelConflict,
    resolveKeepMine,
    resolveTakeTheirs,
  }
}
