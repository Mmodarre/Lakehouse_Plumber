import type { QueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

import { writeFile } from '@/api/files'
import { ApiError } from '@/api/client'
import { isReadOnlyPath, useWorkspaceStore } from '@/store/workspaceStore'
import { useDocumentStore } from '@/store/documentStore'
import type { RunController } from '@/store/runStore'
import {
  isYamlPath,
  derivePipelineFromYaml,
  startScopedValidate,
} from '@/components/editor/yamlSaveSupport'

// ── persistBufferToDisk — the disk half of an in-memory commit ───
//
// documentStore.mutate / workspaceStore.updateContent only touch the in-memory
// buffer (they flip `isDirty`). The actual PUT lives in useWorkspaceSave.
// saveBuffer, which is bound to the live Monaco editor ref. Callers that mutate
// a buffer WITHOUT a mounted editor (the graph action modal's ActionModalEditor)
// need the same
// persistence without those editor-only side effects, so this reuses the exact
// on-success marker saveBuffer uses — workspaceStore.setEtagAndBaseline (updates
// etag + baseline, clears isDirty) — plus documentStore.reparse to re-anchor the
// entity handle, and the ['files'] invalidation. It adds NO new store action.
//
// It ALSO mirrors saveBuffer's clean-YAML validation refresh (Fix #1): on a
// clean write of a YAML flowgroup it derives the owning pipeline and starts a
// scoped validate run, so the Validation / Problems panes reflect the modal edit
// instead of going stale. Only that refresh is mirrored — the Monaco-marker /
// synthetic syntax-issue side effects are editor-only and stay in saveBuffer.

/**
 * Write the buffer at `path` to disk and mark it saved. Returns `true` only on
 * a clean write; a 412 conflict or a persisted-but-unparseable YAML both toast
 * and return `false` (the write for a 412 never happened; the yaml_error write
 * did persist but the file needs fixing in the Code view).
 */
export async function persistBufferToDisk(
  path: string,
  queryClient: QueryClient,
  runController: RunController,
): Promise<boolean> {
  const store = useWorkspaceStore.getState()
  const buffer = store.buffers.find((b) => b.path === path)
  if (!buffer || buffer.isSaving) return false

  const filename = path.split('/').pop() ?? path
  if (isReadOnlyPath(path)) {
    toast.error('This file is read-only')
    return false
  }
  if (buffer.loading || buffer.loadFailed) {
    // The buffer never received its on-disk content — writing it would clobber
    // the real file with ''.
    toast.error(`${filename} has not loaded — retry loading before saving`)
    return false
  }

  const content = buffer.content
  const etag = buffer.etag

  store.setSaving(path, true)
  try {
    const res = await writeFile(path, content, etag)
    // A successful save supersedes any lingering stale-file warning.
    toast.dismiss(`stale:${path}`)
    // The SAME marker saveBuffer uses: update etag + baseline, clear isDirty.
    useWorkspaceStore.getState().setEtagAndBaseline(path, res.etag ?? null, content)
    // Re-anchor the entity document's parse handle to the just-saved text
    // (a no-op here since the buffer text already equals the handle projection).
    useDocumentStore.getState().reparse(path, content)
    void queryClient.invalidateQueries({ queryKey: ['files'] })
    // The operational-metadata columns/presets are declared in the project-root
    // lhp.yaml, so a write there can change what MetadataMultiSelect offers —
    // refresh that query. Scoped to the root config EXACTLY (path === 'lhp.yaml')
    // so ordinary flowgroup saves (any nested *.yaml) don't refetch it.
    if (path === 'lhp.yaml') {
      void queryClient.invalidateQueries({ queryKey: ['operational-metadata'] })
    }

    if (res.yaml_error) {
      toast.error(
        `Saved ${filename} with a YAML syntax error — open it in the Code view to fix it.`,
      )
      return false
    }
    // Clean write: refresh validation exactly as the canonical save path does
    // (useWorkspaceSave.saveBuffer lines 121-122) — derive the owning pipeline
    // and kick a scoped validate so the Validation / Problems panes reflect the
    // just-saved edit. Gated on isYamlPath like saveBuffer (a scoping miss only
    // widens the run, never breaks it).
    if (isYamlPath(path)) {
      const pipeline = derivePipelineFromYaml(content) ?? undefined
      startScopedValidate(runController, pipeline)
    }
    toast.success(`Saved ${filename}`)
    return true
  } catch (err) {
    if (err instanceof ApiError && err.status === 412) {
      toast.error(
        `${filename} changed on disk since you opened it — open it in the Code view to resolve the conflict.`,
        { id: `stale:${path}` },
      )
      void queryClient.invalidateQueries({ queryKey: ['files'] })
    } else {
      const message = err instanceof ApiError ? err.message : 'Failed to save file'
      toast.error(message)
    }
    return false
  } finally {
    useWorkspaceStore.getState().setSaving(path, false)
  }
}
