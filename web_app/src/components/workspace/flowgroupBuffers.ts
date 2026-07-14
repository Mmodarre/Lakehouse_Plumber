import { useEffect } from 'react'
import { toast } from 'sonner'
import { fetchFileContentWithMeta } from '../../api/files'
import { fetchFlowgroupRelatedFiles } from '../../api/flowgroups'
import { errorMessage } from '../../lib/errors'
import { useUIStore } from '../../store/uiStore'
import { useWorkspaceStore } from '../../store/workspaceStore'

// ── flowgroupBuffers — flowgroup ↔ workspace-buffer glue ─────
//
// The scaffold + related-file logic extracted from the retired
// FlowgroupEditorModal: opening a flowgroup (edit) or creating one now
// materialises workspace buffers instead of a modal's local tab state.

export function generateScaffoldYaml(pipeline: string, name: string): string {
  return `pipeline: ${pipeline}\nflowgroup: ${name}\ndescription: ""\n\nactions: []\n`
}

/** Fetch a file's on-disk content into a `loading` (or `loadFailed`, on
 * retry) buffer. A failure is surfaced and marks the buffer load-failed,
 * which blocks editing/saving until a retry succeeds — a load-failed buffer
 * holds no real content and must never reach `writeFile`. */
export async function loadBufferContent(path: string): Promise<void> {
  try {
    const { content, etag } = await fetchFileContentWithMeta(path)
    useWorkspaceStore.getState().markLoaded(path, content, etag)
  } catch (err) {
    useWorkspaceStore.getState().markLoadFailed(path)
    const name = path.split('/').pop() ?? path
    toast.error(`Failed to load ${name}: ${errorMessage(err, 'request failed')}`)
  }
}

/** Create-flowgroup: open the scaffolded YAML as a dirty, unsaved buffer.
 * Saving it (isNew + yaml) triggers the flowgroup-creation invalidations. */
export function openFlowgroupCreateBuffers(
  name: string,
  pipeline: string,
  filePath: string,
): void {
  useWorkspaceStore.getState().openBuffer(filePath, {
    content: generateScaffoldYaml(pipeline, name),
    originalContent: '',
    isDirty: true,
    isNew: true,
    exists: false,
    category: 'yaml',
  })
}

/** Edit-flowgroup: open the source YAML + its related files as a buffer
 * group (source focused), then fetch content for the files that exist. */
export async function openFlowgroupEditBuffers(name: string, env: string): Promise<void> {
  const related = await fetchFlowgroupRelatedFiles(name, env)
  const files: Array<{ path: string; category: string; exists: boolean }> = [
    { ...related.source_file, category: 'yaml' },
    ...related.related_files,
  ]

  const ws = useWorkspaceStore.getState()
  // Buffers already open keep their state (openBuffer is no-op-if-open);
  // only the placeholders created here get a content fetch.
  const alreadyOpen = new Set(ws.buffers.map((b) => b.path))
  for (const f of files) {
    ws.openBuffer(f.path, {
      category: f.category,
      exists: f.exists,
      isNew: !f.exists,
      loading: f.exists && !alreadyOpen.has(f.path),
      activate: false,
    })
  }
  ws.setActive(related.source_file.path)

  await Promise.all(
    files.filter((f) => f.exists && !alreadyOpen.has(f.path)).map((f) => loadBufferContent(f.path)),
  )
}

/**
 * Bridge from the uiStore flowgroup-editor trigger to workspace buffers.
 *
 * @deprecated The feeders (drill modals / tables / CreateFlowgroupDialog that
 * once called `openFlowgroupEditor(...)` / `openFlowgroupEditorCreate(...)`)
 * were demolished with the old shell, so no caller sets the request state
 * today — this bridge is currently inert. It reads `uiStore.flowgroupEditor`
 * and, when set, opens workspace buffers instead of the retired modal.
 * Mounted once in AppShell. Scheduled for retirement in the
 * entity-integration task (T2.4).
 */
export function useFlowgroupEditorBridge(): void {
  const flowgroupEditor = useUIStore((s) => s.flowgroupEditor)

  useEffect(() => {
    if (!flowgroupEditor) return
    const ui = useUIStore.getState()
    // Consume the request synchronously so StrictMode's double effect-run
    // (and re-renders while the async open is in flight) can't duplicate it.
    ui.closeFlowgroupEditor()
    // The workspace opens behind any drill dialog — close the drill stack so
    // the buffers are actually visible (the old editor modal stacked on top).
    ui.closePipelineModal()

    if (flowgroupEditor.mode === 'create' && flowgroupEditor.filePath) {
      openFlowgroupCreateBuffers(
        flowgroupEditor.name,
        flowgroupEditor.pipeline,
        flowgroupEditor.filePath,
      )
    } else {
      void openFlowgroupEditBuffers(flowgroupEditor.name, ui.selectedEnv).catch(
        (err: unknown) => {
          toast.error(errorMessage(err, 'Failed to open flowgroup files'))
        },
      )
    }
  }, [flowgroupEditor])
}
