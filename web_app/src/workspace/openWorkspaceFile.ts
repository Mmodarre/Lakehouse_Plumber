import { toast } from 'sonner'
import { captureWorkspaceEditors, focusInvalidWorkspaceDraft } from './editorCommands'
import { errorMessage } from '@/lib/errors'
import { fetchFileContentWithMeta } from '@/api/files'
import { tabBufferPath, useWorkspaceStore, workspaceTabId, type ConfigKind } from '@/store/workspaceStore'

let latestRequest = 0
let navigationRevision = 0
useWorkspaceStore.subscribe((state, previous) => {
  if (state.activePath !== previous.activePath || state.projectRoot !== previous.projectRoot) navigationRevision++
})

export function configurationKindForPath(path: string): ConfigKind | undefined {
  if (path === 'lhp.yaml' || path === 'lhp.yml') return 'project'
  const name = path.split('/').pop() ?? path
  if (/^pipeline_config.*\.ya?ml$/i.test(name)) return 'pipeline'
  if (/^(?:monitoring_)?job_config.*\.ya?ml$/i.test(name)) return 'job'
  return undefined
}

export function isTemplateSourcePath(path: string): boolean {
  return /^templates\/.+\.ya?ml$/i.test(path)
}

/** Every file entry point uses document ownership, never a raw path as a tab ID. */
async function openWorkspaceFileInternal(
  filePath: string,
  options: { source?: boolean; line?: number; configKind?: ConfigKind },
  request: number,
): Promise<void> {
  const path = filePath.replace(/^\/+/, '')
  if (!path) return
  let state = useWorkspaceStore.getState()
  const configKind = options.configKind ?? configurationKindForPath(path)
  if (isTemplateSourcePath(path)) {
    const existing = state.tabs.find((tab) => tabBufferPath(tab) === path)
    const name = existing?.kind === 'entity' ? existing.flowgroup : path.slice('templates/'.length).replace(/\.ya?ml$/i, '')
    state.openEntityTab('', name, path, { docKind: 'template', ...(options.source || options.line ? { view: 'code' as const } : {}) })
  } else if (configKind) {
    state.openConfigTab(path, configKind, options.source === undefined ? undefined : { view: options.source ? 'yaml' : 'form' })
  }
  state = useWorkspaceStore.getState()
  const owner = state.tabs.find((t) => workspaceTabId(t) === state.activePath && tabBufferPath(t) === path)
    ?? state.tabs.find((t) => tabBufferPath(t) === path)
  if (owner?.kind === 'resource' && options.source) {
    const previousId = workspaceTabId(owner)
    useWorkspaceStore.setState((s) => ({
      tabs: s.tabs.map((tab) => workspaceTabId(tab) === previousId ? { kind: 'file' as const, path } : tab),
      activePath: path, pinnedTabIds: s.pinnedTabIds.map((id) => id === previousId ? path : id),
    }))
  } else if (owner) {
    const id = workspaceTabId(owner)
    if (options.source) {
      if (owner.kind === 'entity') state.setTabView(id, 'code')
      if (owner.kind === 'config') state.setTabView(id, 'yaml')
    }
    state.setActive(id)
  }
  let stillRequested = true
  if (!state.buffers.some((b) => b.path === path)) {
    const expectedRevision = navigationRevision
    const projectRoot = state.projectRoot
    const { content, etag } = await fetchFileContentWithMeta(path)
    if (useWorkspaceStore.getState().projectRoot !== projectRoot) return
    stillRequested = request === latestRequest && expectedRevision === navigationRevision
    useWorkspaceStore.getState().openBuffer(path, { content, etag, exists: true, activate: stillRequested })
  } else if (!owner) {
    state.openBuffer(path)
  }
  if (stillRequested && (options.source || (typeof options.line === 'number' && options.line > 0))) {
    useWorkspaceStore.getState().revealFile(path, options.line ?? 0)
  }
}

export async function openWorkspaceFile(
  filePath: string,
  options: { source?: boolean; line?: number; configKind?: ConfigKind } = {},
): Promise<void> {
  try {
    captureWorkspaceEditors()
    if (focusInvalidWorkspaceDraft()) {
      toast.error('Correct the invalid field before opening another file')
      return
    }
    await openWorkspaceFileInternal(filePath, options, ++latestRequest)
  }
  catch (error) { toast.error(errorMessage(error, 'Failed to open file')) }
}
