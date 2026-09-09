import { tabBufferPath, workspaceTabId, type EditorBuffer, type WorkspaceTabRef } from '@/store/workspaceStore'

export type TabCloseCommand = 'close' | 'others' | 'right' | 'saved' | 'all'

/** Bulk commands respect pins and use the clicked tab as their anchor. */
export function tabCloseTargets(
  tabs: WorkspaceTabRef[], buffers: EditorBuffer[], pinnedIds: string[],
  anchor: string, command: TabCloseCommand,
): string[] {
  const index = tabs.findIndex((t) => workspaceTabId(t) === anchor)
  if (index < 0) return []
  return tabs.filter((tab, i) => {
    const id = workspaceTabId(tab)
    if (command === 'close') return id === anchor
    if (pinnedIds.includes(id)) return false
    if (command === 'others') return id !== anchor
    if (command === 'right') return i > index
    if (command === 'saved') return !buffers.find((b) => b.path === tabBufferPath(tab))?.isDirty
    return true
  }).map(workspaceTabId)
}

/** Dirty documents only need review when their last owning tab is closing. */
export function closingDocumentPaths(tabs: WorkspaceTabRef[], ids: string[]): string[] {
  const targets = new Set(ids)
  const retained = new Set(tabs.filter((t) => !targets.has(workspaceTabId(t))).map(tabBufferPath))
  return [...new Set(tabs.filter((t) => targets.has(workspaceTabId(t))).map(tabBufferPath)
    .filter((path): path is string => path !== null && !retained.has(path)))]
}
