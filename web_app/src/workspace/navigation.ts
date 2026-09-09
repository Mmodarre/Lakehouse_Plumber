import { create } from 'zustand'
import type { WorkspaceTabRef } from '@/store/workspaceStore'

export function encodeTab(tab: WorkspaceTabRef): string { return JSON.stringify(tab) }
export function decodeTab(value: string | null): WorkspaceTabRef | null {
  if (!value || value.length > 8000) return null
  try {
    const t = JSON.parse(value) as Record<string, unknown>
    const strings = (...keys: string[]) => keys.every((key) => typeof t[key] === 'string')
    switch (t.kind) {
      case 'file': return strings('path') ? { kind: 'file', path: t.path as string } : null
      case 'entity': return strings('pipeline', 'flowgroup', 'filePath') && ['flowgroup', 'template'].includes(String(t.docKind))
        ? { kind: 'entity', pipeline: t.pipeline as string, flowgroup: t.flowgroup as string, filePath: t.filePath as string, docKind: t.docKind as 'flowgroup' | 'template', view: t.view === 'code' ? 'code' : 'graph' } : null
      case 'config': return strings('path') && ['project', 'pipeline', 'job'].includes(String(t.configKind))
        ? { kind: 'config', path: t.path as string, configKind: t.configKind as 'project' | 'pipeline' | 'job', view: t.view === 'yaml' ? 'yaml' : 'form' } : null
      case 'project-map': return { kind: 'project-map' }
      case 'pipeline-dag': return strings('pipeline') ? { kind: 'pipeline-dag', pipeline: t.pipeline as string } : null
      case 'table-detail': return strings('fqn') ? { kind: 'table-detail', fqn: t.fqn as string } : null
      case 'resource': return strings('name', 'filePath') && ['preset', 'template', 'blueprint', 'environment'].includes(String(t.resourceKind))
        ? { kind: 'resource', resourceKind: t.resourceKind as 'preset' | 'template' | 'blueprint' | 'environment', name: t.name as string, filePath: t.filePath as string } : null
      default: return null
    }
  } catch { return null }
}

interface NavigationState {
  entries: WorkspaceTabRef[]
  index: number
  visit: (tab: WorkspaceTabRef) => void
  move: (offset: number) => WorkspaceTabRef | null
  reset: () => void
}
export const useNavigationStore = create<NavigationState>((set, get) => ({
  entries: [], index: -1,
  visit: (tab) => set((s) => {
    if (s.entries[s.index] && encodeTab(s.entries[s.index]) === encodeTab(tab)) return {}
    const entries = [...s.entries.slice(0, s.index + 1), tab].slice(-100)
    return { entries, index: entries.length - 1 }
  }),
  move: (offset) => {
    const s = get(), index = s.index + offset
    if (index < 0 || index >= s.entries.length) return null
    set({ index }); return s.entries[index]
  },
  reset: () => set({ entries: [], index: -1 }),
}))
