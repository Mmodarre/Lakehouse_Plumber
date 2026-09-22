import { create } from 'zustand'
import { useWorkspaceStore } from '@/store/workspaceStore'
import type { RailSelection } from './docFormSupport'

interface ViewState {
  selection?: RailSelection
  query?: string
  configuredOnly?: boolean
  collapsed?: Record<string, boolean>
  section?: string
  scrollTop?: number
}

interface ConfigViewStore {
  views: Record<string, ViewState>
  update: (path: string, patch: Partial<ViewState>) => void
}

/** Presentation state follows an open file, and is dropped when that file closes. */
export const useConfigViewStore = create<ConfigViewStore>((set) => ({
  views: {},
  update: (path, patch) =>
    set((state) => ({
      views: { ...state.views, [path]: { ...state.views[path], ...patch } },
    })),
}))

useWorkspaceStore.subscribe((state, previous) => {
  if (state.buffers === previous.buffers) return
  const open = new Set(state.buffers.map((buffer) => buffer.path))
  const closed = previous.buffers.filter((buffer) => !open.has(buffer.path))
  if (!closed.length) return
  useConfigViewStore.setState(({ views }) => ({
    views: Object.fromEntries(
      Object.entries(views).filter(
        ([key]) =>
          !closed.some((buffer) => key === buffer.path || key.startsWith(`${buffer.path}#`)),
      ),
    ),
  }))
})
