import { create } from 'zustand'
import { useWorkspaceStore, isReadOnlyPath } from './workspaceStore'
import { useLayoutStore } from './layoutStore'
import { useDocumentStore } from './documentStore'

type Change = { before: string; after: string }
type History = { undo: Change[]; redo: Change[] }
export const useDocumentHistoryStore = create<{
  histories: Record<string, History>
  record: (path: string, before: string, after: string) => void
  apply: (path: string, direction: 'undo' | 'redo') => boolean
}>((set, get) => ({
  histories: {},
  record(path, before, after) {
    if (before === after) return
    const previous = get().histories[path]
    const undo = previous?.undo.at(-1)?.after === before ? previous.undo : []
    set((s) => ({ histories: { ...s.histories, [path]: { undo: [...undo, { before, after }].slice(-50), redo: [] } } }))
  },
  apply(path, direction) {
    const history = get().histories[path]
    const change = history?.[direction].at(-1)
    const buffer = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
    if (!change || !buffer || buffer.loading || buffer.loadFailed || buffer.isSaving || useLayoutStore.getState().viewerMode || isReadOnlyPath(path)) return false
    // A Code edit or external refresh invalidates graph history; never overwrite it.
    if (buffer.content !== (direction === 'undo' ? change.after : change.before)) return false
    const text = direction === 'undo' ? change.before : change.after
    const opposite = direction === 'undo' ? 'redo' : 'undo'
    useWorkspaceStore.getState().updateContent(path, text)
    useDocumentStore.getState().reparse(path, text)
    set((s) => ({ histories: { ...s.histories, [path]: { ...history, [direction]: history[direction].slice(0, -1), [opposite]: [...history[opposite], change] } } }))
    return true
  },
}))

// History belongs to open documents in this project; it must not survive a
// close/reopen or a switch to another served project with the same file paths.
useWorkspaceStore.subscribe((state, previous) => {
  if (state.projectRoot !== previous.projectRoot) {
    useDocumentHistoryStore.setState({ histories: {} })
    return
  }
  if (state.buffers === previous.buffers) return
  const paths = new Set(state.buffers.map((buffer) => buffer.path))
  const histories = useDocumentHistoryStore.getState().histories
  if (Object.keys(histories).some((path) => !paths.has(path))) {
    useDocumentHistoryStore.setState({ histories: Object.fromEntries(Object.entries(histories).filter(([path]) => paths.has(path))) })
  }
})
