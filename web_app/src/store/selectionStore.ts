import { create } from 'zustand'
import type { InspectorSelection } from '@/components/designer/useDesignerCompose'

export type { InspectorSelection } from '@/components/designer/useDesignerCompose'

// ── selectionStore — the graph-canvas selection, per entity tab ──
//
// The bridge between GraphView (which owns the canvas + computes the current
// InspectorSelection) and the right-dock Inspector's Action editor (a separate
// subtree). GraphView keeps its own local `selectedId` for the in-canvas ring /
// structural toolbar; it ALSO publishes the resolved selection here keyed by the
// tab's id, so the Inspector can render an editor for the selected node without
// GraphView and Inspector sharing a parent. Ephemeral (NOT persisted): a
// selection is meaningless across a reload.

interface SelectionState {
  /** Current graph selection per entity tab id. Absent key ⇒ nothing selected. */
  byTab: Record<string, InspectorSelection>
  /** Publish (or clear, with `null`) the selection for a tab. */
  setSelection: (tabId: string, selection: InspectorSelection | null) => void
  /** Clear the selection for a tab (pane click / tab change / unmount). */
  clearSelection: (tabId: string) => void
  /** Read the current selection for a tab (`null` when none). */
  getSelection: (tabId: string) => InspectorSelection | null
}

export const useSelectionStore = create<SelectionState>((set, get) => ({
  byTab: {},
  setSelection: (tabId, selection) =>
    set((s) => {
      if (selection === null) {
        if (!(tabId in s.byTab)) return s
        const next = { ...s.byTab }
        delete next[tabId]
        return { byTab: next }
      }
      return { byTab: { ...s.byTab, [tabId]: selection } }
    }),
  clearSelection: (tabId) =>
    set((s) => {
      if (!(tabId in s.byTab)) return s
      const next = { ...s.byTab }
      delete next[tabId]
      return { byTab: next }
    }),
  getSelection: (tabId) => get().byTab[tabId] ?? null,
}))

/** Subscribe to the selection for a tab id (`null` id ⇒ always `null`). */
export function useTabSelection(tabId: string | null): InspectorSelection | null {
  return useSelectionStore((s) => (tabId === null ? null : (s.byTab[tabId] ?? null)))
}
