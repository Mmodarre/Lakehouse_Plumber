import { create } from 'zustand'
import { persist } from 'zustand/middleware'

// ── layoutStore — the one persisted geometry store ───────────
//
// Dumb geometry state for the unified workspace shell (§6.4): panel widths /
// heights, collapse flags, the active explorer lens, and the active
// inspector/bottom tabs. Absorbs what used to be scattered across
// uiStore.sidebarOpen, the `lhp-sidebar-width` localStorage key, and
// assistantStore.panelOpen/panelWidth. It holds NO domain data and NO derived
// state — just persisted layout. Plain setters only.

/** Left-explorer lens (§3 / D5). */
export type ExplorerLens = 'structure' | 'tables' | 'files'
/** Right-inspector tab (§3). */
export type InspectorTab = 'validation' | 'help'
/** Bottom-panel tab (§3 / D12). */
export type BottomTab = 'problems' | 'run' | 'history'

interface LayoutState {
  /** Left explorer width in px (drag-resizable). */
  explorerWidth: number
  /** Active explorer lens. */
  explorerLens: ExplorerLens
  /** Explorer collapsed to nothing (⌘B). */
  explorerCollapsed: boolean
  /** Right inspector width in px. */
  inspectorWidth: number
  /** Inspector collapsed to its 42px rail (⌘I). */
  inspectorCollapsed: boolean
  /** Active inspector tab. */
  inspectorTab: InspectorTab
  /** Assistant dock expanded (vs its ~44px rail) — D3. */
  assistantOpen: boolean
  /** Assistant dock expanded width in px. */
  assistantWidth: number
  /** Bottom panel height in px when expanded. */
  bottomHeight: number
  /** Bottom panel collapsed (⌘J). */
  bottomCollapsed: boolean
  /** Active bottom-panel tab. */
  bottomTab: BottomTab
  /** Read-only viewer lens (D10) — feeds the readOnly chain + `data-viewer`. */
  viewerMode: boolean

  setExplorerWidth: (px: number) => void
  setExplorerLens: (lens: ExplorerLens) => void
  setExplorerCollapsed: (collapsed: boolean) => void
  toggleExplorer: () => void
  setInspectorWidth: (px: number) => void
  setInspectorCollapsed: (collapsed: boolean) => void
  toggleInspector: () => void
  setInspectorTab: (tab: InspectorTab) => void
  setAssistantOpen: (open: boolean) => void
  toggleAssistant: () => void
  setAssistantWidth: (px: number) => void
  setBottomHeight: (px: number) => void
  setBottomCollapsed: (collapsed: boolean) => void
  toggleBottom: () => void
  setBottomTab: (tab: BottomTab) => void
  setViewerMode: (on: boolean) => void
  toggleViewerMode: () => void
}

// Defaults per §3 geometry: explorer ~260, inspector ~300, assistant
// closed/320, bottom collapsed, lens 'files', inspectorTab 'validation',
// bottomTab 'problems', viewerMode off.
const DEFAULTS = {
  explorerWidth: 260,
  explorerLens: 'files' as ExplorerLens,
  explorerCollapsed: false,
  inspectorWidth: 300,
  inspectorCollapsed: false,
  inspectorTab: 'validation' as InspectorTab,
  assistantOpen: false,
  assistantWidth: 320,
  bottomHeight: 240,
  bottomCollapsed: true,
  bottomTab: 'problems' as BottomTab,
  viewerMode: false,
}

export const useLayoutStore = create<LayoutState>()(
  persist(
    (set) => ({
      ...DEFAULTS,

      setExplorerWidth: (px) => set({ explorerWidth: px }),
      setExplorerLens: (lens) => set({ explorerLens: lens }),
      setExplorerCollapsed: (collapsed) => set({ explorerCollapsed: collapsed }),
      toggleExplorer: () => set((s) => ({ explorerCollapsed: !s.explorerCollapsed })),
      setInspectorWidth: (px) => set({ inspectorWidth: px }),
      setInspectorCollapsed: (collapsed) => set({ inspectorCollapsed: collapsed }),
      toggleInspector: () => set((s) => ({ inspectorCollapsed: !s.inspectorCollapsed })),
      setInspectorTab: (tab) => set({ inspectorTab: tab }),
      setAssistantOpen: (open) => set({ assistantOpen: open }),
      toggleAssistant: () => set((s) => ({ assistantOpen: !s.assistantOpen })),
      setAssistantWidth: (px) => set({ assistantWidth: px }),
      setBottomHeight: (px) => set({ bottomHeight: px }),
      setBottomCollapsed: (collapsed) => set({ bottomCollapsed: collapsed }),
      toggleBottom: () => set((s) => ({ bottomCollapsed: !s.bottomCollapsed })),
      setBottomTab: (tab) => set({ bottomTab: tab }),
      setViewerMode: (on) => set({ viewerMode: on }),
      toggleViewerMode: () => set((s) => ({ viewerMode: !s.viewerMode })),
    }),
    {
      name: 'lhp-layout',
      // v1: the default explorer lens became 'files' (was 'structure').
      //   Sessions persisted before this store had a version are treated as v0;
      //   a plain default change never reaches them, so the v0 → v1 migrate
      //   resets the lens to the new default ONCE.
      // v2: the Action inspector tab was removed (field editing moved to the
      //   graph's double-click modal), so `inspectorTab` no longer admits
      //   'action'. A session persisted with 'action' is normalized to
      //   'validation' so a stale stored value can't slip past the narrowed
      //   type. Every other persisted field survives both hops.
      version: 2,
      migrate: (persisted, version) => {
        const state = { ...((persisted ?? {}) as Record<string, unknown>) }
        if (version < 1) {
          state.explorerLens = 'files'
        }
        if (version < 2 && state.inspectorTab === 'action') {
          state.inspectorTab = 'validation'
        }
        return state
      },
      // Persist the geometry only (the setters are dropped by JSON anyway).
      partialize: (s) => ({
        explorerWidth: s.explorerWidth,
        explorerLens: s.explorerLens,
        explorerCollapsed: s.explorerCollapsed,
        inspectorWidth: s.inspectorWidth,
        inspectorCollapsed: s.inspectorCollapsed,
        inspectorTab: s.inspectorTab,
        assistantOpen: s.assistantOpen,
        assistantWidth: s.assistantWidth,
        bottomHeight: s.bottomHeight,
        bottomCollapsed: s.bottomCollapsed,
        bottomTab: s.bottomTab,
        viewerMode: s.viewerMode,
      }),
    },
  ),
)
