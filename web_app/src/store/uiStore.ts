import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface UIState {
  // Environment
  selectedEnv: string
  setSelectedEnv: (env: string) => void

  // Sandbox mode (persisted). ON narrows the dashboard, lists, and file tree
  // to the pipelines resolved from .lhp/profile.yaml and switches
  // Validate/Generate to --sandbox; OFF = the full project.
  sandboxEnabled: boolean
  setSandboxEnabled: (enabled: boolean) => void
  toggleSandbox: () => void

  // Graph
  pipelineFilter: string | null
  setPipelineFilter: (pipeline: string | null) => void

  /**
   * @deprecated Drill-down modal navigation. Replaced by workspaceStore center
   * tabs (openEntityTab / openProjectMap). Still consumed by the surviving
   * graph surfaces (DependencyGraph / ExternalBadge fire openPipelineModal on
   * node click) and the Wave-4-reserved FlowgroupMiniGraph; retired in a later
   * wave once node clicks route to center tabs.
   */
  drillPipeline: string | null
  /** @deprecated See {@link UIState.drillPipeline}. */
  openPipelineModal: (name: string) => void
  /** @deprecated See {@link UIState.drillPipeline}. */
  closePipelineModal: () => void
  /** @deprecated See {@link UIState.drillPipeline}. */
  drillFlowgroup: { name: string; pipeline: string } | null
  /** @deprecated See {@link UIState.drillPipeline}. */
  openFlowgroupModal: (name: string, pipeline: string) => void
  /** @deprecated See {@link UIState.drillPipeline}. */
  closeFlowgroupModal: () => void

  // Create flowgroup dialog
  createFlowgroupDialog: boolean
  /** Optional seed for the dialog (e.g. the pipeline the drill modal opened from). */
  createFlowgroupSeed: { pipeline?: string } | null
  openCreateFlowgroupDialog: (seed?: { pipeline?: string }) => void
  closeCreateFlowgroupDialog: () => void

  /**
   * @deprecated Flowgroup open/create request bridge — consumed by
   * components/workspace/flowgroupBuffers.ts (useFlowgroupEditorBridge, mounted
   * in AppShell). Replaced by the direct workspaceStore.openEntityTab action.
   * The bridge hook still reads this state; its feeders were demolished with
   * the old shell, so no caller sets it today. Retired in T2.4.
   */
  flowgroupEditor: {
    name: string
    pipeline: string
    mode?: 'edit' | 'create'
    filePath?: string
  } | null
  /** @deprecated Use workspaceStore.openEntityTab. */
  openFlowgroupEditor: (name: string, pipeline: string) => void
  /** @deprecated Use workspaceStore.openEntityTab. */
  openFlowgroupEditorCreate: (name: string, pipeline: string, filePath: string) => void
  /** @deprecated See {@link UIState.flowgroupEditor}. */
  closeFlowgroupEditor: () => void

  // Run configuration — the pipeline_config file Validate/Generate use
  // (persisted). Deliberately SEPARATE from any per-tab file selection:
  // only the explicit "Use for runs" toggle (or the chip's clear) changes it.
  selectedPipelineConfig: string | null
  setSelectedPipelineConfig: (path: string | null) => void
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      // Environment
      selectedEnv: 'dev',
      setSelectedEnv: (env) => set({ selectedEnv: env }),

      // Sandbox mode (persisted)
      sandboxEnabled: false,
      setSandboxEnabled: (enabled) => set({ sandboxEnabled: enabled }),
      toggleSandbox: () => set((s) => ({ sandboxEnabled: !s.sandboxEnabled })),

      // Graph
      pipelineFilter: null,
      setPipelineFilter: (pipeline) => set({ pipelineFilter: pipeline }),

      // Drill-down navigation
      drillPipeline: null,
      openPipelineModal: (name) => set({ drillPipeline: name, drillFlowgroup: null }),
      closePipelineModal: () => set({ drillPipeline: null, drillFlowgroup: null }),
      drillFlowgroup: null,
      openFlowgroupModal: (name, pipeline) => set({ drillFlowgroup: { name, pipeline } }),
      closeFlowgroupModal: () => set({ drillFlowgroup: null }),

      // Create flowgroup dialog
      createFlowgroupDialog: false,
      createFlowgroupSeed: null,
      openCreateFlowgroupDialog: (seed) =>
        set({ createFlowgroupDialog: true, createFlowgroupSeed: seed ?? null }),
      closeCreateFlowgroupDialog: () =>
        set({ createFlowgroupDialog: false, createFlowgroupSeed: null }),

      // Flowgroup open/create request (consumed by the workspace bridge)
      flowgroupEditor: null,
      openFlowgroupEditor: (name, pipeline) => set({ flowgroupEditor: { name, pipeline } }),
      openFlowgroupEditorCreate: (name, pipeline, filePath) =>
        set({
          flowgroupEditor: { name, pipeline, mode: 'create', filePath },
          createFlowgroupDialog: false,
        }),
      closeFlowgroupEditor: () => set({ flowgroupEditor: null }),

      // Run pipeline-config selection (persisted)
      selectedPipelineConfig: null,
      setSelectedPipelineConfig: (path) => set({ selectedPipelineConfig: path }),
    }),
    {
      name: 'lhp-ui',
      // Everything else in this store is deliberately session-only; only the
      // run-config binding and the sandbox toggle survive a reload.
      partialize: (s) => ({
        selectedPipelineConfig: s.selectedPipelineConfig,
        sandboxEnabled: s.sandboxEnabled,
      }),
    },
  ),
)
