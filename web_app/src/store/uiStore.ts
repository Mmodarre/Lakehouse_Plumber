import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export type SelectedNode = {
  type: 'flowgroup' | 'preset' | 'template' | 'pipeline'
  name: string
} | null

/** The two file-backed Config tabs (kept dependency-free on purpose: the
 * persisted slice below must never import config components). */
export type ConfigFileTab = 'pipeline' | 'job'

interface UIState {
  // Environment
  selectedEnv: string
  setSelectedEnv: (env: string) => void

  // Graph
  pipelineFilter: string | null
  setPipelineFilter: (pipeline: string | null) => void

  // Detail modal
  selectedNode: SelectedNode
  setSelectedNode: (node: SelectedNode) => void
  modalOpen: boolean
  openModal: (node: SelectedNode) => void
  closeModal: () => void

  // Sidebar
  sidebarOpen: boolean
  toggleSidebar: () => void

  // Drill-down navigation
  drillPipeline: string | null
  openPipelineModal: (name: string) => void
  closePipelineModal: () => void
  drillFlowgroup: { name: string; pipeline: string } | null
  openFlowgroupModal: (name: string, pipeline: string) => void
  closeFlowgroupModal: () => void

  // Create flowgroup dialog
  createFlowgroupDialog: boolean
  openCreateFlowgroupDialog: () => void
  closeCreateFlowgroupDialog: () => void

  // Flowgroup open/create request — consumed by the workspace bridge
  // (components/workspace/flowgroupBuffers.ts), which opens the flowgroup's
  // files as workspace buffers and then clears this via closeFlowgroupEditor.
  flowgroupEditor: {
    name: string
    pipeline: string
    mode?: 'edit' | 'create'
    filePath?: string
  } | null
  openFlowgroupEditor: (name: string, pipeline: string) => void
  openFlowgroupEditorCreate: (name: string, pipeline: string, filePath: string) => void
  closeFlowgroupEditor: () => void

  // Config section — per-tab file selection (persisted; see `partialize`)
  selectedConfigFiles: Record<ConfigFileTab, string | null>
  setSelectedConfigFile: (tab: ConfigFileTab, path: string | null) => void

  // Run configuration — the pipeline_config file Validate/Generate use
  // (persisted). Deliberately SEPARATE from selectedConfigFiles: picking a
  // file in the pipeline tab never implicitly changes what runs use; only
  // the explicit "Use for runs" toggle (or the header chip's clear) does.
  selectedPipelineConfig: string | null
  setSelectedPipelineConfig: (path: string | null) => void
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      // Environment
      selectedEnv: 'dev',
      setSelectedEnv: (env) => set({ selectedEnv: env }),

      // Graph
      pipelineFilter: null,
      setPipelineFilter: (pipeline) => set({ pipelineFilter: pipeline }),

      // Detail modal
      selectedNode: null,
      setSelectedNode: (node) => set({ selectedNode: node }),
      modalOpen: false,
      openModal: (node) => set({ selectedNode: node, modalOpen: true }),
      closeModal: () => set({ modalOpen: false, selectedNode: null }),

      // Sidebar
      sidebarOpen: true,
      toggleSidebar: () => set((s) => ({ sidebarOpen: !s.sidebarOpen })),

      // Drill-down navigation
      drillPipeline: null,
      openPipelineModal: (name) => set({ drillPipeline: name, drillFlowgroup: null }),
      closePipelineModal: () => set({ drillPipeline: null, drillFlowgroup: null }),
      drillFlowgroup: null,
      openFlowgroupModal: (name, pipeline) => set({ drillFlowgroup: { name, pipeline } }),
      closeFlowgroupModal: () => set({ drillFlowgroup: null }),

      // Create flowgroup dialog
      createFlowgroupDialog: false,
      openCreateFlowgroupDialog: () => set({ createFlowgroupDialog: true }),
      closeCreateFlowgroupDialog: () => set({ createFlowgroupDialog: false }),

      // Flowgroup open/create request (consumed by the workspace bridge)
      flowgroupEditor: null,
      openFlowgroupEditor: (name, pipeline) => set({ flowgroupEditor: { name, pipeline } }),
      openFlowgroupEditorCreate: (name, pipeline, filePath) =>
        set({
          flowgroupEditor: { name, pipeline, mode: 'create', filePath },
          createFlowgroupDialog: false,
        }),
      closeFlowgroupEditor: () => set({ flowgroupEditor: null }),

      // Config section file selection (persisted)
      selectedConfigFiles: { pipeline: null, job: null },
      setSelectedConfigFile: (tab, path) =>
        set((s) => ({
          selectedConfigFiles: { ...s.selectedConfigFiles, [tab]: path },
        })),

      // Run pipeline-config selection (persisted)
      selectedPipelineConfig: null,
      setSelectedPipelineConfig: (path) => set({ selectedPipelineConfig: path }),
    }),
    {
      name: 'lhp-ui',
      // Everything else in this store is deliberately session-only; only the
      // Config tabs' file selection and the run-config binding survive a
      // reload.
      partialize: (s) => ({
        selectedConfigFiles: s.selectedConfigFiles,
        selectedPipelineConfig: s.selectedPipelineConfig,
      }),
    },
  ),
)
