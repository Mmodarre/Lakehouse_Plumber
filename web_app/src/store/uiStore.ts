import { create } from 'zustand'

export type SelectedNode = {
  type: 'flowgroup' | 'preset' | 'template' | 'pipeline'
  name: string
} | null

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
}

export const useUIStore = create<UIState>((set) => ({
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
}))
