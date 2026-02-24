import { create } from 'zustand'

export type WorkspaceStatus = 'init' | 'checking' | 'no_workspace' | 'creating' | 'active' | 'error'

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

  // Open file (Monaco-ready)
  openFile: { path: string; content: string; isDirty: boolean; isSaving: boolean } | null
  openFilePath: (path: string, content: string) => void
  closeFile: () => void
  setFileDirty: (dirty: boolean) => void
  setFileSaving: (saving: boolean) => void
  updateFileContent: (content: string) => void

  // Create flowgroup dialog
  createFlowgroupDialog: boolean
  openCreateFlowgroupDialog: () => void
  closeCreateFlowgroupDialog: () => void

  // Flowgroup editor (multi-tab)
  flowgroupEditor: {
    name: string
    pipeline: string
    mode?: 'edit' | 'create'
    filePath?: string
  } | null
  openFlowgroupEditor: (name: string, pipeline: string) => void
  openFlowgroupEditorCreate: (name: string, pipeline: string, filePath: string) => void
  closeFlowgroupEditor: () => void

  // Workspace lifecycle
  workspaceStatus: WorkspaceStatus
  workspaceError: string | null
  setWorkspaceStatus: (status: WorkspaceStatus) => void
  setWorkspaceError: (error: string | null) => void
  setWorkspaceLost: () => void
  setWorkspaceActive: () => void
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

  // Open file (Monaco-ready)
  openFile: null,
  openFilePath: (path, content) => set({ openFile: { path, content, isDirty: false, isSaving: false } }),
  closeFile: () => set({ openFile: null }),
  setFileDirty: (dirty) => set((s) => (s.openFile ? { openFile: { ...s.openFile, isDirty: dirty } } : {})),
  setFileSaving: (saving) => set((s) => (s.openFile ? { openFile: { ...s.openFile, isSaving: saving } } : {})),
  updateFileContent: (content) =>
    set((s) => (s.openFile ? { openFile: { ...s.openFile, content, isDirty: false } } : {})),

  // Create flowgroup dialog
  createFlowgroupDialog: false,
  openCreateFlowgroupDialog: () => set({ createFlowgroupDialog: true }),
  closeCreateFlowgroupDialog: () => set({ createFlowgroupDialog: false }),

  // Flowgroup editor (multi-tab)
  flowgroupEditor: null,
  openFlowgroupEditor: (name, pipeline) => set({ flowgroupEditor: { name, pipeline } }),
  openFlowgroupEditorCreate: (name, pipeline, filePath) =>
    set({
      flowgroupEditor: { name, pipeline, mode: 'create', filePath },
      createFlowgroupDialog: false,
    }),
  closeFlowgroupEditor: () => set({ flowgroupEditor: null }),

  // Workspace lifecycle
  workspaceStatus: 'init',
  workspaceError: null,
  setWorkspaceStatus: (status) => set({ workspaceStatus: status, workspaceError: null }),
  setWorkspaceError: (error) => set({ workspaceStatus: 'error', workspaceError: error }),
  setWorkspaceLost: () => set({ workspaceStatus: 'no_workspace', workspaceError: null }),
  setWorkspaceActive: () => set({ workspaceStatus: 'active', workspaceError: null }),
}))
