import { create } from 'zustand'
import type {
  WizardStep,
  BuilderPath,
  BasicInfo,
  TemplateInfo,
  ActionNodeConfig,
  BuilderNode,
  BuilderEdge,
} from '../types/builder'
import { toViewName } from '../types/builder'
import type { NodeChange, EdgeChange, Connection } from '@xyflow/react'
import { applyNodeChanges, applyEdgeChanges, addEdge } from '@xyflow/react'

export interface EditModeInfo {
  isEdit: boolean
  filePath: string
  originalYAML: string
}

interface LoadStateData {
  basicInfo: Partial<BasicInfo>
  templateInfo?: Partial<TemplateInfo>
  chosenPath: BuilderPath
  nodes: BuilderNode[]
  edges: BuilderEdge[]
  actionConfigs: Map<string, ActionNodeConfig>
}

interface BuilderState {
  // Wizard navigation
  currentStep: WizardStep
  chosenPath: BuilderPath
  setStep: (step: WizardStep) => void
  setPath: (path: BuilderPath) => void

  // Step 1: Basic Info
  basicInfo: BasicInfo
  updateBasicInfo: (updates: Partial<BasicInfo>) => void

  // Step 3a: Template Wizard
  templateInfo: TemplateInfo
  updateTemplateInfo: (updates: Partial<TemplateInfo>) => void

  // Step 3b: Flow Canvas
  nodes: BuilderNode[]
  edges: BuilderEdge[]
  actionConfigs: Map<string, ActionNodeConfig>
  selectedNodeId: string | null
  onNodesChange: (changes: NodeChange<BuilderNode>[]) => void
  onEdgesChange: (changes: EdgeChange[]) => void
  onConnect: (connection: Connection) => void
  addNode: (node: BuilderNode, config: ActionNodeConfig) => void
  removeNode: (id: string) => void
  setSelectedNode: (id: string | null) => void
  updateActionConfig: (id: string, updates: Partial<ActionNodeConfig>) => void

  // YAML output
  generatedYAML: string
  editedYAML: string | null
  setGeneratedYAML: (yaml: string) => void
  setEditedYAML: (yaml: string | null) => void

  // Save state
  isSaving: boolean
  saveError: string | null
  setSaving: (saving: boolean) => void
  setSaveError: (error: string | null) => void

  // Edit mode
  editMode: EditModeInfo | null
  setEditMode: (info: EditModeInfo | null) => void
  /** Bulk-load state from deserialized YAML (used by edit mode). */
  loadState: (data: LoadStateData) => void

  // Reset
  reset: () => void

  // Dirty check
  hasData: () => boolean
}

const initialBasicInfo: BasicInfo = {
  pipeline: '',
  isNewPipeline: false,
  newPipeline: '',
  flowgroupName: '',
  subdirectory: '',
  isNewSubdir: false,
  newSubdir: '',
  presets: [],
}

const initialTemplateInfo: TemplateInfo = {
  templateName: '',
  parameters: {},
}

export const useBuilderStore = create<BuilderState>((set, get) => ({
  // Wizard navigation
  currentStep: 'basic-info',
  chosenPath: null,
  setStep: (step) => set({ currentStep: step }),
  setPath: (path) => set({ chosenPath: path }),

  // Step 1: Basic Info
  basicInfo: { ...initialBasicInfo },
  updateBasicInfo: (updates) =>
    set((s) => ({ basicInfo: { ...s.basicInfo, ...updates } })),

  // Step 3a: Template Wizard
  templateInfo: { ...initialTemplateInfo },
  updateTemplateInfo: (updates) =>
    set((s) => ({ templateInfo: { ...s.templateInfo, ...updates } })),

  // Step 3b: Flow Canvas
  nodes: [],
  edges: [],
  actionConfigs: new Map(),
  selectedNodeId: null,
  onNodesChange: (changes) =>
    set((s) => ({ nodes: applyNodeChanges(changes, s.nodes) })),
  onEdgesChange: (changes) =>
    set((s) => ({ edges: applyEdgeChanges(changes, s.edges) })),
  onConnect: (connection) =>
    set((s) => {
      const newEdges = addEdge(connection, s.edges)
      // Auto-populate downstream node's config.source with upstream target view name
      const sourceConfig = s.actionConfigs.get(connection.source)
      const targetConfig = s.actionConfigs.get(connection.target)
      if (sourceConfig?.target && targetConfig) {
        const newConfigs = new Map(s.actionConfigs)
        newConfigs.set(connection.target, {
          ...targetConfig,
          config: { ...targetConfig.config, source: sourceConfig.target },
        })
        return { edges: newEdges, actionConfigs: newConfigs }
      }
      return { edges: newEdges }
    }),
  addNode: (node, config) =>
    set((s) => {
      const newConfigs = new Map(s.actionConfigs)
      newConfigs.set(node.id, config)
      return { nodes: [...s.nodes, node], actionConfigs: newConfigs }
    }),
  removeNode: (id) =>
    set((s) => {
      const newConfigs = new Map(s.actionConfigs)
      newConfigs.delete(id)
      return {
        nodes: s.nodes.filter((n) => n.id !== id),
        edges: s.edges.filter((e) => e.source !== id && e.target !== id),
        actionConfigs: newConfigs,
        selectedNodeId: s.selectedNodeId === id ? null : s.selectedNodeId,
      }
    }),
  setSelectedNode: (id) => set({ selectedNodeId: id }),
  updateActionConfig: (id, updates) =>
    set((s) => {
      const newConfigs = new Map(s.actionConfigs)
      const existing = newConfigs.get(id)
      if (!existing) return { actionConfigs: newConfigs }

      let merged = { ...existing, ...updates }

      // When actionName changes on a non-write node, sync the target view name
      if (updates.actionName && existing.actionType !== 'write') {
        const oldAutoTarget = toViewName(existing.actionName)
        // If current target matches the auto pattern, recompute it
        if (!merged.target || merged.target === oldAutoTarget) {
          const newTarget = toViewName(updates.actionName)
          merged = { ...merged, target: newTarget }
        }
      }

      newConfigs.set(id, merged)

      let updatedNodes = s.nodes
      // Also update the node's label if actionName changed
      if (updates.actionName) {
        updatedNodes = s.nodes.map((n) =>
          n.id === id
            ? { ...n, data: { ...n.data, actionName: updates.actionName!, label: updates.actionName! } }
            : n,
        )
      }

      // Propagate target changes to downstream nodes' config.source
      const finalTarget = newConfigs.get(id)?.target
      if (finalTarget && finalTarget !== existing.target) {
        const downstreamEdges = s.edges.filter((e) => e.source === id)
        for (const edge of downstreamEdges) {
          const downstream = newConfigs.get(edge.target)
          if (downstream && downstream.config.source === existing.target) {
            newConfigs.set(edge.target, {
              ...downstream,
              config: { ...downstream.config, source: finalTarget },
            })
          }
        }
      }

      return { actionConfigs: newConfigs, nodes: updatedNodes }
    }),

  // YAML output
  generatedYAML: '',
  editedYAML: null,
  setGeneratedYAML: (yaml) => set({ generatedYAML: yaml }),
  setEditedYAML: (yaml) => set({ editedYAML: yaml }),

  // Save state
  isSaving: false,
  saveError: null,
  setSaving: (saving) => set({ isSaving: saving }),
  setSaveError: (error) => set({ saveError: error }),

  // Edit mode
  editMode: null,
  setEditMode: (info) => set({ editMode: info }),
  loadState: (data) =>
    set({
      basicInfo: { ...initialBasicInfo, ...data.basicInfo },
      templateInfo: data.templateInfo
        ? { ...initialTemplateInfo, ...data.templateInfo }
        : { ...initialTemplateInfo },
      chosenPath: data.chosenPath,
      nodes: data.nodes,
      edges: data.edges,
      actionConfigs: data.actionConfigs,
      selectedNodeId: null,
      currentStep: data.chosenPath === 'template' ? 'template-wizard' : 'flow-canvas',
      generatedYAML: '',
      editedYAML: null,
    }),

  // Reset
  reset: () =>
    set({
      currentStep: 'basic-info',
      chosenPath: null,
      basicInfo: { ...initialBasicInfo },
      templateInfo: { ...initialTemplateInfo },
      nodes: [],
      edges: [],
      actionConfigs: new Map(),
      selectedNodeId: null,
      generatedYAML: '',
      editedYAML: null,
      isSaving: false,
      saveError: null,
      editMode: null,
    }),

  // Dirty check
  hasData: () => {
    const s = get()
    return (
      s.basicInfo.flowgroupName !== '' ||
      s.basicInfo.pipeline !== '' ||
      s.nodes.length > 0 ||
      s.templateInfo.templateName !== ''
    )
  },
}))
