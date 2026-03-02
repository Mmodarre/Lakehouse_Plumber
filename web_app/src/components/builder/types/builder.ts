import type { Node, Edge } from '@xyflow/react'

// ── Wizard Navigation ────────────────────────────────────

export type WizardStep =
  | 'basic-info'
  | 'choose-path'
  | 'template-wizard'
  | 'flow-canvas'
  | 'preview-save'

export type BuilderPath = 'template' | 'canvas' | null

// ── Step 1: Basic Info ───────────────────────────────────

export interface BasicInfo {
  pipeline: string
  isNewPipeline: boolean
  newPipeline: string
  flowgroupName: string
  subdirectory: string
  isNewSubdir: boolean
  newSubdir: string
  presets: string[]
}

// ── Step 3a: Template Wizard ─────────────────────────────

export interface TemplateInfo {
  templateName: string
  parameters: Record<string, unknown>
}

// ── Step 3b: Flow Canvas ─────────────────────────────────

/** Convert an action name to a valid LHP view name (used as `target` for loads/transforms). */
export function toViewName(actionName: string): string {
  return 'v_' + actionName.toLowerCase().replace(/[^a-z0-9_]/g, '_')
}

export type ActionType = 'load' | 'transform' | 'write'

export type ActionSubtype =
  // Load subtypes
  | 'cloudfiles'
  | 'delta'
  | 'sql'
  | 'jdbc'
  | 'python'
  | 'custom_datasource'
  | 'kafka'
  // Transform subtypes
  | 'sql_transform'
  | 'python_transform'
  | 'data_quality'
  | 'temp_table'
  | 'schema'
  // Write subtypes
  | 'streaming_table'
  | 'materialized_view'
  | 'sink'

export interface ActionNodeConfig {
  id: string
  actionName: string
  actionType: ActionType
  actionSubtype: ActionSubtype
  target: string
  config: Record<string, unknown>
  yamlOverride?: string
  isYAMLMode: boolean
  /** Unknown/extra fields from YAML deserialization, displayed read-only */
  _extras?: Record<string, unknown>
}

export interface BuilderNodeData extends Record<string, unknown> {
  actionType: ActionType
  actionSubtype: ActionSubtype
  actionName: string
  label: string
  isConfigured: boolean
}

export type BuilderNode = Node<BuilderNodeData, 'builderAction'>
export type BuilderEdge = Edge

// ── Action Catalog Entry ─────────────────────────────────

export interface ActionCatalogEntry {
  type: ActionType
  subtype: ActionSubtype
  label: string
  description: string
  hasMVPForm: boolean
}

// ── API Types for detail endpoints ───────────────────────

export interface TemplateSummary {
  name: string
  description: string | null
  parameter_count: number
  action_count: number
  action_types: string[]
}

export interface TemplateListDetailResponse {
  templates: TemplateSummary[]
  total: number
}

export interface PresetSummary {
  name: string
  description: string | null
  extends: string | null
}

export interface PresetListDetailResponse {
  presets: PresetSummary[]
  total: number
}
