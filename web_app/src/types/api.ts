// ── Error Types ──────────────────────────────────────────

export interface ErrorDetail {
  code: string
  category: string
  message: string
  details: string
  suggestions: string[]
  context: Record<string, unknown>
  http_status: number
}

export interface ErrorResponse {
  error: ErrorDetail
}

// ── Health ───────────────────────────────────────────────

export interface HealthResponse {
  status: string
  version: string
  python_version: string
  dev_mode: boolean
}

// ── Project ──────────────────────────────────────────────

export interface ResourceCounts {
  pipelines: number
  flowgroups: number
  presets: number
  templates: number
  environments: number
}

export interface ProjectInfoResponse {
  name: string
  version: string
  description: string | null
  author: string | null
  resource_counts: ResourceCounts
}

export interface ProjectStatsResponse {
  total_pipelines: number
  total_flowgroups: number
  total_actions: number
  actions_by_type: Record<string, number>
  pipelines: Array<Record<string, unknown>>
}

// ── Pipelines ────────────────────────────────────────────

export interface PipelineSummary {
  name: string
  flowgroup_count: number
  action_count: number
}

export interface PipelineListResponse {
  pipelines: PipelineSummary[]
  total: number
}

export interface PipelineDetailResponse {
  name: string
  flowgroup_count: number
  flowgroups: string[]
  config: Record<string, unknown>
}

// ── Flowgroups ───────────────────────────────────────────

export interface FlowgroupSummary {
  name: string
  pipeline: string
  action_count: number
  action_types: string[]
  source_file: string
  presets: string[]
  template: string | null
}

export interface FlowgroupListResponse {
  flowgroups: FlowgroupSummary[]
  total: number
}

export interface PipelineFlowgroupsResponse {
  flowgroups: FlowgroupSummary[]
  total: number
}

// ── Tables ───────────────────────────────────────────────

export interface TableSummary {
  full_name: string
  target_type: string
  pipeline: string
  flowgroup: string
  write_mode: string | null
  scd_type: number | null
  source_file: string
}

export interface TableListResponse {
  tables: TableSummary[]
  total: number
  warnings: string[]
}

export interface FlowgroupDetailResponse {
  flowgroup: Record<string, unknown>
  source_file: string
}

export interface ResolvedFlowgroupResponse {
  flowgroup: Record<string, unknown>
  environment: string
  applied_presets: string[]
  applied_template: string | null
}

export interface RelatedFileInfo {
  path: string
  category: 'sql' | 'python' | 'schema' | 'expectations'
  action_name: string
  field: string
  exists: boolean
}

export interface FlowgroupRelatedFilesResponse {
  flowgroup: string
  source_file: RelatedFileInfo
  related_files: RelatedFileInfo[]
  environment: string
}

// ── Presets ──────────────────────────────────────────────

export interface PresetListResponse {
  presets: string[]
  total: number
}

export interface PresetDetailResponse {
  name: string
  raw: Record<string, unknown>
  resolved: Record<string, unknown>
}

// ── Templates ────────────────────────────────────────────

export interface TemplateInfoResponse {
  name: string
  version: string
  description: string
  parameters: Array<Record<string, unknown>>
  action_count: number
}

export interface TemplateListResponse {
  templates: string[]
  total: number
}

export interface TemplateDetailResponse {
  name: string
  template: TemplateInfoResponse
}

// ── Environments ─────────────────────────────────────────

export interface EnvironmentListResponse {
  environments: string[]
  total: number
}

// ── Dependencies ─────────────────────────────────────────

export interface GraphNode {
  id: string
  label: string
  type: string
  pipeline: string
  flowgroup: string
  stage: number
  metadata: Record<string, unknown>
}

export interface GraphEdge {
  source: string
  target: string
  type: string
}

export interface GraphMetadata {
  level: string
  total_nodes: number
  total_edges: number
  stages: number
  has_circular: boolean
  circular_dependencies: string[][]
  external_sources: string[]
}

export interface GraphResponse {
  nodes: GraphNode[]
  edges: GraphEdge[]
  metadata: GraphMetadata
}

export interface ExecutionOrderResponse {
  stages: string[][]
  total_stages: number
  flat_order: string[]
}

export interface CircularDependencyResponse {
  has_circular: boolean
  cycles: string[][]
  total_cycles: number
}

// ── Validation ───────────────────────────────────────────

export interface ValidateResponse {
  success: boolean
  errors: string[]
  warnings: string[]
  validated_pipelines: string[]
  error_message: string | null
}

// ── State / Staleness ────────────────────────────────────

export interface StalenessResponse {
  has_work_to_do: boolean
  pipelines_needing_generation: Record<string, unknown>
  pipelines_up_to_date: Record<string, number>
  has_global_changes: boolean
  global_changes: string[]
  total_new: number
  total_stale: number
  total_up_to_date: number
}

// ── Graph Level Type ─────────────────────────────────────

export type GraphLevel = 'pipeline' | 'flowgroup' | 'action'

// ── Files ───────────────────────────────────────────────

export interface FileItem {
  name: string
  type: 'file' | 'directory'
  size: number
}

export interface FileListResponse {
  items: FileItem[]
  total: number
  offset: number
  limit: number
  project_root: string
}

export interface FileReadResponse {
  type: 'file' | 'directory' | 'binary'
  path: string
  content?: string
  items?: FileItem[]
  size?: number
  message?: string
}

export interface FileWriteResponse {
  written: boolean
  path: string
}

// ── Git / Workspace ─────────────────────────────────────

export interface GitStatusResponse {
  branch: string
  modified: string[]
  staged: string[]
  untracked: string[]
  ahead: number
  behind: number
}

export interface CommitResponse {
  committed: boolean
  sha: string
}

// ── Workspace Lifecycle ─────────────────────────────────

export type WorkspaceState = 'creating' | 'active' | 'idle' | 'stopped' | 'deleted'

export interface WorkspaceResponse {
  state: WorkspaceState
  branch: string
  created_at: number
  last_activity: number
  has_uncommitted_changes: boolean
}

export interface HeartbeatResponse {
  ok: boolean
}
