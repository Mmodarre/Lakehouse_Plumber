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

// ── Graph Level Type ─────────────────────────────────────

export type GraphLevel = 'pipeline' | 'flowgroup' | 'action'

// ── Files ───────────────────────────────────────────────

// Single recursive tree node from `GET /api/files`. The backend returns ONE
// full tree (no pagination, no lazy per-directory fetch). The root node has
// `path: ""`. `children` is present only on directory nodes.
export interface FileNode {
  name: string
  path: string
  type: 'file' | 'directory'
  children?: FileNode[]
}

// `GET /api/files` returns the root node of the recursive tree directly.
export type FileListResponse = FileNode

export interface FileWriteResponse {
  written: boolean
  path: string
  // Set (1-based, Monaco-compatible) when a *.yaml/*.yml write has a YAML
  // syntax error. The write STILL persists; this only reports the parse
  // failure so the editor can surface a marker. `null` on clean writes and
  // for non-YAML files.
  yaml_error: { line: number; column: number; message: string } | null
}

// ── Stream: serialized run-result DTOs ───────────────────
//
// These mirror the public lhp.api DTOs as serialized by
// `lhp.api._serialization.to_dict` (Path → string, Tuple → array,
// Mapping → object, None → null, dataclass → object keyed by field
// name; @property methods are NOT serialized). Field names match the
// Python dataclass fields exactly.

// Per-issue diagnostic — projection of `lhp.api.views.ValidationIssueView`.
// Replaces the old flat `string[]` errors/warnings on ValidateResponse.
export interface ValidationIssue {
  code: string
  category: string
  severity: 'error' | 'warning'
  title: string
  details: string | null
  pipeline_name: string | null
  flowgroup_name: string | null
  file_path: string | null
  suggestions: string[]
  context: Record<string, unknown>
  doc_link: string | null
}

// Projection of `lhp.api.responses.ValidationResponse`.
export interface ValidationResult {
  success: boolean
  issues: ValidationIssue[]
  validated_pipelines: string[]
  error_message: string | null
}

// Projection of `lhp.api.responses.BatchValidationResponse` — the
// `response` payload carried by a `ValidationCompletedFrame`.
export interface BatchValidationResult {
  success: boolean
  pipeline_responses: Record<string, ValidationResult>
  total_errors: number
  total_warnings: number
  validated_pipelines: string[]
  error_message: string | null
  error_code: string | null
}

// Projection of `lhp.api.responses.GenerationResponse`.
export interface GenerationResult {
  success: boolean
  generated_filenames: string[]
  files_written: number
  total_flowgroups: number
  output_location: string | null
  performance_info: Record<string, unknown>
  duration_s: number
  error_message: string | null
  error_code: string | null
  error: ValidationIssue | null
}

// Projection of `lhp.api.responses.BatchGenerationResponse` — the
// `response` payload carried by a `GenerationCompletedFrame`.
export interface BatchGenerationResult {
  success: boolean
  pipeline_responses: Record<string, GenerationResult>
  total_files_written: number
  aggregate_generated_filenames: string[]
  output_location: string | null
  error_message: string | null
  error_code: string | null
}

// ── Stream: NDJSON frames ────────────────────────────────
//
// One JSON object per line over POST /api/{validate,generate}/stream.
// Discriminated by `type`. Event frames use the public event class
// name as the discriminant; their remaining keys are the event's
// dataclass fields (serialized structurally).

export interface OperationStartedFrame {
  type: 'OperationStarted'
  operation_name: string
  env: string | null
}

export interface PhaseStartedFrame {
  type: 'PhaseStarted'
  phase: string
}

export interface PhaseCompletedFrame {
  type: 'PhaseCompleted'
  phase: string
  duration_s: number
  success: boolean
}

export interface PipelineStartedFrame {
  type: 'PipelineStarted'
  pipeline: string
}

export interface PipelineCompletedFrame {
  type: 'PipelineCompleted'
  pipeline: string
  duration_s: number
  files_written: number
}

export interface PipelineFailedFrame {
  type: 'PipelineFailed'
  pipeline: string
  code: string
  message: string
}

export interface WarningEmittedFrame {
  type: 'WarningEmitted'
  message: string
  code: string
  category: string
  file: string | null
  flowgroup: string | null
}

export interface ValidationCompletedFrame {
  type: 'ValidationCompleted'
  response: BatchValidationResult
}

export interface GenerationCompletedFrame {
  type: 'GenerationCompleted'
  response: BatchGenerationResult
}

// Synthetic transport frames (not LHP events).

export interface ProgressFrame {
  type: 'progress'
  total: number
  done: number
  current: string | null
}

export interface InfoFrame {
  type: 'info'
  code: string
  message: string
}

// Terminal error frame — the stream ends after this is emitted.
export interface ErrorFrame {
  type: 'error'
  code: string
  title: string
  details: string | null
  suggestions: string[]
  context: Record<string, unknown>
  doc_link: string | null
}

export type StreamFrame =
  | OperationStartedFrame
  | PhaseStartedFrame
  | PhaseCompletedFrame
  | PipelineStartedFrame
  | PipelineCompletedFrame
  | PipelineFailedFrame
  | WarningEmittedFrame
  | ValidationCompletedFrame
  | GenerationCompletedFrame
  | ProgressFrame
  | InfoFrame
  | ErrorFrame

// ── Stream: reshaped run state the UI keeps after a run ──
//
// What a validate/generate run retains once the stream terminates: the
// terminal response DTO plus the flat list of issues surfaced during
// the run (replacing the legacy flat `string[]` errors).
export interface ValidationRunResult {
  kind: 'validation'
  response: BatchValidationResult
  issues: ValidationIssue[]
}

export interface GenerationRunResult {
  kind: 'generation'
  response: BatchGenerationResult
  issues: ValidationIssue[]
}

export type RunResult = ValidationRunResult | GenerationRunResult
