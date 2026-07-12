// Hand-maintained HTTP contract types for the LHP web IDE.
//
// Most response types below are re-based onto the generated OpenAPI schema
// (`./api.generated`, regenerated from the FastAPI backend via
// `npm run gen:api`) so backend schema drift surfaces as a tsc break or a
// git diff on the generated file. Types that are deliberately richer than
// the wire schema (literal-union refinements) extend the generated type
// instead; types with no OpenAPI counterpart (untyped endpoints, the
// middleware error envelope, NDJSON stream frames) stay fully hand-written.

import type { components } from './api.generated'

type Schemas = components['schemas']

// ── Error Types ──────────────────────────────────────────
// Error envelope produced by the error-handler middleware — not an OpenAPI
// response model, so it has no generated counterpart.

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

// Refinement of the generated schema: `project_state` keeps a literal union
// for autocomplete/typo-checking on the known states ('ok' when a project is
// loaded, 'no_project' when the server started outside an LHP project); the
// `(string & {})` arm keeps future backend values assignable. Optional for
// older backends that did not send it (absent → treated as loaded).
export interface HealthResponse
  extends Omit<Schemas['HealthResponse'], 'project_state'> {
  project_state?: 'ok' | 'no_project' | (string & {})
}

// ── Project ──────────────────────────────────────────────

export type ResourceCounts = Schemas['ResourceCounts']
export type ProjectInfoResponse = Schemas['ProjectInfoResponse']
export type ProjectStatsResponse = Schemas['ProjectStatsResponse']

// ── Pipelines ────────────────────────────────────────────

export type PipelineSummary = Schemas['PipelineSummary']
export type PipelineListResponse = Schemas['PipelineListResponse']
export type PipelineDetailResponse = Schemas['PipelineDetailResponse']

// ── Flowgroups ───────────────────────────────────────────

export type FlowgroupSummary = Schemas['FlowgroupSummary']
export type FlowgroupListResponse = Schemas['FlowgroupListResponse']
export type PipelineFlowgroupsResponse = Schemas['PipelineFlowgroupsResponse']

// ── Tables ───────────────────────────────────────────────

export type TableSummary = Schemas['TableSummary']
export type TableListResponse = Schemas['TableListResponse']

export type FlowgroupDetailResponse = Schemas['FlowgroupDetailResponse']
export type ResolvedFlowgroupResponse = Schemas['ResolvedFlowgroupResponse']
export type FlowgroupActionSummary = Schemas['FlowgroupActionSummary']

// Refinement of the generated schema: `category` is narrowed to the known
// file categories (the wire type is a plain string).
export interface RelatedFileInfo
  extends Omit<Schemas['RelatedFileInfo'], 'category'> {
  category: 'sql' | 'python' | 'schema' | 'expectations'
}

export interface FlowgroupRelatedFilesResponse
  extends Omit<
    Schemas['FlowgroupRelatedFilesResponse'],
    'source_file' | 'related_files'
  > {
  source_file: RelatedFileInfo
  related_files: RelatedFileInfo[]
}

// ── Presets ──────────────────────────────────────────────

export type PresetListResponse = Schemas['PresetListResponse']
export type PresetDetailResponse = Schemas['PresetDetailResponse']

// ── Templates ────────────────────────────────────────────

export type TemplateInfoResponse = Schemas['TemplateInfoResponse']
export type TemplateListResponse = Schemas['TemplateListResponse']
export type TemplateDetailResponse = Schemas['TemplateDetailResponse']

// ── Environments ─────────────────────────────────────────

// `GET /api/environments` returns an untyped dict on the backend, so there
// is no generated counterpart for this shape.
export interface EnvironmentListResponse {
  environments: string[]
  total: number
}

// `GET /api/environments/{env}/resolved` — resolved substitution context.
export type SecretReferenceSummary = Schemas['SecretReferenceSummary']
export type SubstitutionResolvedResponse = Schemas['SubstitutionResolvedResponse']

// ── Sandbox ──────────────────────────────────────────────

// `GET /api/sandbox` — the developer's `.lhp/profile.yaml` selection plus the
// concrete pipeline scope `lhp generate --sandbox` would run.
export type SandboxScope = Schemas['SandboxResponse']

// ── Blueprints ───────────────────────────────────────────

export type BlueprintInstanceSummary = Schemas['BlueprintInstanceSummary']
export type BlueprintSummary = Schemas['BlueprintSummary']
export type BlueprintListResponse = Schemas['BlueprintListResponse']

// ── Project init ─────────────────────────────────────────

// `POST /api/project/init` — scaffolding a new project when the server was
// started outside one (health `project_state === 'no_project'`).
export type InitProjectRequest = Schemas['InitProjectRequest']
export type InitProjectResponse = Schemas['InitProjectResponse']

// ── Dependencies ─────────────────────────────────────────

export type GraphNode = Schemas['GraphNode']
export type GraphEdge = Schemas['GraphEdge']
export type GraphMetadata = Schemas['GraphMetadata']
export type GraphResponse = Schemas['GraphResponse']
export type ExecutionOrderResponse = Schemas['ExecutionOrderResponse']
export type CircularDependencyResponse = Schemas['CircularDependencyResponse']

// ── Runs (run history) ───────────────────────────────────

export type RunSummary = Schemas['RunSummary']
export type RunIssue = Schemas['RunIssue']
export type RunDetail = Schemas['RunDetail']
export type RunListResponse = Schemas['RunListResponse']

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
// full tree (no pagination, no lazy per-directory fetch) as an untyped dict,
// so there is no generated counterpart. The root node has `path: ""`.
// `children` is present only on directory nodes.
export interface FileNode {
  name: string
  path: string
  type: 'file' | 'directory'
  children?: FileNode[]
}

// `GET /api/files` returns the root node of the recursive tree directly.
export type FileListResponse = FileNode

// `PUT /api/files/{path}` response — see the generated schema's docstring
// for the `yaml_error` (write persists; 1-based Monaco diagnostic) and
// `etag` (optimistic-concurrency token) semantics.
export type FileWriteResponse = Schemas['FileWriteResponse']

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
