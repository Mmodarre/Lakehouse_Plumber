/**
 * Public surface of the config-model layer (Config UI Phase 1b).
 *
 * Typed view-models, document classification, passthrough detection, and
 * pure client-side validators for the three config surfaces (lhp.yaml /
 * pipeline_config / job_config), mirroring the Python loaders. Forms render
 * from these models and block Save on `severity: 'error'` issues; writes go
 * through `../yaml-doc`.
 */
export type { ValidationIssue } from './validators'
export {
  errorIssue,
  intRangeCheck,
  isEnumMember,
  isPlainObject,
  isStrictInt,
  isStringArray,
  nonStringValueKeys,
  parseLaxBool,
  presentKeys,
  warningIssue,
} from './validators'
export type { IntRangeResult } from './validators'

export type {
  PipelineCluster,
  PipelineClusterAutoscale,
  PipelineDefaultsDoc,
  PipelineDoc,
  PipelineDocKind,
  PipelineEventLogOverride,
  PipelineNotification,
  PipelinePermissionEntry,
  PipelineSettings,
} from './pipelineConfig'
export {
  classifyPipelineDoc,
  listPipelinePassthroughKeys,
  MONITORING_PIPELINE_ALIAS,
  PIPELINE_ALLOWED_CHANNELS,
  PIPELINE_ALLOWED_EDITIONS,
  PIPELINE_ALLOWED_PACKAGING_MODES,
  PIPELINE_BUILTIN_DEFAULTS,
  PIPELINE_KNOWN_KEYS,
  validatePipelineConfigFile,
} from './pipelineConfig'

export type {
  JobDefaultsDoc,
  JobDoc,
  JobDocKind,
  JobEmailNotifications,
  JobNotebookCluster,
  JobPermissionEntry,
  JobSchedule,
  JobSettings,
  JobWebhookNotifications,
} from './jobConfig'
export {
  classifyJobDoc,
  JOB_BUILTIN_DEFAULTS,
  JOB_KNOWN_KEYS,
  listJobPassthroughKeys,
  validateJobConfigFile,
} from './jobConfig'

export type {
  EventLogSection,
  MetadataColumnInput,
  MetadataColumnSpec,
  MetadataPresetInput,
  MetadataPresetSpec,
  MonitoringMaterializedView,
  MonitoringSection,
  OperationalMetadataSection,
  ProjectConfigModel,
  SandboxSection,
  TestReportingSection,
  UcTaggingSection,
  WheelSection,
} from './projectConfig'
export {
  listProjectPassthroughKeys,
  listProjectSections,
  PROJECT_KNOWN_KEYS,
  PROJECT_OPTIONAL_SECTIONS,
  validateProjectConfig,
} from './projectConfig'
