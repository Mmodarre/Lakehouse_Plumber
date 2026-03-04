import { parse, parseAllDocuments } from 'yaml'
import { stringify } from 'yaml'
import type {
  ActionType,
  ActionSubtype,
  ActionNodeConfig,
  BasicInfo,
  TemplateInfo,
  BuilderEdge,
} from '../types/builder'

// ── Types ──────────────────────────────────────────────────

export type FlowgroupKind = 'template' | 'canvas' | 'unknown'

export interface DeserializedFlowgroup {
  kind: FlowgroupKind
  basicInfo: Partial<BasicInfo>
  templateInfo?: Partial<TemplateInfo>
  actionConfigs?: ActionNodeConfig[]
  edges?: BuilderEdge[]
  /** Top-level extras not captured by known fields */
  docExtras?: Record<string, unknown>
}

interface ClassifiedAction {
  actionType: ActionType
  actionSubtype: ActionSubtype
}

// ── Known top-level fields ─────────────────────────────────

const KNOWN_DOC_FIELDS = new Set([
  'pipeline', 'flowgroup', 'description', 'presets',
  'use_template', 'template_parameters', 'actions',
])

const KNOWN_ACTION_FIELDS: Record<string, Set<string>> = {
  'load:cloudfiles': new Set([
    'name', 'type', 'readMode', 'source', 'target', 'description', 'operational_metadata',
  ]),
  'load:delta': new Set([
    'name', 'type', 'readMode', 'source', 'target', 'description', 'operational_metadata',
    'where_clause', 'select_columns',
  ]),
  'load:sql': new Set([
    'name', 'type', 'readMode', 'source', 'target', 'description',
  ]),
  'load:python': new Set([
    'name', 'type', 'readMode', 'source', 'target', 'description', 'operational_metadata',
  ]),
  'load:custom_datasource': new Set([
    'name', 'type', 'readMode', 'source', 'target', 'description', 'operational_metadata',
  ]),
  'transform:sql_transform': new Set([
    'name', 'type', 'transform_type', 'source', 'target', 'sql', 'sql_path', 'readMode', 'description',
  ]),
  'transform:python_transform': new Set([
    'name', 'type', 'transform_type', 'source', 'target', 'module_path', 'function_name',
    'parameters', 'readMode', 'description', 'operational_metadata',
  ]),
  'transform:data_quality': new Set([
    'name', 'type', 'transform_type', 'source', 'target', 'expectations_file', 'readMode', 'description',
  ]),
  'transform:temp_table': new Set([
    'name', 'type', 'transform_type', 'source', 'target', 'sql', 'readMode', 'description',
  ]),
  'transform:schema': new Set([
    'name', 'type', 'transform_type', 'source', 'target', 'schema_file', 'schema_inline',
    'enforcement', 'readMode', 'description',
  ]),
  'write:streaming_table': new Set([
    'name', 'type', 'source', 'readMode', 'once', 'create_table', 'table_schema',
    'row_filter', 'spark_conf', 'write_target', 'description',
  ]),
  'write:materialized_view': new Set([
    'name', 'type', 'source', 'table_schema', 'row_filter', 'write_target', 'description',
  ]),
}

// ── Helpers ─────────────────────────────────────────────────

let idCounter = 0

function nextId(): string {
  idCounter++
  return `edit-action-${idCounter}`
}

/** Reset counter (useful for testing). */
export function resetIdCounter(): void {
  idCounter = 0
}

function asString(value: unknown): string {
  if (value === undefined || value === null) return ''
  return String(value)
}

function asStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String)
  return []
}

function asRecord(value: unknown): Record<string, string> {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const result: Record<string, string> = {}
    for (const [k, v] of Object.entries(value)) {
      result[k] = String(v ?? '')
    }
    return result
  }
  return {}
}

// ── Action Classification ──────────────────────────────────

export function classifyAction(action: Record<string, unknown>): ClassifiedAction {
  const actionType = asString(action.type) as ActionType

  if (actionType === 'load') {
    const source = action.source as Record<string, unknown> | undefined
    const sourceType = asString(source?.type).toLowerCase()
    const subtypeMap: Record<string, ActionSubtype> = {
      cloudfiles: 'cloudfiles',
      delta: 'delta',
      sql: 'sql',
      jdbc: 'jdbc',
      python: 'python',
      custom_datasource: 'custom_datasource',
      kafka: 'kafka',
    }
    return { actionType: 'load', actionSubtype: subtypeMap[sourceType] ?? 'cloudfiles' }
  }

  if (actionType === 'transform') {
    const transformType = asString(action.transform_type).toLowerCase()
    const subtypeMap: Record<string, ActionSubtype> = {
      sql: 'sql_transform',
      python: 'python_transform',
      data_quality: 'data_quality',
      temp_table: 'temp_table',
      schema: 'schema',
    }
    return { actionType: 'transform', actionSubtype: subtypeMap[transformType] ?? 'sql_transform' }
  }

  if (actionType === 'write') {
    const writeTarget = action.write_target as Record<string, unknown> | undefined
    const wtType = asString(writeTarget?.type).toLowerCase()
    const subtypeMap: Record<string, ActionSubtype> = {
      streaming_table: 'streaming_table',
      materialized_view: 'materialized_view',
      sink: 'sink',
    }
    return { actionType: 'write', actionSubtype: subtypeMap[wtType] ?? 'streaming_table' }
  }

  // Fallback: try to infer from fields
  if (action.source) return { actionType: 'load', actionSubtype: 'cloudfiles' }
  if (action.transform_type) return { actionType: 'transform', actionSubtype: 'sql_transform' }
  if (action.write_target) return { actionType: 'write', actionSubtype: 'streaming_table' }

  return { actionType: 'load', actionSubtype: 'cloudfiles' }
}

// ── Extras Extraction ──────────────────────────────────────

export function extractExtras(
  obj: Record<string, unknown>,
  knownFields: Set<string>,
): Record<string, unknown> | undefined {
  const extras: Record<string, unknown> = {}
  for (const key of Object.keys(obj)) {
    if (!knownFields.has(key)) {
      extras[key] = obj[key]
    }
  }
  return Object.keys(extras).length > 0 ? extras : undefined
}

// ── Reverse Mappers ────────────────────────────────────────

function reverseCloudFilesLoad(action: Record<string, unknown>): Record<string, unknown> {
  const source = (action.source ?? {}) as Record<string, unknown>
  const options = (source.options ?? {}) as Record<string, unknown>

  const config: Record<string, unknown> = {
    source_path: asString(source.path),
    file_format: asString(source.format),
    read_mode: asString(action.readMode) || 'stream',
    description: asString(action.description),
  }

  // Extract known options back into flat config fields
  if (options['cloudFiles.schemaHints']) config.schema_hints = asString(options['cloudFiles.schemaHints'])
  if (options['cloudFiles.maxFilesPerTrigger']) config.max_files_per_trigger = options['cloudFiles.maxFilesPerTrigger']
  if (options['cloudFiles.schemaEvolutionMode']) config.schema_evolution_mode = asString(options['cloudFiles.schemaEvolutionMode'])
  if (options['cloudFiles.inferColumnTypes'] === false) config.infer_column_types = false
  if (options['cloudFiles.rescuedDataColumn']) config.rescued_data_column = asString(options['cloudFiles.rescuedDataColumn'])

  // Remaining options are reader_options
  const readerOpts: Record<string, string> = {}
  for (const [k, v] of Object.entries(options)) {
    if (!k.startsWith('cloudFiles.')) {
      readerOpts[k] = String(v)
    }
  }
  if (Object.keys(readerOpts).length > 0) config.reader_options = readerOpts

  if (action.operational_metadata) config.operational_metadata = asStringArray(action.operational_metadata)

  return config
}

function reverseDeltaLoad(action: Record<string, unknown>): Record<string, unknown> {
  const source = (action.source ?? {}) as Record<string, unknown>
  const options = (source.options ?? {}) as Record<string, unknown>

  const config: Record<string, unknown> = {
    database: asString(source.database),
    source_table: asString(source.table),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }

  // Delta-specific options
  if (options.readChangeFeed === true) config.readChangeFeed = true
  if (options.startingVersion) config.startingVersion = options.startingVersion
  if (options.startingTimestamp) config.startingTimestamp = asString(options.startingTimestamp)
  if (options.versionAsOf) config.versionAsOf = options.versionAsOf
  if (options.timestampAsOf) config.timestampAsOf = asString(options.timestampAsOf)
  if (options.ignoreDeletes === true) config.ignoreDeletes = true
  if (options.skipChangeCommits === true) config.skipChangeCommits = true
  if (options.maxFilesPerTrigger) config.maxFilesPerTrigger = options.maxFilesPerTrigger

  if (action.where_clause) config.where_clause = asString(action.where_clause)
  if (action.select_columns) config.select_columns = asStringArray(action.select_columns)
  if (action.operational_metadata) config.operational_metadata = asStringArray(action.operational_metadata)

  return config
}

function reverseSQLLoad(action: Record<string, unknown>): Record<string, unknown> {
  const source = (action.source ?? {}) as Record<string, unknown>
  return {
    sql: asString(source.sql),
    sql_file: asString(source.sql_path),
    read_mode: asString(action.readMode) || 'batch',
    description: asString(action.description),
  }
}

function reversePythonLoad(action: Record<string, unknown>): Record<string, unknown> {
  const source = (action.source ?? {}) as Record<string, unknown>
  const config: Record<string, unknown> = {
    module_path: asString(source.module_path),
    function_name: asString(source.function_name),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }

  if (source.parameters) config.parameters = asRecord(source.parameters)
  if (action.operational_metadata) config.operational_metadata = asStringArray(action.operational_metadata)

  return config
}

function reverseCustomDataSourceLoad(action: Record<string, unknown>): Record<string, unknown> {
  const source = (action.source ?? {}) as Record<string, unknown>
  const config: Record<string, unknown> = {
    module_path: asString(source.module_path),
    custom_datasource_class: asString(source.custom_datasource_class),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }

  if (source.options) config.options = asRecord(source.options)
  if (action.operational_metadata) config.operational_metadata = asStringArray(action.operational_metadata)

  return config
}

function reverseSQLTransform(action: Record<string, unknown>): Record<string, unknown> {
  return {
    source: asString(action.source),
    sql: asString(action.sql),
    sql_file: asString(action.sql_path),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }
}

function reversePythonTransform(action: Record<string, unknown>): Record<string, unknown> {
  const config: Record<string, unknown> = {
    module_path: asString(action.module_path),
    function_name: asString(action.function_name),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }

  // Multi-source support
  const source = action.source
  if (Array.isArray(source)) {
    config.sources = asStringArray(source)
  } else {
    config.source = asString(source)
  }

  if (action.parameters) config.parameters = asRecord(action.parameters)
  if (action.operational_metadata) config.operational_metadata = asStringArray(action.operational_metadata)

  return config
}

function reverseDataQualityTransform(action: Record<string, unknown>): Record<string, unknown> {
  return {
    source: asString(action.source),
    expectations_file: asString(action.expectations_file),
    read_mode: asString(action.readMode) || 'stream',
    description: asString(action.description),
  }
}

function reverseTempTableTransform(action: Record<string, unknown>): Record<string, unknown> {
  return {
    source: asString(action.source),
    sql: asString(action.sql),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }
}

function reverseSchemaTransform(action: Record<string, unknown>): Record<string, unknown> {
  return {
    source: asString(action.source),
    schema_file: asString(action.schema_file),
    schema_inline: asString(action.schema_inline),
    enforcement: asString(action.enforcement),
    read_mode: asString(action.readMode),
    description: asString(action.description),
  }
}

function reverseStreamingTableWrite(action: Record<string, unknown>): Record<string, unknown> {
  const wt = (action.write_target ?? {}) as Record<string, unknown>
  const cdcConfig = (wt.cdc_config ?? {}) as Record<string, unknown>

  const config: Record<string, unknown> = {
    source: asString(action.source),
    read_mode: asString(action.readMode),
    description: asString(action.description),
    database: asString(wt.database),
    table: asString(wt.table),
    comment: asString(wt.comment),
  }

  // CDC mode detection
  const mode = asString(wt.mode)
  if (mode === 'cdc' || mode === 'snapshot_cdc') {
    config.cdc_mode = mode
    if (cdcConfig.keys) config.cdc_keys = asStringArray(cdcConfig.keys)
    if (cdcConfig.sequence_by) config.sequence_by = asString(cdcConfig.sequence_by)
    if (cdcConfig.scd_type !== undefined) config.scd_type = cdcConfig.scd_type
    if (cdcConfig.ignore_null_updates === true) config.ignore_null_updates = true
    if (cdcConfig.apply_as_deletes) config.apply_as_deletes = asString(cdcConfig.apply_as_deletes)
    if (cdcConfig.apply_as_truncates) config.apply_as_truncates = asString(cdcConfig.apply_as_truncates)
    if (cdcConfig.track_history_column_list) config.track_history_column_list = asStringArray(cdcConfig.track_history_column_list)
    if (cdcConfig.track_history_except_column_list) config.track_history_except_column_list = asStringArray(cdcConfig.track_history_except_column_list)
    if (cdcConfig.except_column_list) config.except_column_list = asStringArray(cdcConfig.except_column_list)
  }

  if (wt.partition_columns) config.partition_columns = asStringArray(wt.partition_columns)
  if (wt.cluster_columns) config.cluster_columns = asStringArray(wt.cluster_columns)
  if (wt.table_properties) config.table_properties = asRecord(wt.table_properties)
  if (action.spark_conf) config.spark_conf = asRecord(action.spark_conf)
  if (action.once === true) config.once = true
  if (action.create_table === false) config.create_table = false
  if (action.table_schema) config.table_schema = asString(action.table_schema)
  if (action.row_filter) config.row_filter = asString(action.row_filter)

  return config
}

function reverseMaterializedViewWrite(action: Record<string, unknown>): Record<string, unknown> {
  const wt = (action.write_target ?? {}) as Record<string, unknown>

  const config: Record<string, unknown> = {
    source: asString(action.source),
    description: asString(action.description),
    database: asString(wt.database),
    table: asString(wt.table),
    comment: asString(wt.comment),
  }

  // MV can have sql/sql_path in write_target (self-contained MV)
  if (wt.sql) config.wt_sql = asString(wt.sql)
  if (wt.sql_path) config.wt_sql_file = asString(wt.sql_path)

  if (wt.partition_columns) config.partition_columns = asStringArray(wt.partition_columns)
  if (wt.cluster_columns) config.cluster_columns = asStringArray(wt.cluster_columns)
  if (wt.table_properties) config.table_properties = asRecord(wt.table_properties)
  if (action.table_schema) config.table_schema = asString(action.table_schema)
  if (action.row_filter) config.row_filter = asString(action.row_filter)

  return config
}

// ── Reverse Mapper Dispatcher ──────────────────────────────

const UNSUPPORTED_SUBTYPES: Set<ActionSubtype> = new Set(['jdbc', 'kafka', 'sink'])

function reverseMapAction(
  action: Record<string, unknown>,
  classified: ClassifiedAction,
): { config: Record<string, unknown>; isYAMLMode: boolean; yamlOverride?: string } {
  // Unsupported types → YAML-only mode
  if (UNSUPPORTED_SUBTYPES.has(classified.actionSubtype)) {
    const { name: _name, ...rest } = action
    return {
      config: {},
      isYAMLMode: true,
      yamlOverride: stringify(rest, { indent: 2 }),
    }
  }

  const key = `${classified.actionType}:${classified.actionSubtype}`
  let config: Record<string, unknown>

  switch (key) {
    case 'load:cloudfiles':
      config = reverseCloudFilesLoad(action)
      break
    case 'load:delta':
      config = reverseDeltaLoad(action)
      break
    case 'load:sql':
      config = reverseSQLLoad(action)
      break
    case 'load:python':
      config = reversePythonLoad(action)
      break
    case 'load:custom_datasource':
      config = reverseCustomDataSourceLoad(action)
      break
    case 'transform:sql_transform':
      config = reverseSQLTransform(action)
      break
    case 'transform:python_transform':
      config = reversePythonTransform(action)
      break
    case 'transform:data_quality':
      config = reverseDataQualityTransform(action)
      break
    case 'transform:temp_table':
      config = reverseTempTableTransform(action)
      break
    case 'transform:schema':
      config = reverseSchemaTransform(action)
      break
    case 'write:streaming_table':
      config = reverseStreamingTableWrite(action)
      break
    case 'write:materialized_view':
      config = reverseMaterializedViewWrite(action)
      break
    default:
      // Unknown but not explicitly unsupported — try generic
      config = { ...action }
      delete config.name
      delete config.type
      return { config, isYAMLMode: false }
  }

  return { config, isYAMLMode: false }
}

// ── Target Extraction ──────────────────────────────────────

function extractTarget(action: Record<string, unknown>, classified: ClassifiedAction): string {
  // Explicit target field
  if (action.target) return asString(action.target)

  // For write actions, target is write_target.database + write_target.table
  if (classified.actionType === 'write') {
    const wt = action.write_target as Record<string, unknown> | undefined
    if (wt?.table) {
      return wt.database ? `${wt.database}.${wt.table}` : asString(wt.table)
    }
    return ''
  }

  // For loads/transforms, target is usually a view name
  // If not explicitly set, generate from action name
  const name = asString(action.name)
  if (name) return `v_${name.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`
  return ''
}

// ── Edge Reconstruction ────────────────────────────────────

/**
 * Reconstruct edges from action source references.
 * Each action's `source` field may reference another action's `target`.
 */
export function reconstructEdges(actionConfigs: ActionNodeConfig[]): BuilderEdge[] {
  // Build map: target name → node ID
  const targetToId = new Map<string, string>()
  for (const config of actionConfigs) {
    if (config.target) {
      targetToId.set(config.target, config.id)
    }
  }

  const edges: BuilderEdge[] = []
  let edgeCounter = 0

  for (const config of actionConfigs) {
    // Check config.config.source
    const source = config.config.source as string | undefined
    if (source && targetToId.has(source)) {
      edgeCounter++
      edges.push({
        id: `edit-edge-${edgeCounter}`,
        source: targetToId.get(source)!,
        target: config.id,
        type: 'builderEdge',
      })
    }

    // Check config.config.sources (multi-source for python transforms)
    const sources = config.config.sources as string[] | undefined
    if (sources) {
      for (const src of sources) {
        if (targetToId.has(src)) {
          edgeCounter++
          edges.push({
            id: `edit-edge-${edgeCounter}`,
            source: targetToId.get(src)!,
            target: config.id,
            type: 'builderEdge',
          })
        }
      }
    }

    // For write actions, check source at top level of the action config
    if (config.actionType === 'write') {
      const writeSource = config.config.source as string | undefined
      if (writeSource && targetToId.has(writeSource) && !edges.some((e) => e.target === config.id && e.source === targetToId.get(writeSource))) {
        // Already handled above
      }
    }
  }

  return edges
}

// ── Flowgroup Kind Detection ───────────────────────────────

export function detectFlowgroupKind(parsed: Record<string, unknown>): FlowgroupKind {
  if (parsed.use_template) return 'template'
  if (parsed.actions) return 'canvas'
  return 'unknown'
}

// ── Main Deserializer ──────────────────────────────────────

/**
 * Parse a multi-document YAML string and find the flowgroup document.
 * If flowgroupName is provided, match by name. Otherwise, return the first document.
 */
function findFlowgroupDoc(
  yamlString: string,
  flowgroupName?: string,
): Record<string, unknown> | null {
  // Try multi-document parse
  const docs = parseAllDocuments(yamlString)
  if (docs.length === 0) return null

  for (const doc of docs) {
    if (doc.errors.length > 0) continue
    const parsed = doc.toJSON() as Record<string, unknown>
    if (!parsed || typeof parsed !== 'object') continue

    // If it's an array (array syntax multi-flowgroup), search inside
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        if (typeof item === 'object' && item !== null) {
          if (!flowgroupName || asString(item.flowgroup) === flowgroupName) {
            return item as Record<string, unknown>
          }
        }
      }
      continue
    }

    if (!flowgroupName || asString(parsed.flowgroup) === flowgroupName) {
      return parsed
    }
  }

  // Fallback: single-document parse
  try {
    const single = parse(yamlString) as Record<string, unknown>
    if (single && typeof single === 'object') return single
  } catch {
    // parse error
  }

  return null
}

/**
 * Deserialize a YAML string into builder state.
 *
 * @param yamlString - Raw YAML content
 * @param flowgroupName - Optional name to match in multi-doc files
 * @returns Deserialized builder state, or null if parsing fails
 */
export function deserializeFlowgroup(
  yamlString: string,
  flowgroupName?: string,
): DeserializedFlowgroup | null {
  const doc = findFlowgroupDoc(yamlString, flowgroupName)
  if (!doc) return null

  const kind = detectFlowgroupKind(doc)

  // Extract basic info
  const presets = doc.presets
  const presetsArray: string[] = Array.isArray(presets)
    ? presets.map(String)
    : presets
      ? [String(presets)]
      : []

  const basicInfo: Partial<BasicInfo> = {
    pipeline: asString(doc.pipeline),
    flowgroupName: asString(doc.flowgroup),
    presets: presetsArray,
  }

  // Extract document-level extras
  const docExtras = extractExtras(doc, KNOWN_DOC_FIELDS)

  if (kind === 'template') {
    const templateParams = (doc.template_parameters ?? {}) as Record<string, unknown>
    const templateInfo: Partial<TemplateInfo> = {
      templateName: asString(doc.use_template),
      parameters: templateParams,
    }
    return { kind, basicInfo, templateInfo, docExtras }
  }

  if (kind === 'canvas' || kind === 'unknown') {
    const rawActions = (doc.actions ?? []) as Record<string, unknown>[]
    if (!Array.isArray(rawActions)) {
      return { kind: 'canvas', basicInfo, actionConfigs: [], edges: [], docExtras }
    }

    const actionConfigs: ActionNodeConfig[] = []

    for (const action of rawActions) {
      if (!action || typeof action !== 'object') continue

      const classified = classifyAction(action)
      const { config, isYAMLMode, yamlOverride } = reverseMapAction(action, classified)
      const actionName = asString(action.name) || `action_${actionConfigs.length + 1}`
      const target = extractTarget(action, classified)
      const id = nextId()

      // Extract extras (unknown fields)
      const knownKey = `${classified.actionType}:${classified.actionSubtype}`
      const knownFields = KNOWN_ACTION_FIELDS[knownKey]
      const extras = knownFields ? extractExtras(action, knownFields) : undefined

      actionConfigs.push({
        id,
        actionName,
        actionType: classified.actionType,
        actionSubtype: classified.actionSubtype,
        target,
        config,
        isYAMLMode,
        yamlOverride,
        _extras: extras,
      })
    }

    const edges = reconstructEdges(actionConfigs)

    return { kind: 'canvas', basicInfo, actionConfigs, edges, docExtras }
  }

  return { kind, basicInfo, docExtras }
}

/**
 * Check if a YAML string contains comments.
 * Used to warn users that comments will be lost in visual mode.
 */
export function yamlHasComments(yamlString: string): boolean {
  const lines = yamlString.split('\n')
  for (const line of lines) {
    const trimmed = line.trim()
    if (trimmed.startsWith('#')) return true
    // Check for inline comments (but not inside strings)
    const hashIndex = line.indexOf(' #')
    if (hashIndex >= 0) {
      // Simple heuristic: if # appears after content and not inside quotes
      const before = line.substring(0, hashIndex)
      const singleQuotes = (before.match(/'/g) || []).length
      const doubleQuotes = (before.match(/"/g) || []).length
      if (singleQuotes % 2 === 0 && doubleQuotes % 2 === 0) return true
    }
  }
  return false
}
