import { useMemo } from 'react'
import { stringify } from 'yaml'
import { useBuilderStore } from './useBuilderStore'
import type { ActionNodeConfig, BuilderNode } from '../types/builder'

// ── Helpers ───────────────────────────────────────────────

/**
 * Recursively strip undefined, null, empty strings, empty arrays,
 * and empty objects from an object so optional YAML fields only
 * appear when non-empty.
 */
export function stripEmpty(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(obj)) {
    if (value === undefined || value === null || value === '') continue
    if (Array.isArray(value) && value.length === 0) continue
    if (typeof value === 'object' && !Array.isArray(value)) {
      const cleaned = stripEmpty(value as Record<string, unknown>)
      if (Object.keys(cleaned).length === 0) continue
      result[key] = cleaned
    } else {
      result[key] = value
    }
  }
  return result
}

/** Topologically sort nodes based on edges (sources first, then targets). */
function topoSort(nodes: BuilderNode[], edges: { source: string; target: string }[]): BuilderNode[] {
  const inDegree = new Map<string, number>()
  const adjacency = new Map<string, string[]>()

  for (const node of nodes) {
    inDegree.set(node.id, 0)
    adjacency.set(node.id, [])
  }

  for (const edge of edges) {
    inDegree.set(edge.target, (inDegree.get(edge.target) ?? 0) + 1)
    adjacency.get(edge.source)?.push(edge.target)
  }

  const queue: string[] = []
  for (const [id, deg] of inDegree) {
    if (deg === 0) queue.push(id)
  }

  const sorted: string[] = []
  while (queue.length > 0) {
    const id = queue.shift()!
    sorted.push(id)
    for (const neighbor of adjacency.get(id) ?? []) {
      const newDeg = (inDegree.get(neighbor) ?? 1) - 1
      inDegree.set(neighbor, newDeg)
      if (newDeg === 0) queue.push(neighbor)
    }
  }

  // If there are cycles, append remaining nodes
  for (const node of nodes) {
    if (!sorted.includes(node.id)) sorted.push(node.id)
  }

  const nodeMap = new Map(nodes.map((n) => [n.id, n]))
  return sorted.map((id) => nodeMap.get(id)!).filter(Boolean)
}

/** Get the upstream target view name for a node based on incoming edges. */
function getSourceFromEdges(
  nodeId: string,
  edges: { source: string; target: string }[],
  configs: Map<string, ActionNodeConfig>,
): string | undefined {
  const incoming = edges.filter((e) => e.target === nodeId)
  if (incoming.length === 0) return undefined
  const sourceConfig = configs.get(incoming[0].source)
  return sourceConfig?.target || sourceConfig?.actionName
}

/** Get multiple upstream sources for nodes that support multi-source (e.g. python transform). */
function getMultiSourceFromEdges(
  nodeId: string,
  edges: { source: string; target: string }[],
  configs: Map<string, ActionNodeConfig>,
): string[] {
  const incoming = edges.filter((e) => e.target === nodeId)
  return incoming
    .map((e) => {
      const sourceConfig = configs.get(e.source)
      return sourceConfig?.target || sourceConfig?.actionName || ''
    })
    .filter(Boolean)
}

// ── Mappers ───────────────────────────────────────────────

function mapCloudFilesLoad(config: Record<string, unknown>, target?: string): Record<string, unknown> {
  const opts: Record<string, unknown> = {}
  const readerOpts = config.reader_options as Record<string, string> | undefined
  if (readerOpts) Object.assign(opts, readerOpts)
  if (config.schema_hints) opts['cloudFiles.schemaHints'] = config.schema_hints
  if (config.max_files_per_trigger) opts['cloudFiles.maxFilesPerTrigger'] = Number(config.max_files_per_trigger)
  if (config.schema_evolution_mode && String(config.schema_evolution_mode).trim())
    opts['cloudFiles.schemaEvolutionMode'] = config.schema_evolution_mode
  if (config.infer_column_types === false) opts['cloudFiles.inferColumnTypes'] = false
  if (config.rescued_data_column) opts['cloudFiles.rescuedDataColumn'] = config.rescued_data_column

  const source: Record<string, unknown> = {
    type: 'cloudfiles',
    path: config.source_path,
    format: config.file_format,
    options: Object.keys(opts).length > 0 ? opts : undefined,
  }

  const opMeta = config.operational_metadata as string[] | undefined

  return stripEmpty({
    type: 'load',
    readMode: config.read_mode && config.read_mode !== 'stream' ? config.read_mode : undefined,
    operational_metadata: opMeta?.length ? opMeta : undefined,
    source,
    target,
    description: config.description,
  })
}

function mapDeltaLoad(config: Record<string, unknown>, target?: string): Record<string, unknown> {
  const opts: Record<string, unknown> = {}
  if (config.readChangeFeed === true) opts.readChangeFeed = true
  if (config.startingVersion) opts.startingVersion = config.startingVersion
  if (config.startingTimestamp) opts.startingTimestamp = config.startingTimestamp
  if (config.versionAsOf) opts.versionAsOf = config.versionAsOf
  if (config.timestampAsOf) opts.timestampAsOf = config.timestampAsOf
  if (config.ignoreDeletes === true) opts.ignoreDeletes = true
  if (config.skipChangeCommits === true) opts.skipChangeCommits = true
  if (config.maxFilesPerTrigger) opts.maxFilesPerTrigger = Number(config.maxFilesPerTrigger)

  const source: Record<string, unknown> = {
    type: 'delta',
    database: config.database,
    table: config.source_table,
    options: Object.keys(opts).length > 0 ? opts : undefined,
  }

  const opMeta = config.operational_metadata as string[] | undefined

  return stripEmpty({
    type: 'load',
    readMode: config.read_mode,
    operational_metadata: opMeta?.length ? opMeta : undefined,
    source,
    target,
    where_clause: config.where_clause,
    select_columns: (config.select_columns as string[])?.length ? config.select_columns : undefined,
    description: config.description,
  })
}

function mapSQLLoad(config: Record<string, unknown>, target?: string): Record<string, unknown> {
  const source: Record<string, unknown> = {
    type: 'sql',
    sql: config.sql || undefined,
    sql_path: config.sql_file || undefined,
  }

  return stripEmpty({
    type: 'load',
    readMode: config.read_mode || 'batch',
    source,
    target,
    description: config.description,
  })
}

function mapPythonLoad(config: Record<string, unknown>, target?: string): Record<string, unknown> {
  const params = config.parameters as Record<string, string> | undefined
  const source: Record<string, unknown> = {
    type: 'python',
    module_path: config.module_path,
    function_name: config.function_name || 'get_df',
    parameters: params && Object.keys(params).length > 0 ? params : undefined,
  }

  const opMeta = config.operational_metadata as string[] | undefined

  return stripEmpty({
    type: 'load',
    readMode: config.read_mode,
    operational_metadata: opMeta?.length ? opMeta : undefined,
    source,
    target,
    description: config.description,
  })
}

function mapCustomDataSourceLoad(config: Record<string, unknown>, target?: string): Record<string, unknown> {
  const opts = config.options as Record<string, string> | undefined
  const source: Record<string, unknown> = {
    type: 'custom_datasource',
    module_path: config.module_path,
    custom_datasource_class: config.custom_datasource_class,
    options: opts && Object.keys(opts).length > 0 ? opts : undefined,
  }

  const opMeta = config.operational_metadata as string[] | undefined

  return stripEmpty({
    type: 'load',
    readMode: config.read_mode,
    operational_metadata: opMeta?.length ? opMeta : undefined,
    source,
    target,
    description: config.description,
  })
}

function mapSQLTransform(config: Record<string, unknown>, source?: string, target?: string): Record<string, unknown> {
  return stripEmpty({
    type: 'transform',
    transform_type: 'sql',
    source: config.source ?? source,
    target,
    sql: config.sql,
    sql_path: config.sql_file,
    readMode: config.read_mode,
    description: config.description,
  })
}

function mapPythonTransform(config: Record<string, unknown>, source?: string, target?: string): Record<string, unknown> {
  const params = config.parameters as Record<string, string> | undefined
  const opMeta = config.operational_metadata as string[] | undefined
  // Multi-source: use config.sources (string[]) if provided, else fall back to single source
  const sources = config.sources as string[] | undefined
  const resolvedSource = sources?.length
    ? (sources.length === 1 ? sources[0] : sources)
    : (config.source ?? source)

  return stripEmpty({
    type: 'transform',
    transform_type: 'python',
    source: resolvedSource,
    target,
    module_path: config.module_path,
    function_name: config.function_name,
    parameters: params && Object.keys(params).length > 0 ? params : undefined,
    readMode: config.read_mode,
    operational_metadata: opMeta?.length ? opMeta : undefined,
    description: config.description,
  })
}

function mapDataQualityTransform(config: Record<string, unknown>, source?: string, target?: string): Record<string, unknown> {
  return stripEmpty({
    type: 'transform',
    transform_type: 'data_quality',
    source: config.source ?? source,
    target,
    expectations_file: config.expectations_file,
    readMode: config.read_mode || 'stream',
    description: config.description,
  })
}

function mapTempTableTransform(config: Record<string, unknown>, source?: string, target?: string): Record<string, unknown> {
  return stripEmpty({
    type: 'transform',
    transform_type: 'temp_table',
    source: config.source ?? source,
    target,
    sql: config.sql,
    readMode: config.read_mode,
    description: config.description,
  })
}

function mapSchemaTransform(config: Record<string, unknown>, source?: string, target?: string): Record<string, unknown> {
  return stripEmpty({
    type: 'transform',
    transform_type: 'schema',
    source: config.source ?? source,
    target,
    schema_file: config.schema_file,
    schema_inline: config.schema_inline,
    enforcement: config.enforcement,
    readMode: config.read_mode,
    description: config.description,
  })
}

function mapStreamingTableWrite(config: Record<string, unknown>, source?: string): Record<string, unknown> {
  const cdcConfig: Record<string, unknown> = {}
  if (config.cdc_mode === 'cdc' || config.cdc_mode === 'snapshot_cdc') {
    cdcConfig.keys = config.cdc_keys
    cdcConfig.sequence_by = config.sequence_by
    cdcConfig.scd_type = config.scd_type ?? 1
    if (config.ignore_null_updates === true) cdcConfig.ignore_null_updates = true
    if (config.apply_as_deletes) cdcConfig.apply_as_deletes = config.apply_as_deletes
    if (config.apply_as_truncates) cdcConfig.apply_as_truncates = config.apply_as_truncates
    const trackHistCols = config.track_history_column_list as string[] | undefined
    if (trackHistCols?.length) cdcConfig.track_history_column_list = trackHistCols
    const trackHistExceptCols = config.track_history_except_column_list as string[] | undefined
    if (trackHistExceptCols?.length) cdcConfig.track_history_except_column_list = trackHistExceptCols
    const exceptCols = config.except_column_list as string[] | undefined
    if (exceptCols?.length) cdcConfig.except_column_list = exceptCols
  }

  const partCols = config.partition_columns as string[] | undefined
  const clusterCols = config.cluster_columns as string[] | undefined
  const tblProps = config.table_properties as Record<string, string> | undefined
  const sparkConf = config.spark_conf as Record<string, string> | undefined

  const writeTarget: Record<string, unknown> = {
    type: 'streaming_table',
    database: config.database,
    table: config.table,
    comment: config.comment,
    partition_columns: partCols?.length ? partCols : undefined,
    cluster_columns: clusterCols?.length ? clusterCols : undefined,
    table_properties: tblProps && Object.keys(tblProps).length > 0 ? tblProps : undefined,
    mode: config.cdc_mode === 'cdc' ? 'cdc' : config.cdc_mode === 'snapshot_cdc' ? 'snapshot_cdc' : undefined,
    cdc_config: (config.cdc_mode === 'cdc' || config.cdc_mode === 'snapshot_cdc') ? stripEmpty(cdcConfig) : undefined,
  }

  return stripEmpty({
    type: 'write',
    source: config.source ?? source,
    readMode: config.read_mode,
    once: config.once === true ? true : undefined,
    create_table: config.create_table === false ? false : undefined,
    table_schema: config.table_schema,
    row_filter: config.row_filter,
    spark_conf: sparkConf && Object.keys(sparkConf).length > 0 ? sparkConf : undefined,
    write_target: stripEmpty(writeTarget),
    description: config.description,
  })
}

function mapMaterializedViewWrite(config: Record<string, unknown>, source?: string): Record<string, unknown> {
  const clusterCols = config.cluster_columns as string[] | undefined
  const partCols = config.partition_columns as string[] | undefined
  const tblProps = config.table_properties as Record<string, string> | undefined

  // wt_sql / wt_sql_file are used for write_target-level SQL to avoid collision
  // with action-level sql/sql_file
  const writeTarget: Record<string, unknown> = {
    type: 'materialized_view',
    database: config.database,
    table: config.table,
    sql: config.wt_sql || undefined,
    sql_path: config.wt_sql_file || undefined,
    comment: config.comment,
    partition_columns: partCols?.length ? partCols : undefined,
    cluster_columns: clusterCols?.length ? clusterCols : undefined,
    table_properties: tblProps && Object.keys(tblProps).length > 0 ? tblProps : undefined,
  }

  // When write_target has sql/sql_path, source is not needed
  const hasSqlInWriteTarget = !!(config.wt_sql || config.wt_sql_file)
  const resolvedSource = hasSqlInWriteTarget ? undefined : (config.source ?? source)

  return stripEmpty({
    type: 'write',
    source: resolvedSource,
    table_schema: config.table_schema,
    row_filter: config.row_filter,
    write_target: stripEmpty(writeTarget),
    description: config.description,
  })
}

// ── Action Dispatcher ─────────────────────────────────────

function mapAction(
  actionConfig: ActionNodeConfig,
  source?: string,
  multiSources?: string[],
): Record<string, unknown> {
  const key = `${actionConfig.actionType}:${actionConfig.actionSubtype}`

  // If user edited raw YAML, use that
  if (actionConfig.isYAMLMode && actionConfig.yamlOverride) {
    try {
      return { _raw_yaml: actionConfig.yamlOverride }
    } catch {
      // fall through
    }
  }

  let mapped: Record<string, unknown>
  switch (key) {
    // Load actions
    case 'load:cloudfiles':
      mapped = mapCloudFilesLoad(actionConfig.config, actionConfig.target)
      break
    case 'load:delta':
      mapped = mapDeltaLoad(actionConfig.config, actionConfig.target)
      break
    case 'load:sql':
      mapped = mapSQLLoad(actionConfig.config, actionConfig.target)
      break
    case 'load:python':
      mapped = mapPythonLoad(actionConfig.config, actionConfig.target)
      break
    case 'load:custom_datasource':
      mapped = mapCustomDataSourceLoad(actionConfig.config, actionConfig.target)
      break

    // Transform actions
    case 'transform:sql_transform':
      mapped = mapSQLTransform(actionConfig.config, source, actionConfig.target)
      break
    case 'transform:python_transform': {
      // For python transforms, inject multi-source if available
      const configWithSources = { ...actionConfig.config }
      if (multiSources?.length && !configWithSources.sources) {
        configWithSources.sources = multiSources
      }
      mapped = mapPythonTransform(configWithSources, source, actionConfig.target)
      break
    }
    case 'transform:data_quality':
      mapped = mapDataQualityTransform(actionConfig.config, source, actionConfig.target)
      break
    case 'transform:temp_table':
      mapped = mapTempTableTransform(actionConfig.config, source, actionConfig.target)
      break
    case 'transform:schema':
      mapped = mapSchemaTransform(actionConfig.config, source, actionConfig.target)
      break

    // Write actions
    case 'write:streaming_table':
      mapped = mapStreamingTableWrite(actionConfig.config, source)
      break
    case 'write:materialized_view':
      mapped = mapMaterializedViewWrite(actionConfig.config, source)
      break

    default:
      // Generic: merge type info with config
      mapped = {
        type: actionConfig.actionType,
        ...actionConfig.config,
      }
      if (source && !mapped.source) mapped.source = source
      break
  }

  // Merge _extras back to preserve unknown fields from deserialization
  const extras = actionConfig._extras
  if (extras && Object.keys(extras).length > 0) {
    Object.assign(mapped, extras)
  }

  return { name: actionConfig.actionName, ...mapped }
}

// ── Hook ──────────────────────────────────────────────────

export function useYAMLGenerator(): string {
  const { basicInfo, chosenPath, templateInfo, nodes, edges, actionConfigs } =
    useBuilderStore()

  return useMemo(() => {
    const activePipeline = basicInfo.isNewPipeline ? basicInfo.newPipeline : basicInfo.pipeline
    if (!activePipeline || !basicInfo.flowgroupName) return ''

    const doc: Record<string, unknown> = {
      pipeline: activePipeline,
      flowgroup: basicInfo.flowgroupName,
    }

    if (basicInfo.presets.length > 0) {
      doc.presets = basicInfo.presets
    }

    if (chosenPath === 'template') {
      // Template path
      if (templateInfo.templateName) {
        doc.use_template = templateInfo.templateName

        // Filter out empty/default parameters
        const params: Record<string, unknown> = {}
        for (const [key, value] of Object.entries(templateInfo.parameters)) {
          if (value === '' || value === undefined || value === null) continue
          if (Array.isArray(value) && value.length === 0) continue
          if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value as object).length === 0) continue
          params[key] = value
        }
        if (Object.keys(params).length > 0) {
          doc.template_parameters = params
        }
      }
    } else if (chosenPath === 'canvas') {
      // Canvas path: build actions from nodes
      const sortedNodes = topoSort(nodes, edges)
      const actions: Record<string, unknown>[] = []

      for (const node of sortedNodes) {
        const config = actionConfigs.get(node.id)
        if (!config) continue

        const source = getSourceFromEdges(node.id, edges, actionConfigs)
        const multiSources = getMultiSourceFromEdges(node.id, edges, actionConfigs)
        const action = mapAction(config, source, multiSources)
        actions.push(action)
      }

      if (actions.length > 0) {
        doc.actions = actions
      }
    }

    return stringify(doc, { indent: 2 })
  }, [basicInfo, chosenPath, templateInfo, nodes, edges, actionConfigs])
}
