/**
 * Flowgroup document model for the Designer (typed binding over yaml-doc).
 *
 * The Designer canvas is a pure projection of a flowgroup YAML file: this
 * module parses the file (comment-preserving, via `./yaml-doc`), enumerates
 * the flowgroup documents it contains, exposes typed read accessors for one
 * selected flowgroup, derives the action graph the canvas renders, and
 * applies surgical mutations through `setPath`/`deletePath`/`insertListItem`
 * so hand-written comments, key order, and unrelated formatting survive a
 * save. Pure TypeScript: no React, no store, no API calls.
 *
 * File forms (ground truth: `src/lhp/parsers/yaml_parser.py`
 * `_flowgroups_from_documents`):
 *  - 'single': one YAML document that IS a flowgroup.
 *  - 'multi':  several `---`-separated flowgroup documents.
 *  - 'array':  one document with shared root fields plus `flowgroups: []`;
 *    the root's `pipeline`, `use_template`, `presets`,
 *    `operational_metadata`, and `job_name` inherit into each entry unless
 *    the entry overrides them (yaml_parser.py:205-217).
 *
 * Wiring semantics (ground truth:
 * `src/lhp/core/dependencies/source_extractor.py` — mirrored here so the
 * canvas needs no server round-trip and works for unsaved files):
 *  - load/transform actions produce the view named by `target`.
 *  - consumers name inputs via `source` (string, list = fan-in, or dict via
 *    the `view` / `source` / `sources` / `table`+`catalog`+`schema` keys,
 *    same precedence as extract_action_sources:84-102).
 *  - a LOAD's plain-string source is inline SQL text, not a view name
 *    (generators/load/sql.py:25-29), so it derives no edge.
 *  - write actions are terminal. mode 'cdc' prefers `cdc_config.source`;
 *    mode 'snapshot_cdc' uses `snapshot_cdc_config.source_function` (no
 *    inbound at all) or `snapshot_cdc_config.source`
 *    (source_extractor.py:28-57). A materialized_view may carry its SQL in
 *    `write_target.sql`/`sql_path` instead of `source`
 *    (validators/action/write.py:157-164) — then no inbound edge is
 *    derivable client-side.
 *  - test actions also consume `reference` (referential_integrity.py:38)
 *    and `lookup_table` (all_lookups_found.py:42) as upstream relations.
 *  - `depends_on` names are extra manual edges of kind 'depends_on'
 *    (models/_action.py:98-101).
 *  - any referenced name that matches no in-flowgroup producer becomes an
 *    external node (a table, or a view owned by another flowgroup).
 */
import type { YAMLError } from 'yaml'

import { isPlainObject, isStringArray } from './config-model'
import {
  deletePath,
  documentCount,
  insertListItem,
  parseConfigFile,
  serializeConfigFile,
  setPath,
  toJS,
} from './yaml-doc'
import type { ConfigFileHandle, YamlPath } from './yaml-doc'

export type { YamlPath } from './yaml-doc'

// ---------------------------------------------------------------------------
// File-level parse + enumeration
// ---------------------------------------------------------------------------

/** Which of the three file syntaxes a flowgroup entry comes from. */
export type FlowgroupFileForm = 'single' | 'multi' | 'array'

/** One flowgroup found in a file. */
export interface FlowgroupInfo {
  /** `flowgroup` name; '' when the document lacks a string `flowgroup` key. */
  name: string
  /** Effective pipeline (array form: inherited from the file root when absent). */
  pipeline: string | undefined
  form: FlowgroupFileForm
  /** Ordinal position among the file's flowgroups (0-based). */
  index: number
}

/**
 * Opaque handle to a parsed flowgroup file. `errors` / `warnings` /
 * `flowgroups` are parse-time snapshots; `listFlowgroups` re-enumerates
 * after mutations (e.g. a renamed flowgroup).
 */
export interface FlowgroupFileHandle {
  /** YAML parse errors (yaml-doc's shape); mutations throw while non-empty. */
  readonly errors: readonly YAMLError[]
  /**
   * File-shape warnings mirroring the CLI parser's rejections: mixed
   * `---`/array syntax (VAL_014), duplicate flowgroup names (VAL_013),
   * skipped blueprint/instance/non-mapping documents, unnamed entries.
   */
  readonly warnings: readonly string[]
  readonly flowgroups: readonly FlowgroupInfo[]
  /**
   * At least one document was skipped as a blueprint definition or
   * instance file (structured counterpart of the prose warnings above —
   * consumers must use this, not string-sniff `warnings`).
   */
  readonly blueprintLike: boolean
  /**
   * At least one document is a template definition (top-level `name` +
   * `actions`, no `flowgroup`/`flowgroups`). Templates are not enumerated as
   * flowgroups — reach their parameters + action body via `selectTemplate`.
   */
  readonly templateLike: boolean
}

/** Handle to ONE selected flowgroup within a file; all edits target it. */
export interface FlowgroupDocHandle {
  readonly file: FlowgroupFileHandle
  readonly info: FlowgroupInfo
}

/**
 * One declared template parameter (models/_template.py `parameters[]`). Every
 * field but `name` is optional; absent keys read as undefined. `default` is
 * the raw YAML value (string/number/bool/list/map) and is only present when
 * the key exists (so an explicit `default: ""` is distinguishable from none).
 */
export interface TemplateParamRead {
  name: string
  /** `string | object | array | boolean | number` when declared. */
  type?: string
  required?: boolean
  default?: unknown
  description?: string
  /** Position in the `parameters` list (mutator address). */
  index: number
  /** The parameter's raw mapping, unfiltered. */
  raw: Record<string, unknown>
}

/** Top-level template metadata (models/_template.py). */
export interface TemplateInfo {
  /** Top-level `name` ('' when missing). */
  name: string
  version?: string
  description?: string
  presets?: string[]
  /** Number of declared parameters. */
  paramCount: number
}

/**
 * Handle to a template file: its declared parameters (read via
 * `readTemplateParams`) plus an action `body` that reuses the flowgroup
 * action mutators + `deriveGraph` verbatim — a template's `actions:` wire by
 * view name exactly like a flowgroup's. Parameter edits go through the
 * template-param mutators, which target the same document's `parameters`
 * list.
 */
export interface TemplateDocHandle {
  readonly file: FlowgroupFileHandle
  readonly info: TemplateInfo
  readonly body: FlowgroupDocHandle
}

interface FileState extends FlowgroupFileHandle {
  _file: ConfigFileHandle
}

interface DocState extends FlowgroupDocHandle {
  _docIndex: number
  /** [] for single/multi form; ['flowgroups', i] for array form. */
  _basePath: YamlPath
}

interface EnumeratedFlowgroup {
  info: FlowgroupInfo
  docIndex: number
  basePath: YamlPath
}

/** Root fields the array form inherits into entries (yaml_parser.py:208-214). */
const INHERITABLE_FIELDS = [
  'pipeline',
  'use_template',
  'presets',
  'operational_metadata',
  'job_name',
] as const

/**
 * Parse a flowgroup YAML file. Never throws on YAML errors: they surface on
 * `handle.errors` (and block mutations), while whatever is enumerable is
 * still listed so a broken file can be inspected.
 */
export function parseFlowgroupFile(source: string): FlowgroupFileHandle {
  const file = parseConfigFile(source)
  const { entries, warnings, blueprintLike, templateLike } = enumerateFlowgroups(file)
  const handle: FileState = {
    errors: file.errors,
    warnings,
    flowgroups: entries.map((e) => e.info),
    blueprintLike,
    templateLike,
    _file: file,
  }
  return handle
}

/** Serialize back to YAML text (byte-identical when nothing was mutated). */
export function serializeFlowgroupFile(handle: FlowgroupFileHandle): string {
  return serializeConfigFile(fileState(handle)._file)
}

/** Re-enumerate the flowgroups (fresh, reflecting any mutations). */
export function listFlowgroups(handle: FlowgroupFileHandle): FlowgroupInfo[] {
  return enumerateFlowgroups(fileState(handle)._file).entries.map((e) => e.info)
}

/** Select a flowgroup by name (first match). Undefined when absent. */
export function selectFlowgroup(
  handle: FlowgroupFileHandle,
  name: string,
): FlowgroupDocHandle | undefined {
  const entry = enumerateFlowgroups(fileState(handle)._file).entries.find(
    (e) => e.info.name === name,
  )
  return entry === undefined ? undefined : toDocHandle(handle, entry)
}

/** Select a flowgroup by ordinal position (covers unnamed entries). */
export function selectFlowgroupAt(
  handle: FlowgroupFileHandle,
  index: number,
): FlowgroupDocHandle | undefined {
  const entry = enumerateFlowgroups(fileState(handle)._file).entries[index]
  return entry === undefined ? undefined : toDocHandle(handle, entry)
}

function toDocHandle(handle: FlowgroupFileHandle, entry: EnumeratedFlowgroup): FlowgroupDocHandle {
  const doc: DocState = {
    file: handle,
    info: entry.info,
    _docIndex: entry.docIndex,
    _basePath: entry.basePath,
  }
  return doc
}

function enumerateFlowgroups(file: ConfigFileHandle): {
  entries: EnumeratedFlowgroup[]
  warnings: string[]
  blueprintLike: boolean
  templateLike: boolean
} {
  const entries: EnumeratedFlowgroup[] = []
  const warnings: string[] = []
  const seen = new Set<string>()
  const duplicates = new Set<string>()
  let sawArray = false
  let sawDirect = false
  let blueprintLike = false
  let templateLike = false

  const docCount = documentCount(file)
  const directForm: FlowgroupFileForm = docCount > 1 ? 'multi' : 'single'

  for (let d = 0; d < docCount; d++) {
    let js: unknown
    try {
      js = toJS(file, d)
    } catch {
      warnings.push(`Document ${d + 1} could not be read (broken YAML)`)
      continue
    }
    if (js === null || js === undefined) continue
    if (!isPlainObject(js)) {
      warnings.push(`Document ${d + 1} is not a mapping; ignored`)
      continue
    }
    // Routing mirrors yaml_parser.py:167-199: blueprint definitions and
    // blueprint instance files are not editable as flowgroups.
    if (looksLikeBlueprint(js)) {
      blueprintLike = true
      warnings.push(
        `Document ${d + 1} looks like a blueprint definition (parameters + flowgroups, no actions); not editable here`,
      )
      continue
    }
    if (looksLikeInstance(js)) {
      blueprintLike = true
      warnings.push(`Document ${d + 1} is a blueprint instance file; not editable here`)
      continue
    }
    // A template definition is authored via the Parameters panel + action
    // canvas (selectTemplate), never enumerated as a flowgroup — and its
    // top-level `name` (not `flowgroup`) must not raise the missing-name
    // warning the direct-form branch below would emit.
    if (looksLikeTemplate(js)) {
      templateLike = true
      continue
    }

    if ('flowgroups' in js) {
      sawArray = true
      if (!Array.isArray(js.flowgroups)) {
        warnings.push(`Document ${d + 1}: 'flowgroups' is not a list; ignored`)
        continue
      }
      const list: unknown[] = js.flowgroups
      list.forEach((fg, j) => {
        if (!isPlainObject(fg)) {
          warnings.push(`Document ${d + 1}: flowgroups[${j}] is not a mapping; ignored`)
          return
        }
        const name = typeof fg.flowgroup === 'string' ? fg.flowgroup : ''
        if (name === '') warnings.push(`Document ${d + 1}: flowgroups[${j}] has no 'flowgroup' name`)
        const pipeline =
          typeof fg.pipeline === 'string'
            ? fg.pipeline
            : typeof js.pipeline === 'string'
              ? js.pipeline
              : undefined
        registerName(name, seen, duplicates)
        entries.push({
          info: { name, pipeline, form: 'array', index: entries.length },
          docIndex: d,
          basePath: ['flowgroups', j],
        })
      })
    } else {
      sawDirect = true
      const name = typeof js.flowgroup === 'string' ? js.flowgroup : ''
      if (name === '') warnings.push(`Document ${d + 1} has no 'flowgroup' name`)
      const pipeline = typeof js.pipeline === 'string' ? js.pipeline : undefined
      registerName(name, seen, duplicates)
      entries.push({
        info: { name, pipeline, form: directForm, index: entries.length },
        docIndex: d,
        basePath: [],
      })
    }
  }

  if (sawArray && sawDirect) {
    warnings.push(
      "Mixed flowgroup syntax: the file combines '---' documents and a 'flowgroups:' array; the CLI rejects this (VAL_014)",
    )
  }
  for (const name of duplicates) {
    warnings.push(`Duplicate flowgroup name '${name}'; the CLI rejects this (VAL_013)`)
  }
  return { entries, warnings, blueprintLike, templateLike }
}

function registerName(name: string, seen: Set<string>, duplicates: Set<string>): void {
  if (name === '') return
  if (seen.has(name)) duplicates.add(name)
  seen.add(name)
}

/** Blueprint definition shape (blueprint_parser.py:358-370). */
function looksLikeBlueprint(js: Record<string, unknown>): boolean {
  return 'parameters' in js && 'flowgroups' in js && !('actions' in js)
}

/** Blueprint instance shape (blueprint_parser.py:378-392). */
function looksLikeInstance(js: Record<string, unknown>): boolean {
  if ('use_blueprint' in js) return true
  return typeof js.blueprint === 'string' && !('flowgroups' in js) && !('actions' in js)
}

/**
 * Template definition shape (models/_template.py): a top-level `name` plus an
 * `actions` and/or `parameters` block, and NO `flowgroup`/`flowgroups`/
 * `pipeline` keys. Distinct from a flowgroup (which names itself with
 * `flowgroup`, never a top-level `name`) and from a blueprint (which carries
 * `flowgroups` and no `actions`).
 */
function looksLikeTemplate(js: Record<string, unknown>): boolean {
  return (
    typeof js.name === 'string' &&
    !('flowgroup' in js) &&
    !('flowgroups' in js) &&
    !('pipeline' in js) &&
    ('actions' in js || 'parameters' in js)
  )
}

// ---------------------------------------------------------------------------
// Read accessors
// ---------------------------------------------------------------------------

/**
 * Typed flowgroup metadata (models/_flowgroup.py fields). Values that do
 * not match the expected shape read as undefined — the raw value is still
 * reachable via `readFlowgroupValue`. `inherited` lists the fields whose
 * value came from the file root (array form only).
 */
export interface FlowgroupMeta {
  pipeline?: string
  flowgroup?: string
  job_name?: string
  variables?: Record<string, string>
  presets?: string[]
  use_template?: string
  template_parameters?: Record<string, unknown>
  operational_metadata?: boolean | string[]
  inherited: string[]
}

export function readFlowgroupMeta(doc: FlowgroupDocHandle): FlowgroupMeta {
  const d = docState(doc)
  const fg = flowgroupJs(d)
  const shared = sharedRootFields(d)
  const inherited: string[] = []
  const effective = (field: string): unknown => {
    if (field in fg) return fg[field]
    if (
      (INHERITABLE_FIELDS as readonly string[]).includes(field) &&
      shared !== undefined &&
      field in shared
    ) {
      inherited.push(field)
      return shared[field]
    }
    return undefined
  }

  const meta: FlowgroupMeta = { inherited }
  const pipeline = effective('pipeline')
  if (typeof pipeline === 'string') meta.pipeline = pipeline
  const flowgroup = effective('flowgroup')
  if (typeof flowgroup === 'string') meta.flowgroup = flowgroup
  const jobName = effective('job_name')
  if (typeof jobName === 'string') meta.job_name = jobName
  const variables = effective('variables')
  if (isStringRecord(variables)) meta.variables = variables
  const presets = effective('presets')
  if (isStringArray(presets)) meta.presets = presets
  const useTemplate = effective('use_template')
  if (typeof useTemplate === 'string') meta.use_template = useTemplate
  const templateParameters = effective('template_parameters')
  if (isPlainObject(templateParameters)) meta.template_parameters = templateParameters
  const operationalMetadata = effective('operational_metadata')
  if (typeof operationalMetadata === 'boolean' || isStringArray(operationalMetadata)) {
    meta.operational_metadata = operationalMetadata
  }
  return meta
}

/** Raw (plain-JS) value at `path` within the selected flowgroup. */
export function readFlowgroupValue(doc: FlowgroupDocHandle, path: YamlPath): unknown {
  return navigate(flowgroupJs(docState(doc)), path)
}

export type ActionKind = 'load' | 'transform' | 'write' | 'test'

const ACTION_KINDS: readonly string[] = ['load', 'transform', 'write', 'test']

/** One action, read for display/graph purposes. */
export interface ActionRead {
  /** `name` field ('' when missing). */
  name: string
  /** `type` field; undefined when missing or not a known kind. */
  kind: ActionKind | undefined
  /**
   * Sub-type with the CLI dispatch defaults applied
   * (core/codegen/action_dispatch.py:364-379): load → `source.type` or
   * 'sql' (plain-string source = 'sql'); transform → `transform_type` or
   * 'sql'; write → `write_target.type` or 'streaming_table'; test →
   * `test_type` or 'row_count'. 'unknown' for unrecognized kinds.
   */
  subType: string
  /** Output view name (loads/transforms; tests may set one too). */
  target?: string
  /** Upstream view/table names for edge derivation (see module header). */
  sources: string[]
  /** `depends_on` entries (manual extra edges). */
  dependsOn: string[]
  /** Write actions only: `write_target.mode`, default 'standard'. */
  writeMode?: string
  /** The action's plain-JS mapping, unfiltered. */
  raw: Record<string, unknown>
  /** Position in the actions list. */
  index: number
}

/** Read the selected flowgroup's actions (fresh snapshot on every call). */
export function listActions(doc: FlowgroupDocHandle): ActionRead[] {
  const fg = flowgroupJs(docState(doc))
  const actions: unknown[] = Array.isArray(fg.actions) ? fg.actions : []
  return actions.map((value, index) => {
    const raw = isPlainObject(value) ? value : {}
    const kind =
      typeof raw.type === 'string' && ACTION_KINDS.includes(raw.type)
        ? (raw.type as ActionKind)
        : undefined
    const read: ActionRead = {
      name: typeof raw.name === 'string' ? raw.name : '',
      kind,
      subType: resolveSubType(kind, raw),
      sources: extractSources(kind, raw),
      dependsOn: Array.isArray(raw.depends_on) ? stringItems(raw.depends_on) : [],
      raw,
      index,
    }
    if (typeof raw.target === 'string') read.target = raw.target
    const writeMode = resolveWriteMode(kind, raw)
    if (writeMode !== undefined) read.writeMode = writeMode
    return read
  })
}

/** Sub-type discriminators with dispatch defaults (action_dispatch.py:364-379). */
function resolveSubType(kind: ActionKind | undefined, raw: Record<string, unknown>): string {
  switch (kind) {
    case 'load': {
      const source = raw.source
      if (isPlainObject(source) && typeof source.type === 'string') return source.type
      return 'sql'
    }
    case 'transform':
      return typeof raw.transform_type === 'string' ? raw.transform_type : 'sql'
    case 'write': {
      const target = raw.write_target
      if (isPlainObject(target) && typeof target.type === 'string') return target.type
      return 'streaming_table'
    }
    case 'test':
      return typeof raw.test_type === 'string' ? raw.test_type : 'row_count'
    default:
      return 'unknown'
  }
}

/** `write_target.mode` with the generator default (streaming_table.py:57-58). */
function resolveWriteMode(
  kind: ActionKind | undefined,
  raw: Record<string, unknown>,
): string | undefined {
  if (kind !== 'write') return undefined
  const target = raw.write_target
  if (isPlainObject(target) && typeof target.mode === 'string') return target.mode
  return 'standard'
}

/**
 * Mirror of `extract_action_sources` (source_extractor.py:60-104) plus the
 * two documented deviations: a load's plain-string source is SQL text (no
 * ref), and test actions also reference `reference` / `lookup_table`.
 */
function extractSources(kind: ActionKind | undefined, raw: Record<string, unknown>): string[] {
  const sources: string[] = []
  if (kind === 'write') {
    const cdcSources = extractCdcSources(raw)
    if (cdcSources !== null) return cdcSources
  }

  const source = raw.source
  if (typeof source === 'string') {
    if (source !== '' && kind !== 'load') sources.push(source)
  } else if (Array.isArray(source)) {
    sources.push(...stringItems(source))
  } else if (isPlainObject(source)) {
    // Key precedence mirrors source_extractor.py:84-102.
    if ('view' in source) {
      if (typeof source.view === 'string') sources.push(source.view)
    } else if ('source' in source) {
      const value = source.source
      if (typeof value === 'string') sources.push(value)
      else if (Array.isArray(value)) sources.push(...stringItems(value))
    } else if ('sources' in source) {
      if (Array.isArray(source.sources)) sources.push(...stringItems(source.sources))
    } else if ('table' in source) {
      const catalog = typeof source.catalog === 'string' ? source.catalog : ''
      const schema = typeof source.schema === 'string' ? source.schema : ''
      const table = typeof source.table === 'string' ? source.table : ''
      if (catalog && schema && table) sources.push(`${catalog}.${schema}.${table}`)
      else if (table) sources.push(table)
    }
  }

  if (kind === 'test') {
    if (typeof raw.reference === 'string' && raw.reference !== '') sources.push(raw.reference)
    if (typeof raw.lookup_table === 'string' && raw.lookup_table !== '') {
      sources.push(raw.lookup_table)
    }
  }
  return sources
}

/**
 * CDC write source precedence (source_extractor.py:28-57). Returns null to
 * signal fallback to `action.source`; `[]` means self-contained (a
 * snapshot_cdc `source_function`) — no upstream reference at all.
 */
function extractCdcSources(raw: Record<string, unknown>): string[] | null {
  const target = raw.write_target
  if (!isPlainObject(target)) return null
  if (target.mode === 'cdc') {
    const config = target.cdc_config
    if (isPlainObject(config) && typeof config.source === 'string' && config.source !== '') {
      return [config.source]
    }
  } else if (target.mode === 'snapshot_cdc') {
    const config = isPlainObject(target.snapshot_cdc_config) ? target.snapshot_cdc_config : {}
    if (config.source_function) return []
    if (typeof config.source === 'string' && config.source !== '') return [config.source]
  }
  return null
}

// ---------------------------------------------------------------------------
// Graph derivation
// ---------------------------------------------------------------------------

/** One action node on the canvas. Node id = action name, `#N`-suffixed on duplicates. */
export interface GraphNode {
  id: string
  name: string
  kind: ActionKind | 'unknown'
  subType: string
  writeMode?: string
  /** Position in the actions list (pair with `listActions` for details). */
  actionIndex: number
}

/** An input that matches no in-flowgroup producer (rendered muted). */
export interface ExternalNode {
  /** `ext:` + label (cannot collide with action-name node ids). */
  id: string
  /** The referenced name as written (table or another flowgroup's view). */
  label: string
}

export type EdgeKind = 'data' | 'depends_on'

export interface GraphEdge {
  /** Source endpoint: a `GraphNode.id` or an `ExternalNode.id`. */
  from: string
  /** Target endpoint: always a `GraphNode.id` (writes/tests are terminal). */
  to: string
  /** The view/table name that induced the edge. */
  viewName: string
  kind: EdgeKind
}

export interface FlowgroupGraph {
  nodes: GraphNode[]
  edges: GraphEdge[]
  externals: ExternalNode[]
  /** Duplicate/missing action names, duplicated view producers. */
  warnings: string[]
}

/** Derive the canvas graph for the selected flowgroup (pure, no I/O). */
export function deriveGraph(doc: FlowgroupDocHandle): FlowgroupGraph {
  const actions = listActions(doc)
  const warnings: string[] = []

  const occurrences = new Map<string, number>()
  const nodes: GraphNode[] = actions.map((action) => {
    const base = action.name !== '' ? action.name : `__action_${action.index}`
    if (action.name === '') warnings.push(`Action at position ${action.index + 1} has no name`)
    const count = (occurrences.get(base) ?? 0) + 1
    occurrences.set(base, count)
    if (count === 2) {
      warnings.push(`Duplicate action name '${base}'; later occurrences get '#N' ids`)
    }
    const node: GraphNode = {
      id: count === 1 ? base : `${base}#${count}`,
      name: action.name,
      kind: action.kind ?? 'unknown',
      subType: action.subType,
      actionIndex: action.index,
    }
    if (action.writeMode !== undefined) node.writeMode = action.writeMode
    return node
  })

  // Producers: the brief's wiring rule — only loads and transforms publish
  // their `target` view for in-flowgroup consumption.
  const producers = new Map<string, string>()
  actions.forEach((action, i) => {
    if ((action.kind === 'load' || action.kind === 'transform') && action.target !== undefined) {
      if (producers.has(action.target)) {
        warnings.push(`View '${action.target}' is produced by more than one action`)
      } else {
        producers.set(action.target, nodes[i].id)
      }
    }
  })

  const edges: GraphEdge[] = []
  const edgeKeys = new Set<string>()
  const externals = new Map<string, ExternalNode>()
  const endpointFor = (ref: string): string => {
    const producer = producers.get(ref)
    if (producer !== undefined) return producer
    let external = externals.get(ref)
    if (external === undefined) {
      external = { id: `ext:${ref}`, label: ref }
      externals.set(ref, external)
    }
    return external.id
  }
  const addEdge = (from: string, to: string, viewName: string, kind: EdgeKind): void => {
    const key = `${from}@${to}@${viewName}@${kind}`
    if (edgeKeys.has(key)) return
    edgeKeys.add(key)
    edges.push({ from, to, viewName, kind })
  }

  actions.forEach((action, i) => {
    for (const ref of action.sources) addEdge(endpointFor(ref), nodes[i].id, ref, 'data')
    for (const ref of action.dependsOn) {
      addEdge(endpointFor(ref), nodes[i].id, ref, 'depends_on')
    }
  })

  return { nodes, edges, externals: [...externals.values()], warnings }
}

// ---------------------------------------------------------------------------
// Mutators (all comment-preserving via yaml-doc; serialize to save)
// ---------------------------------------------------------------------------

/**
 * Set a field of the selected flowgroup (e.g. `['job_name']`,
 * `['template_parameters', 'schema_file']`). In array form this writes to
 * the entry, which per the parser's precedence overrides any inherited
 * root value. Path must be non-empty.
 */
export function setFlowgroupField(doc: FlowgroupDocHandle, path: YamlPath, value: unknown): void {
  const d = docState(doc)
  assertNonEmptyPath(path)
  setPath(fileOf(d), d._docIndex, [...d._basePath, ...path], value)
}

/** Delete a field of the selected flowgroup (missing path = no-op). */
export function deleteFlowgroupField(doc: FlowgroupDocHandle, path: YamlPath): void {
  const d = docState(doc)
  assertNonEmptyPath(path)
  deletePath(fileOf(d), d._docIndex, [...d._basePath, ...path])
}

/**
 * Set a field of one action, addressed by name (`'x#2'` addresses the
 * second action named `x`, matching `deriveGraph` node ids). Throws when
 * the action does not exist.
 */
export function setActionField(
  doc: FlowgroupDocHandle,
  actionName: string,
  path: YamlPath,
  value: unknown,
): void {
  const d = docState(doc)
  assertNonEmptyPath(path)
  const index = resolveActionIndex(d, actionName)
  setPath(fileOf(d), d._docIndex, [...d._basePath, 'actions', index, ...path], value)
}

/** Delete a field of one action (missing path = no-op; missing action throws). */
export function deleteActionField(
  doc: FlowgroupDocHandle,
  actionName: string,
  path: YamlPath,
): void {
  const d = docState(doc)
  assertNonEmptyPath(path)
  const index = resolveActionIndex(d, actionName)
  deletePath(fileOf(d), d._docIndex, [...d._basePath, 'actions', index, ...path])
}

/**
 * Insert a new action, after `afterActionName` when given, else appended.
 * Creates the `actions` list when missing. The inserted subtree carries no
 * comments; siblings keep theirs (the containing document is rewritten —
 * see yaml-doc's pinned rewrite caveats).
 */
export function addAction(
  doc: FlowgroupDocHandle,
  action: Record<string, unknown>,
  afterActionName?: string,
): void {
  const d = docState(doc)
  const fg = flowgroupJs(d)
  if (Array.isArray(fg.actions)) {
    const index =
      afterActionName === undefined
        ? fg.actions.length
        : resolveActionIndex(d, afterActionName) + 1
    insertListItem(fileOf(d), d._docIndex, [...d._basePath, 'actions'], index, action)
  } else {
    if (afterActionName !== undefined) {
      throw new Error(`Action '${afterActionName}' not found in flowgroup '${d.info.name}'`)
    }
    setPath(fileOf(d), d._docIndex, [...d._basePath, 'actions'], [action])
  }
}

/** Delete one action by name (its own comments go with it). */
export function deleteAction(doc: FlowgroupDocHandle, actionName: string): void {
  const d = docState(doc)
  const index = resolveActionIndex(d, actionName)
  deletePath(fileOf(d), d._docIndex, [...d._basePath, 'actions', index])
}

/**
 * Deep-copy one action directly after the original under a deterministic
 * unique name (`<name>_copy`, `<name>_copy2`, ...). Returns the new name.
 * The copy carries values only, not the original's comments.
 */
export function duplicateAction(doc: FlowgroupDocHandle, actionName: string): string {
  const d = docState(doc)
  const index = resolveActionIndex(d, actionName)
  const actions = listActions(doc)
  const original = actions[index].raw
  const copy = structuredClone(original)
  const names = new Set(actions.map((a) => a.name))
  const base = actions[index].name !== '' ? actions[index].name : 'action'
  let candidate = `${base}_copy`
  for (let n = 2; names.has(candidate); n++) candidate = `${base}_copy${n}`
  copy.name = candidate
  insertListItem(fileOf(d), d._docIndex, [...d._basePath, 'actions'], index + 1, copy)
  return candidate
}

/** Rename one action (surgical single-scalar edit). Throws on name clash. */
export function renameAction(doc: FlowgroupDocHandle, oldName: string, newName: string): void {
  const d = docState(doc)
  if (newName === '') throw new Error('Action name cannot be empty')
  const index = resolveActionIndex(d, oldName)
  const clash = listActions(doc).some((a) => a.name === newName && a.index !== index)
  if (clash) throw new Error(`An action named '${newName}' already exists`)
  setPath(fileOf(d), d._docIndex, [...d._basePath, 'actions', index, 'name'], newName)
}

/**
 * Append an upstream view to an action's `source` — the fan-in primitive
 * (req 5). A single-string source is converted to a two-element list
 * (`[existing, viewName]`); a list source has `viewName` appended surgically
 * (siblings keep their formatting); an absent/empty source is set to the bare
 * string. Closes the Task-3 stringOrList non-conversion gap for exactly this
 * case. No-op when `viewName` is already an input. Throws when `source` is a
 * structured mapping (a load's typed source carries no view list to extend)
 * — the UI only offers "add input" on string/list sources. Addressed by canvas
 * node id (so duplicate/unnamed actions resolve like every other mutator).
 */
export function appendActionSource(
  doc: FlowgroupDocHandle,
  actionName: string,
  viewName: string,
): void {
  const d = docState(doc)
  const index = resolveActionIndex(d, actionName)
  const source = listActions(doc)[index]?.raw.source
  const path: YamlPath = [...d._basePath, 'actions', index, 'source']
  if (isPlainObject(source)) {
    throw new Error('Cannot add an input to a structured source; edit it in the form.')
  }
  if (Array.isArray(source)) {
    if (source.includes(viewName)) return
    insertListItem(fileOf(d), d._docIndex, path, source.length, viewName)
    return
  }
  if (typeof source === 'string' && source !== '') {
    if (source === viewName) return
    setPath(fileOf(d), d._docIndex, path, [source, viewName])
    return
  }
  setPath(fileOf(d), d._docIndex, path, viewName)
}

/**
 * Append `value` to an action's list field, resolving the insert position from
 * the freshly-parsed `doc` — not a render-time snapshot — so two appends
 * enqueued within one write window each land at the list's real current length
 * (rather than colliding on a stale index and dropping one). Creates the list
 * (`[value]`) when the key is absent or not a list. Addressed by canvas node id
 * like every other action mutator.
 */
export function appendActionListItem(
  doc: FlowgroupDocHandle,
  actionName: string,
  path: YamlPath,
  value: unknown,
): void {
  const d = docState(doc)
  assertNonEmptyPath(path)
  const index = resolveActionIndex(d, actionName)
  const current = navigate(listActions(doc)[index]?.raw ?? {}, path)
  const listPath: YamlPath = [...d._basePath, 'actions', index, ...path]
  if (Array.isArray(current)) {
    insertListItem(fileOf(d), d._docIndex, listPath, current.length, value)
  } else {
    setPath(fileOf(d), d._docIndex, listPath, [value])
  }
}

/**
 * Remove item `index` from an action's list field, deciding delete-on-clear
 * from the freshly-parsed `doc`: when the list holds one item or fewer,
 * removing it deletes the whole key (no orphan `[]`) unless `allowEmpty`. The
 * length guard is read from the doc, not a render-time snapshot, so a second
 * remove enqueued within one write window still clears the key rather than
 * leaving an empty list. The render-time `index` still names the row the user
 * clicked (the UI gates concurrent removes so a shifted index cannot slip in).
 */
export function removeActionListItem(
  doc: FlowgroupDocHandle,
  actionName: string,
  path: YamlPath,
  index: number,
  allowEmpty = false,
): void {
  const d = docState(doc)
  assertNonEmptyPath(path)
  const actionIndex = resolveActionIndex(d, actionName)
  const current = navigate(listActions(doc)[actionIndex]?.raw ?? {}, path)
  const length = Array.isArray(current) ? current.length : 0
  const listPath: YamlPath = [...d._basePath, 'actions', actionIndex, ...path]
  if (length <= 1 && !allowEmpty) {
    deletePath(fileOf(d), d._docIndex, listPath)
  } else {
    deletePath(fileOf(d), d._docIndex, [...listPath, index])
  }
}

// ---------------------------------------------------------------------------
// Templates (parametrized flowgroups under templates/)
// ---------------------------------------------------------------------------

/**
 * Select the template defined in this file (the first template document).
 * Returns undefined when the file holds no template. The returned `body` is a
 * flowgroup-doc handle over the template's root document, so every action
 * accessor/mutator/graph function in this module works on it unchanged.
 */
export function selectTemplate(handle: FlowgroupFileHandle): TemplateDocHandle | undefined {
  const file = fileState(handle)._file
  const docCount = documentCount(file)
  for (let d = 0; d < docCount; d++) {
    let js: unknown
    try {
      js = toJS(file, d)
    } catch {
      continue
    }
    if (!isPlainObject(js) || !looksLikeTemplate(js)) continue
    const name = typeof js.name === 'string' ? js.name : ''
    const info: TemplateInfo = {
      name,
      paramCount: Array.isArray(js.parameters) ? js.parameters.length : 0,
    }
    if (js.version !== undefined && js.version !== null) info.version = String(js.version)
    if (typeof js.description === 'string') info.description = js.description
    if (isStringArray(js.presets)) info.presets = js.presets
    const body: DocState = {
      file: handle,
      info: { name, pipeline: undefined, form: 'single', index: 0 },
      _docIndex: d,
      _basePath: [],
    }
    return { file: handle, info, body }
  }
  return undefined
}

/** Read the template's declared parameters (fresh snapshot on every call). */
export function readTemplateParams(t: TemplateDocHandle): TemplateParamRead[] {
  const params = readFlowgroupValue(t.body, ['parameters'])
  const list: unknown[] = Array.isArray(params) ? params : []
  return list.map((value, index) => {
    const raw = isPlainObject(value) ? value : {}
    const param: TemplateParamRead = {
      name: typeof raw.name === 'string' ? raw.name : '',
      index,
      raw,
    }
    if (typeof raw.type === 'string') param.type = raw.type
    if (typeof raw.required === 'boolean') param.required = raw.required
    if ('default' in raw) param.default = raw.default
    if (typeof raw.description === 'string') param.description = raw.description
    return param
  })
}

/**
 * Append a new parameter to the template's `parameters` list (creating the
 * list when absent). Comment-preserving like every other mutator. Addressed
 * through the template `body` so it composes with a `commit` over that body.
 */
export function addTemplateParam(body: FlowgroupDocHandle, param: Record<string, unknown>): void {
  const d = docState(body)
  const params = flowgroupJs(d).parameters
  const path: YamlPath = [...d._basePath, 'parameters']
  if (Array.isArray(params)) {
    insertListItem(fileOf(d), d._docIndex, path, params.length, param)
  } else {
    setPath(fileOf(d), d._docIndex, path, [param])
  }
}

/** Delete the parameter at `index` (its own comments go with it). */
export function deleteTemplateParam(body: FlowgroupDocHandle, index: number): void {
  deleteFlowgroupField(body, ['parameters', index])
}

/** Set a field of one parameter (e.g. `['type']`, `['default']`). */
export function setTemplateParamField(
  body: FlowgroupDocHandle,
  index: number,
  path: YamlPath,
  value: unknown,
): void {
  setFlowgroupField(body, ['parameters', index, ...path], value)
}

/** Delete a field of one parameter (missing path = no-op). */
export function deleteTemplateParamField(
  body: FlowgroupDocHandle,
  index: number,
  path: YamlPath,
): void {
  deleteFlowgroupField(body, ['parameters', index, ...path])
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

function fileState(handle: FlowgroupFileHandle): FileState {
  return handle as FileState
}

function docState(doc: FlowgroupDocHandle): DocState {
  return doc as DocState
}

function fileOf(d: DocState): ConfigFileHandle {
  return fileState(d.file)._file
}

function assertNonEmptyPath(path: YamlPath): void {
  if (path.length === 0) throw new Error('Field path cannot be empty')
}

/** Plain-JS snapshot of the selected flowgroup's mapping ({} if broken). */
function flowgroupJs(d: DocState): Record<string, unknown> {
  const js = navigate(toJS(fileOf(d), d._docIndex), d._basePath)
  return isPlainObject(js) ? js : {}
}

/** Root fields an array-form entry can inherit from (undefined otherwise). */
function sharedRootFields(d: DocState): Record<string, unknown> | undefined {
  if (d._basePath.length === 0) return undefined
  const root = toJS(fileOf(d), d._docIndex)
  return isPlainObject(root) ? root : undefined
}

function navigate(js: unknown, path: YamlPath): unknown {
  let current = js
  for (const segment of path) {
    if (typeof segment === 'number') {
      current = Array.isArray(current) ? current[segment] : undefined
    } else {
      current = isPlainObject(current) ? current[segment] : undefined
    }
  }
  return current
}

/**
 * Resolve a mutator's action name to a list index. Exact `name` matches
 * win (first occurrence); otherwise a trailing `#N` addresses the Nth
 * occurrence of the base name, and the synthetic `__action_<index>` id
 * `deriveGraph` mints for an unnamed action resolves straight to that list
 * index, so `deriveGraph` node ids are always valid addresses. Throws when
 * nothing matches.
 */
function resolveActionIndex(d: DocState, name: string): number {
  const actions = listActions(d)
  const exact = actions.find((a) => a.name === name)
  if (exact !== undefined) return exact.index
  const suffixed = /^(.+)#([1-9]\d*)$/.exec(name)
  if (suffixed !== null) {
    const matches = actions.filter((a) => a.name === suffixed[1])
    const nth = Number(suffixed[2])
    if (nth <= matches.length) return matches[nth - 1].index
  }
  const synthetic = /^__action_(\d+)$/.exec(name)
  if (synthetic !== null) {
    const idx = Number(synthetic[1])
    if (idx < actions.length) return idx
  }
  throw new Error(`Action '${name}' not found in flowgroup '${d.info.name}'`)
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return isPlainObject(value) && Object.values(value).every((v) => typeof v === 'string')
}

/** The string elements of a mixed-type YAML list (mirrors the Python filters). */
function stringItems(items: unknown[]): string[] {
  return items.filter((item): item is string => typeof item === 'string')
}
