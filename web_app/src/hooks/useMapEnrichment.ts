import { useMemo } from 'react'
import { useRunStore } from '../store/runStore'
import { useTables } from './useTables'
import type { TableSummary, ValidationIssue } from '../types/api'

// ── useMapEnrichment — client-side severity + FQN join (§6.7 G2) ──
//
// The dependency graph nodes carry NO severity and NO produced-table FQN
// (backend §5.1). Rather than add a backend join, this hook derives both on
// the client from the two data the shell already has: `runStore.issues` (the
// current/hydrated validation issues) and `useTables(env)` (the produced-table
// catalog). Consumers (project map nodes, explorer rows) stay dumb — they read
// a resolved `{ severity, fqn }` from here and render dots/badges/sublabels.
//
// Rules (verbatim §6.7):
//   • severity = max severity of issues matching pipeline_name === node.pipeline
//     && flowgroup_name === node.flowgroup (error > warning);
//   • a pipeline node's severity = max over its flowgroups;
//   • issues WITHOUT a flowgroup_name are Problems-only — excluded here;
//   • FQN hint = first matching table's full_name, with a `+n` suffix when the
//     (pipeline, flowgroup) produces more than one dataset.

export type MapSeverity = 'error' | 'warning'

export interface NodeEnrichment {
  severity?: MapSeverity
  fqn?: string
}

/** Node identity the enrichment join keys off (a subset of the graph node's
 * `data`). */
export interface EnrichableNode {
  nodeType?: string
  pipeline?: string
  flowgroup?: string
}

export interface MapEnrichment {
  /** Severity for a (pipeline, flowgroup); omit `flowgroup` for a pipeline-wide
   * max. Undefined when nothing matches. */
  severityFor(pipeline: string, flowgroup?: string): MapSeverity | undefined
  /** First produced-dataset FQN for a (pipeline, flowgroup) (+n), or pipeline-
   * wide when `flowgroup` is omitted. Undefined when nothing is produced. */
  fqnHintFor(pipeline: string, flowgroup?: string): string | undefined
  /** Resolve both for a graph node (routes pipeline vs flowgroup nodes). */
  enrichNode(node: EnrichableNode): NodeEnrichment
}

const NUL = '\u0000'
function fgKey(pipeline: string, flowgroup: string): string {
  return `${pipeline}${NUL}${flowgroup}`
}

/** error dominates warning. */
function maxSeverity(a: MapSeverity | undefined, b: MapSeverity): MapSeverity {
  return a === 'error' || b === 'error' ? 'error' : (a ?? b)
}

function fqnHint(tables: readonly TableSummary[]): string | undefined {
  if (tables.length === 0) return undefined
  const names = tables.map((t) => t.full_name).sort()
  return names.length > 1 ? `${names[0]} +${names.length - 1}` : names[0]
}

/** Pure join (exported for unit tests): fold issues + tables into lookup maps. */
export function buildMapEnrichment(
  issues: readonly ValidationIssue[],
  tables: readonly TableSummary[],
): MapEnrichment {
  const flowgroupSeverity = new Map<string, MapSeverity>()
  const pipelineSeverity = new Map<string, MapSeverity>()
  for (const issue of issues) {
    const pipeline = issue.pipeline_name
    const flowgroup = issue.flowgroup_name
    // Issues without a flowgroup can't be attributed to a map node → skip.
    if (!pipeline || !flowgroup) continue
    const sev: MapSeverity = issue.severity === 'error' ? 'error' : 'warning'
    const key = fgKey(pipeline, flowgroup)
    flowgroupSeverity.set(key, maxSeverity(flowgroupSeverity.get(key), sev))
    pipelineSeverity.set(pipeline, maxSeverity(pipelineSeverity.get(pipeline), sev))
  }

  const flowgroupTables = new Map<string, TableSummary[]>()
  const pipelineTables = new Map<string, TableSummary[]>()
  for (const table of tables) {
    const key = fgKey(table.pipeline, table.flowgroup)
    ;(flowgroupTables.get(key) ?? flowgroupTables.set(key, []).get(key)!).push(table)
    ;(pipelineTables.get(table.pipeline) ??
      pipelineTables.set(table.pipeline, []).get(table.pipeline)!).push(table)
  }

  const severityFor = (pipeline: string, flowgroup?: string): MapSeverity | undefined =>
    flowgroup ? flowgroupSeverity.get(fgKey(pipeline, flowgroup)) : pipelineSeverity.get(pipeline)

  const fqnHintFor = (pipeline: string, flowgroup?: string): string | undefined =>
    fqnHint(flowgroup ? (flowgroupTables.get(fgKey(pipeline, flowgroup)) ?? []) : (pipelineTables.get(pipeline) ?? []))

  return {
    severityFor,
    fqnHintFor,
    enrichNode(node) {
      const pipeline = node.pipeline ?? ''
      if (!pipeline) return {}
      // Pipeline-level nodes aggregate over their flowgroups; every other node
      // resolves against its own (pipeline, flowgroup).
      const flowgroup = node.nodeType === 'pipeline' ? undefined : node.flowgroup || undefined
      return {
        severity: severityFor(pipeline, flowgroup),
        fqn: fqnHintFor(pipeline, flowgroup),
      }
    },
  }
}

export function useMapEnrichment(env: string): MapEnrichment {
  const issues = useRunStore((s) => s.issues)
  const { data } = useTables(env)
  const tables = data?.tables
  return useMemo(() => buildMapEnrichment(issues, tables ?? []), [issues, tables])
}
