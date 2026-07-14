import { Fragment, useEffect, useMemo, useRef, type ReactNode } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  ArrowRight,
  ArrowRightToLine,
  ChevronRight,
  Cloud,
  Database,
  DownloadCloud,
  GitFork,
  Info,
  Map as MapIcon,
  RefreshCw,
  Table2,
  TriangleAlert,
  Wand2,
  Workflow,
  type LucideIcon,
} from 'lucide-react'
import { useLineage } from '../../../hooks/useLineage'
import { useGraphStaleness } from '../../../hooks/useGraphStaleness'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { ApiError } from '../../../api/client'
import { EmptyState } from '../../common/EmptyState'
import { Button } from '../../ui/button'
import type { DatasetConsumer, LineageEdge, LineageNode } from '../../../types/api'
import { cn } from '@/lib/utils'

// ── TableDetailView — produced-dataset detail center view (D6 / §6.7) ──
//
// The "find a table → see how it is loaded" surface: header identity, a
// horizontal lineage rail (external/dataset sources → load/transform actions →
// the focus table, then downstream consumers dimmed), the producing
// flowgroup/action with a jump-to-flowgroup, the columns-unavailable stub, and
// the consumers list. Data comes from GET /api/lineage via useLineage; 404
// (unknown fqn — incl. delta-sink lookups by underlying table name) and 400
// (unknown env) render as friendly states. Warnings are PROJECT-WIDE
// index-health notices (rendered as such, not as problems with this table);
// `stale` reuses the dependency graph's serve-stale refresh affordance.

/** Rail-card presentation per lineage-node kind. */
interface KindMeta {
  label: string
  sub: string
  icon: LucideIcon
  text: string
}

function kindMeta(node: LineageNode, focus: boolean, datasetKind: string): KindMeta {
  if (focus) {
    return {
      label: datasetKind === 'sink' ? 'Sink' : 'Table',
      sub: 'this dataset',
      icon: datasetKind === 'sink' ? ArrowRightToLine : Table2,
      text: 'text-kind-write',
    }
  }
  switch (node.kind) {
    case 'external':
      return { label: 'Source', sub: 'external source', icon: Cloud, text: 'text-faint' }
    case 'dataset':
      return { label: 'Table', sub: 'upstream', icon: Database, text: 'text-muted-foreground' }
    case 'load':
      return { label: 'Load', sub: 'load', icon: DownloadCloud, text: 'text-kind-load' }
    case 'transform':
      return { label: 'Transform', sub: 'transform', icon: Wand2, text: 'text-kind-transform' }
    case 'write':
      return { label: 'Write', sub: 'write', icon: Table2, text: 'text-kind-write' }
    case 'test':
      return { label: 'Test', sub: 'test', icon: TriangleAlert, text: 'text-kind-test' }
    default:
      return { label: node.kind, sub: '', icon: Database, text: 'text-muted-foreground' }
  }
}

/** Best-effort topological order of the lineage nodes from their edges, stable
 * on the backend's already kind-sorted order when the graph is linear or has
 * cycles. The rail reads left→right = upstream→focus. */
function orderNodes(nodes: readonly LineageNode[], edges: readonly LineageEdge[]): LineageNode[] {
  const byId = new Map(nodes.map((n) => [n.id, n]))
  const indeg = new Map(nodes.map((n) => [n.id, 0]))
  const adj = new Map<string, string[]>()
  for (const e of edges) {
    if (!byId.has(e.source) || !byId.has(e.target)) continue
    adj.set(e.source, [...(adj.get(e.source) ?? []), e.target])
    indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1)
  }
  const queue = nodes.filter((n) => (indeg.get(n.id) ?? 0) === 0)
  const visited = new Set<string>()
  const order: LineageNode[] = []
  while (queue.length > 0) {
    const n = queue.shift()!
    if (visited.has(n.id)) continue
    visited.add(n.id)
    order.push(n)
    for (const t of adj.get(n.id) ?? []) {
      indeg.set(t, (indeg.get(t) ?? 1) - 1)
      const tn = byId.get(t)
      if (tn && (indeg.get(t) ?? 0) <= 0 && !visited.has(t)) queue.push(tn)
    }
  }
  for (const n of nodes) if (!visited.has(n.id)) order.push(n)
  return order
}

function RailCard({
  node,
  focus,
  downstream,
  datasetKind,
}: {
  node: LineageNode
  focus?: boolean
  downstream?: boolean
  datasetKind: string
}) {
  const meta = kindMeta(node, !!focus, datasetKind)
  const Icon = meta.icon
  return (
    <div
      className={cn(
        'flex w-40 shrink-0 flex-col justify-center gap-1 rounded-md border bg-card px-3 py-2 shadow-xs',
        focus ? 'w-44 border-primary ring-2 ring-accent-weak' : 'border-border',
        node.kind === 'external' && 'w-48 border-dashed bg-transparent',
        downstream && 'opacity-50',
      )}
    >
      <span
        className={cn(
          'inline-flex items-center gap-1 text-[9px] font-bold tracking-[0.1em] uppercase',
          meta.text,
        )}
      >
        <Icon className="size-3" aria-hidden="true" />
        {meta.label}
      </span>
      <span
        className={cn(
          'font-mono text-xs font-medium text-foreground',
          node.kind === 'external' ? 'break-all' : 'truncate',
        )}
        title={node.label}
      >
        {node.label}
      </span>
      <span className="truncate font-mono text-2xs text-faint">
        {downstream ? 'downstream' : meta.sub}
      </span>
    </div>
  )
}

function RailArrow({ label }: { label?: string }) {
  return (
    <div className="flex w-12 shrink-0 flex-col items-center justify-center gap-0.5 text-edge">
      {label && <span className="font-mono text-2xs text-faint">{label}</span>}
      <ChevronRight className="size-4" aria-hidden="true" />
    </div>
  )
}

function SectionHeader({ icon: Icon, children, hint }: { icon: LucideIcon; children: ReactNode; hint?: string }) {
  return (
    <div className="flex items-center gap-2 text-2xs font-bold tracking-[0.11em] text-faint uppercase">
      <Icon className="size-3.5 text-muted-foreground" aria-hidden="true" />
      {children}
      {hint && (
        <span className="ml-auto font-mono text-2xs font-medium tracking-normal normal-case text-faint">
          {hint}
        </span>
      )}
    </div>
  )
}

/** Serve-stale affordance mirroring the dependency graph's: POST
 * /api/dependencies/refresh (via useGraphStaleness), then refetch lineage once
 * the rebuild finishes. Only mounted while the response is stale. */
function StaleNote() {
  const { refresh, isRefreshing } = useGraphStaleness()
  const queryClient = useQueryClient()
  const wasRefreshing = useRef(false)
  useEffect(() => {
    if (wasRefreshing.current && !isRefreshing) {
      void queryClient.invalidateQueries({ queryKey: ['lineage'] })
    }
    wasRefreshing.current = isRefreshing
  }, [isRefreshing, queryClient])

  return (
    <div className="flex items-center gap-2 rounded-md border border-warning/25 bg-warning/10 px-3 py-1.5 text-xs text-foreground">
      <span>Lineage may be out of date</span>
      <Button size="xs" variant="outline" onClick={() => refresh()} disabled={isRefreshing}>
        <RefreshCw className={cn('size-3', isRefreshing && 'animate-spin')} aria-hidden="true" />
        {isRefreshing ? 'Refreshing…' : 'Refresh'}
      </Button>
    </div>
  )
}

function consumerKey(c: DatasetConsumer): string {
  return `${c.dataset_fqn}@${c.pipeline}@${c.flowgroup}@${c.action_name}`
}

export function TableDetailView({ fqn }: { fqn: string }) {
  const env = useUIStore((s) => s.selectedEnv)
  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)
  const openTableDetail = useWorkspaceStore((s) => s.openTableDetail)
  const openProjectMap = useWorkspaceStore((s) => s.openProjectMap)
  const { data, isLoading, error } = useLineage(env, fqn)

  // Serve-stale refresh affordance shared with the 404 branch below: read the
  // client stale flag and drive POST /api/dependencies/refresh, refetching
  // lineage once the rebuild finishes (mirrors StaleNote on the success path).
  // These hooks stay unconditional so the 404 early-return can still offer a
  // Refresh — a 404 while the graph is stale is a "may be behind", not a hard
  // "not produced".
  const { isStale, refresh, isRefreshing } = useGraphStaleness()
  const staleRefreshQc = useQueryClient()
  const wasStaleRefreshing = useRef(false)
  useEffect(() => {
    if (wasStaleRefreshing.current && !isRefreshing) {
      void staleRefreshQc.invalidateQueries({ queryKey: ['lineage'] })
    }
    wasStaleRefreshing.current = isRefreshing
  }, [isRefreshing, staleRefreshQc])

  const leaf = fqn.startsWith('sink:') ? fqn.slice('sink:'.length) : (fqn.split('.').pop() ?? fqn)

  const ordered = useMemo(
    () => (data ? orderNodes(data.nodes, data.edges) : []),
    [data],
  )
  const focusId = useMemo(
    () => data?.nodes.find((n) => n.kind === 'write' && n.dataset_fqn === data.fqn)?.id ?? null,
    [data],
  )

  if (!env) {
    return (
      <div className="flex h-full items-center justify-center p-6">
        <EmptyState
          icon={Table2}
          title="No environment selected"
          message="Select an environment to resolve this dataset's lineage."
        />
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center bg-background">
        <div className="size-6 animate-spin rounded-full border-2 border-border border-t-muted-foreground" />
      </div>
    )
  }

  if (error) {
    const status = error instanceof ApiError ? error.status : 0
    if (status === 404) {
      // A 404 while the graph is stale means the index may simply be behind
      // (the dataset was added/renamed since the last build) — offer a Refresh
      // that rebuilds and refetches, rather than blaming delta-sink indexing.
      if (isStale) {
        return (
          <div className="flex h-full items-center justify-center p-6">
            <div className="flex flex-col items-center gap-3">
              <EmptyState
                icon={GitFork}
                title="No lineage yet for this dataset"
                message={`The dependency graph may be behind — ${fqn} could be new or changed since the last build. Refresh to rebuild and try again.`}
              />
              <Button
                size="sm"
                variant="outline"
                onClick={() => refresh()}
                disabled={isRefreshing}
              >
                <RefreshCw
                  className={cn('size-3.5', isRefreshing && 'animate-spin')}
                  aria-hidden="true"
                />
                {isRefreshing ? 'Refreshing…' : 'Refresh graph'}
              </Button>
            </div>
          </div>
        )
      }
      // Not stale: a genuine miss. Only the delta-sink limitation is specific to
      // a `sink:` fqn; a plain table fqn gets a neutral "not produced" message.
      return (
        <div className="flex h-full items-center justify-center p-6">
          <EmptyState
            icon={GitFork}
            title="No lineage found for this dataset"
            message={
              fqn.startsWith('sink:')
                ? `${fqn} was not found. Delta sinks are indexed under their sink id, not the underlying table name.`
                : `${fqn} is not a produced dataset in env "${env}".`
            }
          />
        </div>
      )
    }
    const message =
      status === 400
        ? `Environment "${env}" is not defined. Add substitutions/${env}.yaml or pick another environment.`
        : (error instanceof Error ? error.message : 'Failed to load lineage.')
    return (
      <div className="flex h-full items-center justify-center p-6">
        <EmptyState
          icon={TriangleAlert}
          title={status === 400 ? 'Unknown environment' : 'Could not load lineage'}
          message={message}
        />
      </div>
    )
  }

  if (!data) return null

  const warnings = data.warnings ?? []

  return (
    <div className="h-full overflow-auto bg-background">
      <div className="flex max-w-5xl flex-col gap-5 p-4">
        {/* 1. header */}
        <div className="flex flex-wrap items-start gap-3">
          <div className="flex min-w-0 flex-col gap-1.5">
            <div className="flex items-center gap-2.5">
              {data.kind === 'sink' ? (
                <ArrowRightToLine className="size-5 text-warning" aria-hidden="true" />
              ) : (
                <Table2 className="size-5 text-kind-write" aria-hidden="true" />
              )}
              <span className="truncate font-mono text-xl font-bold tracking-tight text-foreground">
                {leaf}
              </span>
              <span
                className={cn(
                  'rounded-sm border px-1.5 py-0.5 text-[10px] font-bold tracking-[0.1em] uppercase',
                  data.kind === 'sink'
                    ? 'border-warning/40 bg-warning/12 text-warning'
                    : 'border-kind-write/40 bg-kind-write/12 text-kind-write',
                )}
              >
                {data.kind === 'sink' ? 'Sink' : 'Table'}
              </span>
            </div>
            <div className="font-mono text-xs text-muted-foreground">{data.fqn}</div>
            <div className="flex flex-wrap items-center gap-2">
              {data.write_mode && (
                <span className="rounded-sm border border-info/40 bg-info/12 px-2 py-0.5 font-mono text-2xs text-info">
                  {data.write_mode}
                </span>
              )}
              {typeof data.scd_type === 'number' && (
                <span className="font-mono text-2xs text-faint">SCD Type {data.scd_type}</span>
              )}
              {data.source_file && (
                <span className="font-mono text-2xs text-faint">{data.source_file}</span>
              )}
            </div>
          </div>
          <div className="flex-1" />
          <Button variant="outline" size="sm" onClick={() => openProjectMap()}>
            <MapIcon className="size-3.5" aria-hidden="true" />
            Open in Project map
          </Button>
        </div>

        {/* index-health + stale notices (project-wide; NOT problems w/ this table) */}
        {data.stale && <StaleNote />}
        {warnings.length > 0 && (
          <div className="rounded-md border border-info/30 bg-info/10 p-3 text-xs">
            <div className="mb-1 flex items-center gap-1.5 font-semibold text-foreground">
              <Info className="size-3.5 text-info" aria-hidden="true" />
              Index health
            </div>
            <ul className="ml-5 list-disc text-muted-foreground">
              {warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          </div>
        )}

        {/* 2. lineage rail */}
        <section className="flex flex-col gap-2.5">
          <SectionHeader icon={GitFork} hint="source → write → downstream">
            Lineage — how it is loaded
          </SectionHeader>
          <div
            className="overflow-x-auto rounded-md border border-border bg-surface shadow-xs"
            style={{
              backgroundImage: 'radial-gradient(var(--grid-dot) 1px, transparent 1px)',
              backgroundSize: '16px 16px',
              backgroundPosition: '12px 12px',
            }}
          >
            <div className="flex min-w-max items-center px-4 py-5">
              {ordered.map((node, i) => (
                <Fragment key={node.id}>
                  {i > 0 && <RailArrow />}
                  <RailCard node={node} focus={node.id === focusId} datasetKind={data.kind} />
                </Fragment>
              ))}
              {data.consumers.map((c) => (
                <Fragment key={consumerKey(c)}>
                  <RailArrow label="reads" />
                  <RailCard
                    node={{
                      id: consumerKey(c),
                      kind: 'dataset',
                      label: c.dataset_fqn || c.flowgroup,
                      pipeline: c.pipeline,
                      flowgroup: c.flowgroup,
                      dataset_fqn: c.dataset_fqn,
                    }}
                    downstream
                    datasetKind={data.kind}
                  />
                </Fragment>
              ))}
            </div>
          </div>
        </section>

        {/* 3. produced by */}
        <section className="flex flex-col gap-2.5">
          <SectionHeader icon={Workflow}>Produced by</SectionHeader>
          <div className="flex flex-wrap items-center gap-3 rounded-md border border-border bg-card p-3 shadow-xs">
            <span className="rounded-sm border border-kind-write/40 bg-kind-write/12 px-1.5 py-0.5 text-[10px] font-bold tracking-[0.1em] text-kind-write uppercase">
              Write
            </span>
            <div className="flex min-w-0 flex-col gap-1">
              <div className="font-mono text-sm font-semibold text-foreground">
                {data.pipeline} <span className="px-0.5 font-normal text-faint">/</span>{' '}
                {data.flowgroup}
              </div>
              <div className="text-xs text-muted-foreground">
                action <span className="font-mono text-foreground">{data.action_name}</span>
                {data.write_mode ? ` · ${data.write_mode}` : ''}
              </div>
            </div>
            <div className="flex-1" />
            <Button
              variant="outline"
              size="sm"
              disabled={!data.source_file}
              onClick={() =>
                openEntityTab(data.pipeline, data.flowgroup, data.source_file, { view: 'code' })
              }
            >
              <Workflow className="size-3.5" aria-hidden="true" />
              Open flowgroup
            </Button>
          </div>
        </section>

        {/* 4. columns (unavailable — no catalog connection, §6.7) */}
        <section className="flex flex-col gap-2.5">
          <SectionHeader icon={Table2}>Columns</SectionHeader>
          <div className="rounded-md border border-border bg-card p-4 text-xs text-muted-foreground">
            Schema not available locally — LHP compiles pipelines; it does not connect to a catalog.
          </div>
        </section>

        {/* 5. consumers */}
        <section className="flex flex-col gap-2.5">
          <SectionHeader
            icon={ArrowRight}
            hint={`${data.consumers.length} downstream`}
          >
            Consumers
          </SectionHeader>
          {data.consumers.length === 0 ? (
            <p className="rounded-md border border-border bg-card p-3 text-xs text-muted-foreground">
              No downstream consumers read this dataset.
            </p>
          ) : (
            <div className="flex flex-col gap-1.5">
              {data.consumers.map((c) => {
                const canOpen = !!c.dataset_fqn
                return (
                  <button
                    key={consumerKey(c)}
                    type="button"
                    disabled={!canOpen}
                    onClick={() => canOpen && openTableDetail(c.dataset_fqn)}
                    className={cn(
                      'flex w-full items-center gap-2.5 rounded-md border border-border bg-card p-2.5 text-left transition-colors',
                      canOpen ? 'hover:border-accent-line' : 'cursor-default opacity-80',
                    )}
                  >
                    <Table2 className="size-3.5 shrink-0 text-kind-transform" aria-hidden="true" />
                    <span className="min-w-0 truncate font-mono text-xs font-semibold text-foreground">
                      {c.dataset_fqn || c.flowgroup}
                    </span>
                    <span className="text-2xs text-muted-foreground">
                      via <span className="font-mono text-foreground">{c.flowgroup}</span>
                    </span>
                    {canOpen && <ArrowRight className="ml-auto size-3.5 shrink-0 text-faint" aria-hidden="true" />}
                  </button>
                )
              })}
            </div>
          )}
        </section>
      </div>
    </div>
  )
}

export default TableDetailView
