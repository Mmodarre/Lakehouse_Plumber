import { EdgeLabelRenderer, getBezierPath, type EdgeProps } from '@xyflow/react'
import { cn } from '@/lib/utils'
import { KIND_STYLES } from '../graph/nodes/kindStyles'

// ── DesignerEdge — the named-pipe canvas edge ────────────────────
//
// LHP wiring IS named views, so a designer edge is a labeled pipe: a recessive
// neutral stroke (identity lives on the node ports, not the line) carrying the
// view name it induced. For a data edge the label is the literal source:/
// target: string as a functional JetBrains-Mono chip — clicking it selects the
// producing action. A depends_on edge is a manual table-level reference, not a
// view, so it renders honestly distinct: its own dashed rhythm and a muted
// "depends on" tag, never a view-name chip. The one-shot draw-on class arrives
// on edge.data (already reduced-motion-gated). The chip is split out as
// EdgePipeChip so its semantics are unit-testable without a React Flow render.

export interface EdgePipeChipProps {
  designerKind: 'data' | 'depends_on'
  /** The induced view name (data) or referenced table/view (depends_on). */
  viewName: string
  /** Producing node's kind, for the identity dot. Empty → no dot (external). */
  producerKind: string
  /** The producing (source) node id — what the chip selects. */
  sourceId: string
  onSelectView?: (nodeId: string) => void
}

export function EdgePipeChip({
  designerKind,
  viewName,
  producerKind,
  sourceId,
  onSelectView,
}: EdgePipeChipProps) {
  if (designerKind === 'depends_on') {
    return (
      <span
        className={cn(
          'inline-flex items-center rounded-md border border-dashed border-border/80 bg-card px-1.5 py-0.5 shadow-xs',
          'text-2xs uppercase leading-none tracking-[0.08em] text-muted-foreground',
        )}
        aria-label={`Table-level dependency on ${viewName}`}
        title={`Table-level dependency on ${viewName}`}
      >
        depends on
      </span>
    )
  }

  if (viewName === '') return null

  const dot = KIND_STYLES[producerKind]?.port
  return (
    <button
      type="button"
      onClick={() => onSelectView?.(sourceId)}
      aria-label={`View ${viewName}, produced by ${sourceId}. Select the producing action.`}
      title={viewName}
      className={cn(
        'inline-flex max-w-56 cursor-pointer items-center gap-1.5 rounded-md border border-border bg-card px-1.5 py-0.5 shadow-xs',
        'font-mono text-2xs leading-none text-card-foreground',
        'transition-colors hover:border-ring/50',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
      )}
    >
      {dot !== undefined && (
        <span aria-hidden="true" className={cn('size-1.5 shrink-0 rounded-full', dot)} />
      )}
      <span className="truncate">{viewName}</span>
    </button>
  )
}

export function DesignerEdge({
  id,
  source,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
}: EdgeProps) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  })

  const designerKind = (data?.designerKind as 'data' | 'depends_on') ?? 'data'
  const viewName = (data?.viewName as string) ?? ''
  const producerKind = (data?.producerKind as string) ?? ''
  const edgeType = (data?.edgeType as string) ?? 'internal'
  const onSelectView = data?.onSelectView as ((nodeId: string) => void) | undefined
  const drawOnClass = (data?.drawOnClass as string) ?? ''
  const isExternal = edgeType === 'external'
  const isDepends = designerKind === 'depends_on'

  return (
    <>
      <path
        id={id}
        d={edgePath}
        fill="none"
        pathLength={1}
        className={cn('lhp-edge', isDepends ? 'lhp-edge--depends' : 'lhp-edge--data', drawOnClass)}
        style={{
          stroke: selected ? 'var(--primary)' : 'var(--edge)',
          strokeOpacity: isExternal && selected !== true ? 0.55 : 1,
        }}
        strokeWidth={selected === true ? 2 : 1.5}
        markerEnd={selected === true ? 'url(#lhp-arrow-selected)' : 'url(#lhp-arrow)'}
      />
      <EdgeLabelRenderer>
        <div
          className="nodrag nopan absolute"
          style={{
            transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            pointerEvents: 'all',
          }}
        >
          <EdgePipeChip
            designerKind={designerKind}
            viewName={viewName}
            producerKind={producerKind}
            sourceId={source}
            onSelectView={onSelectView}
          />
        </div>
      </EdgeLabelRenderer>
    </>
  )
}
