import { getBezierPath, type EdgeProps } from '@xyflow/react'
import { cn } from '@/lib/utils'

// ── DesignerEdge — the named-pipe canvas edge ────────────────────
//
// A designer edge is a bezier pipe between two action ports: a recessive
// neutral stroke for a data (view) edge, a distinct dashed rhythm for a
// depends_on (table-level) reference. Identity lives on the node ports, so the
// line carries NO label (Fix #4 removed the over-the-edge view-name chip). The
// one-shot draw-on class arrives on edge.data (already reduced-motion-gated).
// The view name / producer id are still threaded through edge.data by the graph
// mapping (harmless, unused here).

export function DesignerEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
}: EdgeProps) {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  })

  const designerKind = (data?.designerKind as 'data' | 'depends_on') ?? 'data'
  const edgeType = (data?.edgeType as string) ?? 'internal'
  const drawOnClass = (data?.drawOnClass as string) ?? ''
  const isExternal = edgeType === 'external'
  const isDepends = designerKind === 'depends_on'

  return (
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
  )
}
