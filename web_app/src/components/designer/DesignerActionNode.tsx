import { Handle, Position, type NodeProps } from '@xyflow/react'
import { CircleX, Plus, TriangleAlert } from 'lucide-react'
import { NodeCard, NODE_HANDLE_CLASS } from '../graph/nodes/NodeCard'
import { canvasNodeDomId } from '../graph/nodes/nodeDom'
import { KIND_STYLES, FALLBACK_KIND_STYLE } from '../graph/nodes/kindStyles'

interface NodeValidation {
  errors: number
  warnings: number
}

/**
 * Action node on the designer canvas: the same quiet card and kind identity
 * as the dashboard's ActionNode, but the sublabel arrives precomputed from
 * designerGraph — the source and resolved views word their sub-type claims
 * differently (the resolved DTO cannot name the write target type). After a
 * validate run, a corner badge shows the errors/warnings mapped to this
 * action (same severity vocabulary as the problems strip and Validate button).
 *
 * When the canvas is in compose mode (not read-only), each node carries a
 * downstream "+" that opens the action palette pre-wired to this node's
 * output view — `onAddDownstream`/`composeReadOnly` arrive on `data` from the
 * canvas.
 */
export function DesignerActionNode({ id, data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const nodeType = (data.nodeType as string) ?? 'action'
  const sublabel = (data.sublabel as string) ?? ''
  const validation = data.validation as NodeValidation | undefined
  const kind = KIND_STYLES[nodeType] ?? FALLBACK_KIND_STYLE
  const onAddDownstream = data.onAddDownstream as ((nodeId: string) => void) | undefined
  const composeReadOnly = data.composeReadOnly === true
  const showAdd = onAddDownstream !== undefined && !composeReadOnly

  const hasIssues = validation !== undefined && (validation.errors > 0 || validation.warnings > 0)

  return (
    <>
      <Handle type="target" position={Position.Left} className={NODE_HANDLE_CLASS} />
      {showAdd && (
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation()
            onAddDownstream(id)
          }}
          className="absolute -right-3 top-1/2 z-10 flex size-5 -translate-y-1/2 items-center justify-center rounded-full border border-border bg-card text-muted-foreground opacity-70 shadow-xs transition-colors hover:border-primary hover:text-foreground hover:opacity-100 focus-visible:opacity-100 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary"
          aria-label={`Add action downstream of ${label}`}
          title="Add downstream action"
        >
          <Plus className="size-3" aria-hidden="true" />
        </button>
      )}
      {hasIssues && (
        <div className="absolute -right-1.5 -top-1.5 z-10 flex items-center gap-1 rounded-full border border-border bg-card px-1.5 py-0.5 shadow-xs">
          {validation.errors > 0 && (
            <span className="flex items-center gap-0.5 text-2xs font-medium text-error">
              <CircleX className="size-2.5" aria-hidden="true" />
              {validation.errors}
            </span>
          )}
          {validation.warnings > 0 && (
            <span className="flex items-center gap-0.5 text-2xs font-medium text-warning">
              <TriangleAlert className="size-2.5" aria-hidden="true" />
              {validation.warnings}
            </span>
          )}
        </div>
      )}
      <NodeCard
        label={label}
        sublabel={sublabel}
        icon={kind.icon}
        chipClassName={kind.chip}
        selected={selected}
        port={{ colorClass: kind.port, input: true, output: true }}
        id={canvasNodeDomId(id)}
        className="w-60"
      />
      <Handle type="source" position={Position.Right} className={NODE_HANDLE_CLASS} />
    </>
  )
}
