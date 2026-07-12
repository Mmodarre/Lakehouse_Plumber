import type { Edge } from '@xyflow/react'
import { attachEdgeMeta, type DesignerEdgeMeta } from './designerGraph'

// ── designerEdges — pure decoration for the named-pipe canvas edges ──
//
// Turns the layouted React Flow edges into the designer's named-pipe edges:
// zips on the view name + semantic kind (via designerGraph.attachEdgeMeta),
// the producing node's kind (for the chip's identity dot), the select-producer
// handler, and the one-shot draw-on class for a freshly added edge. Pure: no
// React, no DOM — the motion gate and the new-edge diff are unit-testable.

/** The CSS class that plays the single draw-on stroke animation. */
export const DRAWON_CLASS = 'lhp-edge--drawon'

// U+001F (unit separator) delimits the fields of a stable key. It cannot occur
// in a view name or node id, so joined fields never collide. Kept as a NAMED
// constant with an explicit escape — never an inline invisible byte — so a
// formatter, whitespace-normalizer, or retype can't silently strip it. The key
// is only ever compared whole; it is never re-split, so there is no round-trip
// that a lost separator could quietly corrupt.
const FIELD_SEP = '\u001f'

/**
 * Stable identity of a designer edge across relayouts. React Flow keys edges
 * by array index (`e-{i}`), which shifts when actions are added or removed, so
 * "is this edge new?" is answered against this content key instead.
 */
export function edgeStableKey(
  source: string,
  target: string,
  viewName: string,
  kind: string,
): string {
  return [source, target, viewName, kind].join(FIELD_SEP)
}

/** The content keys of the current edges, aligned to `attachEdgeMeta` order. */
export function stableKeysFor(rfEdges: Edge[], meta: DesignerEdgeMeta[]): string[] {
  if (rfEdges.length !== meta.length) return []
  return rfEdges.map((e, i) => edgeStableKey(e.source, e.target, meta[i].viewName, meta[i].kind))
}

/** Keys present now but absent from the previously-seen set. */
export function diffNewEdgeKeys(seen: Set<string>, current: string[]): Set<string> {
  return new Set(current.filter((k) => !seen.has(k)))
}

/**
 * The draw-on animation class for one edge — the reduced-motion gate. A newly
 * added edge animates once; when the user has asked for reduced motion it
 * never does (returns ''), regardless of newness.
 */
export function drawOnEdgeClass(isNew: boolean, prefersReducedMotion: boolean): string {
  return isNew && !prefersReducedMotion ? DRAWON_CLASS : ''
}

export interface DecorateOptions {
  /** Select the producing (source) action when its named-pipe chip is clicked. */
  onSelectView: (nodeId: string) => void
  /** node id → kind ('load' | 'transform' | 'write' | 'test' | …) for the chip dot. */
  kindByNode: Map<string, string>
  /** Content keys of edges that should draw on this render. */
  newEdgeKeys: Set<string>
  prefersReducedMotion: boolean
}

/**
 * Decorate the layouted edges with the named-pipe facts the DesignerEdge
 * component reads off `edge.data`. Returns the input unchanged on a length
 * mismatch (mid-relayout render), mirroring attachEdgeMeta.
 */
export function decorateDesignerEdges(
  rfEdges: Edge[],
  meta: DesignerEdgeMeta[],
  opts: DecorateOptions,
): Edge[] {
  const base = attachEdgeMeta(rfEdges, meta)
  if (base === rfEdges) return rfEdges
  return base.map((edge) => {
    const data = edge.data ?? {}
    const kind = String(data.designerKind ?? 'data')
    const viewName = String(data.viewName ?? '')
    const key = edgeStableKey(edge.source, edge.target, viewName, kind)
    return {
      ...edge,
      data: {
        ...data,
        producerKind: opts.kindByNode.get(edge.source) ?? '',
        onSelectView: opts.onSelectView,
        drawOnClass: drawOnEdgeClass(opts.newEdgeKeys.has(key), opts.prefersReducedMotion),
      },
    }
  })
}
