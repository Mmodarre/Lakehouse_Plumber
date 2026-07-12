// ── designerKeyboard — pure spatial navigation for the canvas ────
//
// Arrow-key selection movement across the laid-out canvas nodes. Pure
// geometry: given the current selection and a direction, pick the nearest
// node in that half-plane. No React, no DOM — unit-testable in isolation.

export const NAV_KEYS = ['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'] as const
export type ArrowKey = (typeof NAV_KEYS)[number]

export function isArrowKey(key: string): key is ArrowKey {
  return (NAV_KEYS as readonly string[]).includes(key)
}

/** A node's position on the canvas — top-left is fine (all cards share a size). */
export interface NodePoint {
  id: string
  x: number
  y: number
}

// A node just off the primary axis counts less against a candidate than one
// far away along it, so movement biases toward the same row/column but still
// crosses lanes when nothing lines up.
const CROSS_AXIS_WEIGHT = 0.5
const EPSILON = 0.5

/** Leftmost, then topmost — the flow's entry point when nothing is selected. */
function entryNode(nodes: NodePoint[]): string | null {
  let best: NodePoint | null = null
  for (const n of nodes) {
    if (best === null || n.x < best.x || (n.x === best.x && n.y < best.y)) best = n
  }
  return best?.id ?? null
}

/**
 * The id of the node to move to from `currentId` in `key`'s direction, or
 * `null` if there is nowhere to go. With no current selection, the first
 * arrow press lands on the flow's entry node.
 */
export function nextNodeInDirection(
  currentId: string | null,
  key: ArrowKey,
  nodes: NodePoint[],
): string | null {
  if (nodes.length === 0) return null

  const current = currentId === null ? undefined : nodes.find((n) => n.id === currentId)
  if (current === undefined) return entryNode(nodes)

  let bestId: string | null = null
  let bestScore = Infinity
  for (const n of nodes) {
    if (n.id === current.id) continue
    const dx = n.x - current.x
    const dy = n.y - current.y

    let primary: number
    let cross: number
    if (key === 'ArrowRight') {
      if (dx <= EPSILON) continue
      primary = dx
      cross = Math.abs(dy)
    } else if (key === 'ArrowLeft') {
      if (dx >= -EPSILON) continue
      primary = -dx
      cross = Math.abs(dy)
    } else if (key === 'ArrowDown') {
      if (dy <= EPSILON) continue
      primary = dy
      cross = Math.abs(dx)
    } else {
      if (dy >= -EPSILON) continue
      primary = -dy
      cross = Math.abs(dx)
    }

    const score = primary + CROSS_AXIS_WEIGHT * cross
    if (score < bestScore) {
      bestScore = score
      bestId = n.id
    }
  }
  return bestId
}
