/**
 * Stable DOM id for a canvas node card. The designer canvas points its
 * `aria-activedescendant` at this id so keyboard arrow-selection reaches
 * assistive tech; harmless (an unused id) on graphs that don't use it.
 *
 * Kept in its own module (not NodeCard.tsx) so the component file exports only
 * components — react-refresh/fast-refresh requires that.
 */
export function canvasNodeDomId(nodeId: string): string {
  return `lhp-canvas-node-${nodeId}`
}
