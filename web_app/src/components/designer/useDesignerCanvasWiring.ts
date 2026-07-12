import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type RefObject,
} from 'react'
import type { Edge, Node } from '@xyflow/react'
import { canvasNodeDomId } from '../graph/nodes/nodeDom'
import type { DesignerEdgeMeta } from './designerGraph'
import { decorateDesignerEdges, diffNewEdgeKeys, stableKeysFor } from './designerEdges'
import { isArrowKey, nextNodeInDirection, type NodePoint } from './designerKeyboard'

// ── useDesignerCanvasWiring — canvas motion + keyboard, factored out ──
//
// Keeps DesignerCanvas thin: returns the decorated named-pipe edges (with the
// one-shot draw-on already reduced-motion-gated) and the props that make the
// canvas region keyboard-navigable. Everything stateful lives here; the pure
// diff/gate/geometry it composes are unit-tested in designerEdges/Keyboard, and
// the new-edge detection (useNewEdgeKeys) is exported and tested directly.

const DRAW_ON_MS = 480

function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(
    () =>
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches,
  )
  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)')
    const onChange = () => setReduced(mq.matches)
    mq.addEventListener?.('change', onChange)
    return () => mq.removeEventListener?.('change', onChange)
  }, [])
  return reduced
}

/**
 * The content keys of edges that should draw on right now. A key is "new" only
 * the first time it appears after the initial populated render — so relayouts,
 * data refreshes, and resolved-view toggles never re-animate the whole graph,
 * and only a freshly added action's edge draws on, once.
 *
 * Takes the key array directly (no string signature round-trip): the seen-set
 * and animating-set are compared as whole keys, so there is no separator to
 * strip and no way for the draw-on to silently die while tests stay green.
 * The caller passes a memoized array so identity is stable across renders.
 */
export function useNewEdgeKeys(currentKeys: string[]): Set<string> {
  const seenRef = useRef<Set<string>>(new Set())
  const initializedRef = useRef(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const [animating, setAnimating] = useState<Set<string>>(new Set())

  // Layout effect (not effect): mark the new edge before the browser paints, so
  // its first frame already starts the draw-on rather than flashing full first.
  useLayoutEffect(() => {
    if (currentKeys.length === 0) return
    const seen = seenRef.current
    if (!initializedRef.current) {
      initializedRef.current = true
      for (const k of currentKeys) seen.add(k)
      return
    }
    const added = diffNewEdgeKeys(seen, currentKeys)
    for (const k of currentKeys) seen.add(k)
    if (added.size === 0) return
    setAnimating((prev) => new Set([...prev, ...added]))
    clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => setAnimating(new Set()), DRAW_ON_MS)
  }, [currentKeys])

  useEffect(() => () => clearTimeout(timerRef.current), [])
  return animating
}

export interface DesignerCanvasWiringArgs {
  rfNodes: Node[]
  rfEdges: Edge[]
  edgeMeta: DesignerEdgeMeta[]
  selectedId: string | null
  setSelectedId: (id: string | null) => void
  /** The inspector panel — Enter hands keyboard focus to it. */
  inspectorRef: RefObject<HTMLElement | null>
  isLayouting: boolean
  fitView: (options?: { padding?: number; duration?: number }) => void
}

export function useDesignerCanvasWiring({
  rfNodes,
  rfEdges,
  edgeMeta,
  selectedId,
  setSelectedId,
  inspectorRef,
  isLayouting,
  fitView,
}: DesignerCanvasWiringArgs) {
  const prefersReducedMotion = usePrefersReducedMotion()

  // Refit only on structural change (not on selection or data-only refreshes).
  const structureKey = useMemo(
    () =>
      rfNodes
        .map((n) => n.id)
        .sort()
        .join('\n'),
    [rfNodes],
  )
  useEffect(() => {
    if (structureKey === '' || isLayouting) return
    const timer = setTimeout(() => fitView({ padding: 0.15, duration: 300 }), 50)
    return () => clearTimeout(timer)
  }, [structureKey, isLayouting, fitView])

  const currentKeys = useMemo(() => stableKeysFor(rfEdges, edgeMeta), [rfEdges, edgeMeta])
  const newEdgeKeys = useNewEdgeKeys(currentKeys)

  const kindByNode = useMemo(() => {
    const map = new Map<string, string>()
    for (const n of rfNodes) map.set(n.id, String(n.data?.nodeType ?? ''))
    return map
  }, [rfNodes])

  const displayEdges = useMemo(
    () =>
      decorateDesignerEdges(rfEdges, edgeMeta, {
        onSelectView: setSelectedId,
        kindByNode,
        newEdgeKeys,
        prefersReducedMotion,
      }),
    [rfEdges, edgeMeta, setSelectedId, kindByNode, newEdgeKeys, prefersReducedMotion],
  )

  const nodePoints = useMemo<NodePoint[]>(
    () => rfNodes.map((n) => ({ id: n.id, x: n.position.x, y: n.position.y })),
    [rfNodes],
  )

  const onKeyDown = useCallback(
    (e: KeyboardEvent<HTMLDivElement>) => {
      const target = e.target as HTMLElement
      // Never hijack typing inside the inspector's form controls.
      if (
        target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.tagName === 'SELECT' ||
        target.isContentEditable
      ) {
        return
      }
      if (isArrowKey(e.key)) {
        const next = nextNodeInDirection(selectedId, e.key, nodePoints)
        if (next !== null && next !== selectedId) {
          e.preventDefault()
          setSelectedId(next)
        }
        return
      }
      if (e.key === 'Enter' && selectedId !== null) {
        e.preventDefault()
        inspectorRef.current?.focus()
        return
      }
      if (e.key === 'Escape' && selectedId !== null) {
        e.preventDefault()
        setSelectedId(null)
      }
    },
    [selectedId, nodePoints, setSelectedId, inspectorRef],
  )

  // aria-activedescendant points the group at the keyboard-selected node's DOM
  // id, so arrow-key movement is announced by assistive tech (the visible ring
  // alone reaches sighted users only).
  const canvasProps = {
    tabIndex: 0,
    role: 'group' as const,
    'aria-label': 'Pipeline canvas',
    'aria-keyshortcuts': 'ArrowUp ArrowDown ArrowLeft ArrowRight Enter Escape',
    'aria-activedescendant': selectedId !== null ? canvasNodeDomId(selectedId) : undefined,
    onKeyDown,
  }

  return { displayEdges, canvasProps }
}
