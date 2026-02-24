import { useState, useMemo, useCallback } from 'react'
import type { Node, Edge } from '@xyflow/react'

interface UseGraphSearchResult {
  query: string
  setQuery: (q: string) => void
  clear: () => void
  nodes: Node[]
  edges: Edge[]
  matchCount: number
  totalCount: number
  matchedNodeIds: string[]
  isSearchActive: boolean
}

/**
 * Sits between ELK layout output and React Flow rendering.
 * Injects `searchMatch` / `searchDimmed` booleans into node and edge data
 * based on a case-insensitive label match.
 */
export function useGraphSearch(
  sourceNodes: Node[],
  sourceEdges: Edge[],
): UseGraphSearchResult {
  const [query, setQuery] = useState('')

  const clear = useCallback(() => setQuery(''), [])

  const isSearchActive = query.trim().length > 0

  const matchedNodeIds = useMemo(() => {
    if (!isSearchActive) return []
    const q = query.trim().toLowerCase()
    return sourceNodes
      .filter((n) => {
        const label = (n.data?.label as string) ?? ''
        return label.toLowerCase().includes(q)
      })
      .map((n) => n.id)
  }, [sourceNodes, query, isSearchActive])

  const matchedSet = useMemo(() => new Set(matchedNodeIds), [matchedNodeIds])

  const nodes = useMemo(() => {
    if (!isSearchActive) return sourceNodes
    return sourceNodes.map((n) => ({
      ...n,
      data: {
        ...n.data,
        searchMatch: matchedSet.has(n.id),
        searchDimmed: !matchedSet.has(n.id),
      },
    }))
  }, [sourceNodes, isSearchActive, matchedSet])

  const edges = useMemo(() => {
    if (!isSearchActive) return sourceEdges
    return sourceEdges.map((e) => ({
      ...e,
      data: {
        ...e.data,
        searchDimmed: !matchedSet.has(e.source) && !matchedSet.has(e.target),
      },
    }))
  }, [sourceEdges, isSearchActive, matchedSet])

  return {
    query,
    setQuery,
    clear,
    nodes,
    edges,
    matchCount: matchedNodeIds.length,
    totalCount: sourceNodes.length,
    matchedNodeIds,
    isSearchActive,
  }
}
