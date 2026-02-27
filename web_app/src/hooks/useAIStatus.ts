/** Hook to poll AI assistant availability. */

import { useQuery } from '@tanstack/react-query'
import { useEffect } from 'react'
import { fetchAIStatus } from '../api/ai'
import { useChatStore } from '../store/chatStore'

export function useAIStatus() {
  const setAIStatus = useChatStore((s) => s.setAIStatus)

  const query = useQuery({
    queryKey: ['ai-status'],
    queryFn: fetchAIStatus,
    refetchInterval: 30_000, // Re-check every 30s
    retry: false, // Don't retry on failure — AI is optional
  })

  useEffect(() => {
    if (query.data) {
      setAIStatus(query.data.available)
    } else if (query.isError) {
      setAIStatus(false)
    }
  }, [query.data, query.isError, setAIStatus])

  return query
}
