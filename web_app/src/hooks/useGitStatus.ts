import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchGitStatus, commitChanges, pushBranch, pullLatest } from '../api/workspace'

export function useGitStatus() {
  return useQuery({
    queryKey: ['git-status'],
    queryFn: fetchGitStatus,
    refetchInterval: 10_000,
  })
}

export function useCommit() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (message: string) => commitChanges(message),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['git-status'] })
      queryClient.invalidateQueries({ queryKey: ['files'] })
    },
  })
}

export function usePush() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: pushBranch,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['git-status'] })
    },
  })
}

export function usePull() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: pullLatest,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['git-status'] })
      queryClient.invalidateQueries({ queryKey: ['files'] })
    },
  })
}
