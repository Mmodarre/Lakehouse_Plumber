import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  fetchAssistantSession,
  fetchAssistantStatus,
  fetchDatabricksProfiles,
  fetchExecutorConfig,
  installAssistantSkill,
  interruptAssistant,
  newAssistantSession,
  putExecutorConfig,
  resolveAssistantApproval,
  startAssistantDaemon,
} from '../api/assistant'
import { useAssistantStore } from '../store/assistantStore'
import type {
  ApprovalRequestBody,
  AssistantStatus,
  ExecutorConfigUpdate,
} from '../types/assistant'

// ── useAssistant — react-query wrappers for the assistant panel ──
//
// Queries are enabled only while the panel is open (the panel is the sole
// consumer). The status query is the panel's single source of truth for
// gating; it polls at 2s ONLY while the daemon ladder is not yet ready
// (covers both "daemon start in flight" and "panel just opened, daemon
// warming up") and stops as soon as the ladder is green.

/** True when the daemon ladder is fully green (chat can be attempted). */
export function isDaemonReady(status: AssistantStatus | undefined): boolean {
  // binary_found deliberately not required: a daemon run from a venv
  // (binary off PATH) is fully usable; the binary only gates spawning.
  return status !== undefined && status.server_ok && status.host_online
}

export function useAssistantStatus(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-status'],
    queryFn: fetchAssistantStatus,
    enabled: options.enabled,
    retry: 1,
    refetchInterval: (query) =>
      isDaemonReady(query.state.data) ? false : 2_000,
  })
}

export function useExecutorConfig(options: { enabled: boolean }) {
  return useQuery({
    // `null` (not an error) when no config is stored yet — see the api layer.
    queryKey: ['assistant-config'],
    queryFn: fetchExecutorConfig,
    enabled: options.enabled,
  })
}

export function useDatabricksProfiles(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-databricks-profiles'],
    queryFn: fetchDatabricksProfiles,
    enabled: options.enabled,
  })
}

/** Session snapshot for thread rehydration after a reload. 404 (no active
 * session) is an expected state — retries are pointless. */
export function useAssistantSession(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-session'],
    queryFn: fetchAssistantSession,
    enabled: options.enabled,
    retry: false,
    staleTime: Infinity,
  })
}

export function useUpdateExecutorConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: ExecutorConfigUpdate) => putExecutorConfig(body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assistant-config'] })
      void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
    },
  })
}

export function useInstallSkill() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: installAssistantSkill,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
    },
  })
}

export function useStartDaemon() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: startAssistantDaemon,
    onSuccess: () => {
      // The 2s status poll (active while not ready) tracks the daemon
      // coming up; one immediate refetch shortens the first gap.
      void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
    },
  })
}

export function useNewAssistantSession() {
  const queryClient = useQueryClient()
  const newConversation = useAssistantStore((s) => s.newConversation)
  return useMutation({
    mutationFn: newAssistantSession,
    onSuccess: () => {
      newConversation()
      void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
      void queryClient.invalidateQueries({ queryKey: ['assistant-session'] })
    },
  })
}

export function useResolveApproval() {
  const resolveApprovalLocal = useAssistantStore((s) => s.resolveApprovalLocal)
  return useMutation({
    mutationFn: (body: ApprovalRequestBody) => resolveAssistantApproval(body),
    onSuccess: (_data, body) => {
      resolveApprovalLocal(body.elicitation_id, body.action)
    },
  })
}

/** Interrupt the running turn. The stream itself is NOT aborted — the
 * backend answers with an `interrupted` frame and then ends the turn. */
export function useInterruptAssistant() {
  return useMutation({
    mutationFn: interruptAssistant,
  })
}
