import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  archiveAssistantSession,
  fetchAssistantSession,
  fetchAssistantSessions,
  fetchAssistantStatus,
  fetchDatabricksProfiles,
  fetchExecutorConfig,
  fetchPermissionsConfig,
  fetchPricingConfig,
  installAssistantSkill,
  interruptAssistant,
  newAssistantSession,
  putExecutorConfig,
  putPermissionsConfig,
  putPricingConfig,
  resolveAssistantApproval,
  startAssistantDaemon,
} from '../api/assistant'
import { activeSessionId, useAssistantStore } from '../store/assistantStore'
import type {
  ApprovalRequestBody,
  AssistantStatus,
  ExecutorConfigUpdate,
  PermissionsConfig,
  PricingConfig,
} from '../types/assistant'

// ── useAssistant — react-query wrappers for the assistant panel ──
//
// Queries are enabled only while the panel is open (the panel is the sole
// consumer). The status query is the panel's single source of truth for
// gating; it polls at 2s ONLY while the daemon ladder is not yet ready
// (covers both "daemon start in flight" and "panel just opened, daemon
// warming up") and stops as soon as the ladder is green.

/** True when the omnigent daemon ladder is fully green. */
export function isDaemonReady(status: AssistantStatus | undefined): boolean {
  // binary_found deliberately not required: a daemon run from a venv
  // (binary off PATH) is fully usable; the binary only gates spawning.
  return status !== undefined && status.server_ok && status.host_online
}

/** Provider-aware runtime readiness (chat can be attempted).
 *
 * Claude provider: the in-process SDK just needs its bundled binary
 * (`binary_found`). Omnigent: the daemon ladder. No provider configured
 * yet: there is no runtime to wait for — SetupCard is the next gate. */
export function isRuntimeReady(status: AssistantStatus | undefined): boolean {
  if (status === undefined) return false
  if (status.provider === 'claude_sdk') return status.binary_found
  if (status.provider == null) return true
  return isDaemonReady(status)
}

export function useAssistantStatus(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-status'],
    queryFn: fetchAssistantStatus,
    enabled: options.enabled,
    retry: 1,
    // Poll only while a configured runtime is not ready (daemon warming up,
    // SDK missing); there is nothing to track before configuration.
    refetchInterval: (query) =>
      isRuntimeReady(query.state.data) ? false : 2_000,
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

export function usePricingConfig(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-pricing'],
    queryFn: fetchPricingConfig,
    enabled: options.enabled,
  })
}

export function useUpdatePricingConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: PricingConfig) => putPricingConfig(body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assistant-pricing'] })
    },
  })
}

export function usePermissionsConfig(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-permissions'],
    queryFn: fetchPermissionsConfig,
    enabled: options.enabled,
  })
}

export function useUpdatePermissionsConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: PermissionsConfig) => putPermissionsConfig(body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assistant-permissions'] })
    },
  })
}

export function useDatabricksProfiles(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-databricks-profiles'],
    queryFn: fetchDatabricksProfiles,
    enabled: options.enabled,
  })
}

/** Session snapshot for per-tab thread hydration (`sessionId` = the tab's
 * session; `null` = the MRU-active fallback). 404 (no such session) is an
 * expected state — retries are pointless. */
export function useAssistantSession(options: {
  enabled: boolean
  sessionId: string | null
}) {
  return useQuery({
    queryKey: ['assistant-session', options.sessionId],
    queryFn: () => fetchAssistantSession(options.sessionId ?? undefined),
    enabled: options.enabled,
    retry: false,
    staleTime: Infinity,
  })
}

/** All locally-tracked sessions (tabs = active claude rows, history =
 * archived ones), MRU first. */
export function useAssistantSessions(options: { enabled: boolean }) {
  return useQuery({
    queryKey: ['assistant-sessions'],
    queryFn: fetchAssistantSessions,
    enabled: options.enabled,
  })
}

/** Archive one session server-side (tab closed). The local tab removal is
 * the caller's job — the store stays free of I/O. */
export function useArchiveSession() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (sessionId: string) => archiveAssistantSession(sessionId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assistant-sessions'] })
      void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
    },
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
  return useMutation({
    mutationFn: newAssistantSession,
    onSuccess: () => {
      // Omnigent single-session flow: restart the sole tab as a fresh draft
      // (closing the last tab auto-opens one).
      const store = useAssistantStore.getState()
      if (store.activeTabKey !== null) store.closeTab(store.activeTabKey)
      void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
      void queryClient.invalidateQueries({ queryKey: ['assistant-session'] })
    },
  })
}

export function useResolveApproval() {
  return useMutation({
    // The ACTIVE tab's session rides along (approvals are only clickable in
    // the visible thread); drafts have no session — MRU fallback applies.
    mutationFn: (body: ApprovalRequestBody) => {
      const sessionId = activeSessionId()
      return resolveAssistantApproval(
        sessionId !== undefined ? { ...body, session_id: sessionId } : body,
      )
    },
    onSuccess: (_data, body) => {
      const store = useAssistantStore.getState()
      if (store.activeTabKey !== null) {
        store.resolveApprovalLocal(
          store.activeTabKey,
          body.elicitation_id,
          body.action,
        )
      }
    },
  })
}

/** Interrupt the ACTIVE tab's running turn. The stream itself is NOT
 * aborted — the backend answers with an `interrupted` frame on that tab's
 * stream and then ends the turn. */
export function useInterruptAssistant() {
  return useMutation({
    mutationFn: () => interruptAssistant(activeSessionId()),
  })
}
