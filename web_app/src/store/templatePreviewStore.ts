import { create } from 'zustand'
import type { TemplatePreviewContext, TemplatePreviewRequest, TemplatePreviewResponse } from '@/api/template-authoring'
import { useWorkspaceStore, tabBufferPath } from './workspaceStore'

export interface TemplatePreviewSession {
  values: Record<string, unknown>
  context: TemplatePreviewContext
  stage: TemplatePreviewRequest['stage']
  result?: TemplatePreviewResponse
  resultSignature?: string
}
export const newTemplatePreviewSession = (): TemplatePreviewSession => ({ values: {}, context: { pipeline: 'preview', flowgroup: 'preview', environment: '' }, stage: 'expanded' })
export const useTemplatePreviewStore = create<{
  sessions: Record<string, TemplatePreviewSession>
  dependenciesRevision: number
  update: (key: string, patch: Partial<TemplatePreviewSession>) => void
  invalidate: () => void
}>((set) => ({
  sessions: {}, dependenciesRevision: 0,
  update: (key, patch) => set((s) => ({ sessions: { ...s.sessions, [key]: { ...(s.sessions[key] ?? newTemplatePreviewSession()), ...patch } } })),
  invalidate: () => set((s) => ({ dependenciesRevision: s.dependenciesRevision + 1 })),
}))
export function markTemplatePreviewsStale() { useTemplatePreviewStore.getState().invalidate() }
useWorkspaceStore.subscribe((state) => {
  const open = new Set(state.tabs.map(tabBufferPath).filter(Boolean).map((path) => `${state.projectRoot ?? ''}::${path}`))
  const sessions = useTemplatePreviewStore.getState().sessions
  if (Object.keys(sessions).some((key) => !open.has(key))) useTemplatePreviewStore.setState({ sessions: Object.fromEntries(Object.entries(sessions).filter(([key]) => open.has(key))) })
})
