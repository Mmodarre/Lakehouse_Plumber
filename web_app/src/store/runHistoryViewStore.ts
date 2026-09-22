import { create } from 'zustand'

interface HistoryViewState {
  selectedRunId: string | null
  env: string
  pipeline: string
  status: string
  limit: number
  eventsFor: Record<string, boolean>
}
// Survives switching bottom tabs without persisting stale project data to disk.
export const useHistoryViewState = create<HistoryViewState>(() => ({
  selectedRunId: null, env: '', pipeline: '', status: '', limit: 50, eventsFor: {},
}))
