import { StrictMode, type ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fetchRun, fetchRuns } from '../../api/runs'
import { useRunStore } from '../../store/runStore'
import { useHydrateProblems } from '../useHydrateProblems'
vi.mock('../../api/runs', () => ({ fetchRuns: vi.fn(), fetchRun: vi.fn() }))
const summary = { run_id: 'old', kind: 'validate', status: 'completed', started_at: '2026-09-09T00:00:00Z', env: 'test', pipeline: null, finished_at: null, summary: null }
const issue = { code: 'OLD', title: 'Historical issue', severity: 'warning' }
const detail = { ...summary, issues: [], summary: {}, events: [{ type: 'ValidationCompleted', response: { pipeline_responses: { bronze: { issues: [issue] } } } }] }
beforeEach(() => {
  vi.clearAllMocks()
  useRunStore.getState().reset()
  vi.mocked(fetchRuns).mockResolvedValue({ runs: [summary], total: 1 })
  vi.mocked(fetchRun).mockResolvedValue(detail)
})
describe('history hydration lifecycle', () => {
  it('hydrates successfully through StrictMode effect cleanup/restart', async () => {
    renderHook(() => useHydrateProblems(), { wrapper: ({ children }: { children: ReactNode }) => <StrictMode>{children}</StrictMode> })
    await waitFor(() => expect(useRunStore.getState().hydratedFrom?.runId).toBe('old'))
    expect(useRunStore.getState().issues).toEqual([issue])
  })
  it('cannot replace a completed live run with no issues', async () => {
    let resolve!: (value: typeof detail) => void
    vi.mocked(fetchRun).mockImplementation(() => new Promise((done) => { resolve = done }))
    renderHook(() => useHydrateProblems())
    await waitFor(() => expect(fetchRun).toHaveBeenCalled())
    act(() => { useRunStore.getState().begin('validate'); useRunStore.getState().finish() })
    await act(async () => resolve(detail))
    expect(useRunStore.getState().hydratedFrom).toBeNull()
    expect(useRunStore.getState().issues).toEqual([])
  })
})
