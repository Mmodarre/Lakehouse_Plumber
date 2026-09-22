import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { RunHistoryView } from '../RunHistoryView'
import { useHistoryViewState } from '../../../../store/runHistoryViewStore'
import { openWorkspaceFile } from '../../../../workspace/openWorkspaceFile'
const mocks = vi.hoisted(() => ({ useRuns: vi.fn(), useRun: vi.fn() }))
vi.mock('../../../../hooks/useRuns', () => mocks)
vi.mock('../../../../workspace/openWorkspaceFile', () => ({ openWorkspaceFile: vi.fn() }))
vi.mock('../../../detail/JsonTree', () => ({ JsonTree: () => null }))
const rows = Array.from({ length: 50 }, (_, index) => ({ run_id: `run-${index}`, kind: 'validate', env: index % 2 ? 'prod' : 'test', pipeline: index % 2 ? 'gold' : 'bronze', status: index % 2 ? 'failed' : 'completed', started_at: '2026-09-09T00:00:00Z', finished_at: '2026-09-09T00:00:01Z' }))
beforeEach(() => {
  vi.clearAllMocks()
  useHistoryViewState.setState({ selectedRunId: null, env: '', pipeline: '', status: '', limit: 50, eventsFor: {} })
  mocks.useRuns.mockReturnValue({ data: { runs: rows }, isLoading: false, isError: false, isFetching: false })
  mocks.useRun.mockReturnValue({ data: { summary: {}, issues: [{ code: 'YAML', severity: 'error', message: 'Bad field', file: 'pipelines/a.yaml', line: 12 }], events: [] } })
})
describe('history investigation', () => {
  it('filters loaded runs and requests older runs without implying server filtering', () => {
    render(<RunHistoryView />)
    fireEvent.change(screen.getByRole('combobox', { name: 'Environment' }), { target: { value: 'prod' } })
    expect(screen.getByText(/25 of 50 loaded/)).toBeInTheDocument()
    fireEvent.change(screen.getByRole('combobox', { name: 'Status' }), { target: { value: 'completed' } })
    expect(screen.getByText(/No loaded runs match/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Load older runs' }))
    expect(mocks.useRuns).toHaveBeenLastCalledWith(100)
  })
  it('retains expanded selection across panel switches and opens historical source locations', () => {
    mocks.useRuns.mockReturnValue({ data: { runs: rows.slice(0, 2) }, isLoading: false, isError: false })
    const view = render(<RunHistoryView />)
    fireEvent.click(screen.getAllByRole('button', { name: 'Expand run details' })[0])
    fireEvent.click(screen.getByRole('button', { name: /Bad field/ }))
    expect(openWorkspaceFile).toHaveBeenCalledWith('pipelines/a.yaml', { source: true, line: 12 })
    view.unmount()
    render(<RunHistoryView />)
    expect(screen.getByRole('button', { name: /Bad field/ })).toBeInTheDocument()
    expect(useHistoryViewState.getState().selectedRunId).toBe('run-0')
  })
})
