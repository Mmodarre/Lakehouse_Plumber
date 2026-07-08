import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { RunHistoryPage } from '../RunHistoryPage'
import { useRuns, useRun } from '../../hooks/useRuns'
import type { RunDetail, RunListResponse } from '../../types/api'

vi.mock('../../hooks/useRuns', () => ({
  useRuns: vi.fn(),
  useRun: vi.fn(),
}))

const useRunsMock = vi.mocked(useRuns)
const useRunMock = vi.mocked(useRun)

// The page only reads data/isLoading/isError/error off the query results,
// so a partial object cast through the hook's return type suffices.
function queryResult<T>(data: T | undefined, overrides: object = {}) {
  return {
    data,
    isLoading: false,
    isError: false,
    error: null,
    ...overrides,
  } as unknown as ReturnType<typeof useRuns> & ReturnType<typeof useRun>
}

const runList: RunListResponse = {
  runs: [
    {
      run_id: 'r1',
      kind: 'generate',
      env: 'dev',
      pipeline: 'bronze',
      status: 'completed',
      started_at: '2026-07-07T10:00:00Z',
      finished_at: '2026-07-07T10:00:12Z',
      summary: null,
    },
    {
      run_id: 'r2',
      kind: 'validate',
      env: 'prod',
      pipeline: null,
      status: 'failed',
      started_at: '2026-07-07T09:00:00Z',
      finished_at: null,
      summary: null,
    },
  ],
  total: 2,
}

const runDetail: RunDetail = {
  run_id: 'r2',
  kind: 'validate',
  env: 'prod',
  pipeline: null,
  status: 'failed',
  started_at: '2026-07-07T09:00:00Z',
  finished_at: null,
  summary: null,
  issues: [
    {
      severity: 'error',
      code: 'LHP-VAL-002',
      message: 'Duplicate table target',
      file: 'pipelines/silver/orders.yaml',
      line: 4,
    },
  ],
  events: null,
}

beforeEach(() => {
  vi.clearAllMocks()
  useRunsMock.mockReturnValue(queryResult(runList))
  useRunMock.mockReturnValue(queryResult(runDetail))
})

describe('RunHistoryPage', () => {
  it('renders one row per run with kind, env, and status', () => {
    render(<RunHistoryPage />)
    expect(screen.getByText('generate')).toBeInTheDocument()
    expect(screen.getByText('validate')).toBeInTheDocument()
    expect(screen.getByText('dev')).toBeInTheDocument()
    expect(screen.getByText('prod')).toBeInTheDocument()
    expect(screen.getByText('completed')).toBeInTheDocument()
    expect(screen.getByText('failed')).toBeInTheDocument()
    expect(screen.getByText('all pipelines')).toBeInTheDocument()
  })

  it('shows the empty state when there are no runs', () => {
    useRunsMock.mockReturnValue(queryResult({ runs: [], total: 0 }))
    render(<RunHistoryPage />)
    expect(screen.getByText('No runs yet')).toBeInTheDocument()
  })

  it('expands a run row into its issue list on click', async () => {
    const user = userEvent.setup()
    render(<RunHistoryPage />)

    await user.click(screen.getByText('prod'))

    expect(useRunMock).toHaveBeenCalledWith('r2', false)
    expect(screen.getByText('LHP-VAL-002')).toBeInTheDocument()
    expect(screen.getByText('Duplicate table target')).toBeInTheDocument()
    expect(screen.getByText('orders.yaml:4')).toBeInTheDocument()
  })
})
