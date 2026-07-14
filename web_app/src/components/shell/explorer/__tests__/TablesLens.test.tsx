import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { TablesLens } from '../TablesLens'
import { useWorkspaceStore } from '../../../../store/workspaceStore'
import { useUIStore } from '../../../../store/uiStore'

vi.mock('../../../../hooks/useTables', () => ({
  useTables: () => ({
    data: {
      tables: [
        {
          full_name: 'main.bronze.customers',
          flowgroup: 'bronze_customers',
          pipeline: 'bronze',
          target_type: 'streaming_table',
          source_file: 'pipelines/bronze/customers.yaml',
          write_mode: null,
          scd_type: null,
        },
        {
          full_name: 'sink:kafka/out',
          flowgroup: 'bronze_events',
          pipeline: 'bronze',
          target_type: 'sink',
          source_file: 'pipelines/bronze/events.yaml',
          write_mode: null,
          scd_type: null,
        },
      ],
      total: 2,
    },
    isLoading: false,
  }),
}))

beforeEach(() => {
  useUIStore.setState({ selectedEnv: 'dev' })
  useWorkspaceStore.setState({
    buffers: [],
    tabs: [],
    activePath: null,
    projectRoot: 'x',
    restoredDirtyCount: 0,
  })
})

describe('TablesLens', () => {
  it('groups tables by schema prefix with sinks last', () => {
    render(<TablesLens />)
    expect(screen.getByText('main.bronze')).toBeInTheDocument()
    expect(screen.getByText('sinks')).toBeInTheDocument()
    expect(screen.getByText('customers')).toBeInTheDocument()
    expect(screen.getByText('kafka/out')).toBeInTheDocument()
  })

  it('opens a table-detail tab on row click', async () => {
    render(<TablesLens />)
    await userEvent.click(screen.getByRole('button', { name: /customers/ }))
    const tab = useWorkspaceStore.getState().tabs.find((t) => t.kind === 'table-detail')
    expect(tab).toMatchObject({ kind: 'table-detail', fqn: 'main.bronze.customers' })
  })

  it('filters rows by the search box', async () => {
    render(<TablesLens />)
    await userEvent.type(screen.getByLabelText('Filter tables'), 'kafka')
    expect(screen.queryByText('customers')).not.toBeInTheDocument()
    expect(screen.getByText('kafka/out')).toBeInTheDocument()
  })
})
