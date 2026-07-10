import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { fetchMock, renderProjectForm, serveProject } from './projectFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Validator wiring: config-model issues → SaveBar + fields ─

const BROKEN_MONITORING = `name: acme
event_log:
  enabled: true
  catalog: main
  schema: ops
monitoring:
  enabled: true
  checkpoint_path: /tmp/cp
  job_config_path: config/mon.yaml
  max_concurrent_streams: 99
  materialized_views:
    - name: mv_costs
      sql: SELECT 1
      sql_path: sql/mv.sql
`

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('ProjectConfigForm — validation wiring', () => {
  it('range + sql/sql_path XOR violations block Save with a truthful count; fixing re-enables', async () => {
    serveProject(BROKEN_MONITORING)
    await renderProjectForm()
    const user = userEvent.setup()

    // Two blocking errors: max_concurrent_streams out of 1..20, and the
    // materialized view specifying both sql and sql_path.
    expect(await screen.findByText('2 errors')).toBeInTheDocument()
    const saveButton = screen.getByRole('button', { name: 'Save' })
    expect(saveButton).toBeDisabled()

    // The XOR issue lands on ITS row, not somewhere generic.
    const row = screen.getByRole('group', { name: 'Materialized view 1' })
    expect(within(row).getByText(/both 'sql' and 'sql_path'/)).toBeInTheDocument()

    // Fix 1: clear the SQL file path (empty commit deletes the key).
    await user.clear(within(row).getByLabelText('SQL file path'))
    await user.tab()
    await waitFor(() => expect(screen.getByText('1 error')).toBeInTheDocument())

    // Fix 2: bring the stream count into range.
    const streams = screen.getByLabelText('Max concurrent streams')
    await user.clear(streams)
    await user.type(streams, '10')
    await user.tab()

    await waitFor(() => expect(screen.queryByText(/error/)).not.toBeInTheDocument())
    expect(saveButton).toBeEnabled()
  })

  it('an invalid sandbox table_pattern shows an inline error next to the field', async () => {
    serveProject('name: acme\nsandbox:\n  table_pattern: "{namespace}_only"\n')
    await renderProjectForm()

    const field = await screen.findByLabelText('Table pattern')
    expect(field).toHaveAttribute('aria-invalid', 'true')
    const issue = document.getElementById('sandbox-table-pattern-issue')
    expect(issue).not.toBeNull()
    expect(issue!.textContent).toMatch(/is invalid: .*\{table\}/)
    expect(screen.getByText('1 error')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('warnings do not block Save', async () => {
    serveProject('author: someone\n') // no name → warning, not error
    await renderProjectForm()
    const user = userEvent.setup()

    const description = await screen.findByLabelText('Description')
    await user.type(description, 'a project')
    await user.tab()

    expect(screen.queryByText(/\d+ errors?/)).not.toBeInTheDocument()
    await waitFor(() => expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled())
  })

  it('a file with YAML parse errors shows the error card and NO form', async () => {
    serveProject('foo: [unclosed\n')
    await renderProjectForm()

    expect(await screen.findByText(/YAML parse error/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Name')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
  })
})
