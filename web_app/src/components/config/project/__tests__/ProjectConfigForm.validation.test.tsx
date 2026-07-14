import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { fetchMock, renderProjectForm, serveProject } from './projectFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Validator wiring: config-model issues → per-field + section rows ─

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
  it('range + sql/sql_path XOR issues surface on their own fields; fixing each clears it', async () => {
    serveProject(BROKEN_MONITORING)
    await renderProjectForm()
    const user = userEvent.setup()

    // Two config-model issues: max_concurrent_streams out of 1..20, and the
    // materialized view specifying both sql and sql_path. Each lands on ITS
    // own field/row, not somewhere generic. (Issues no longer block editing —
    // saving is the buffer's ⌘S path.)
    const row = await screen.findByRole('group', { name: 'Materialized view 1' })
    expect(within(row).getByText(/both 'sql' and 'sql_path'/)).toBeInTheDocument()
    expect(screen.getByText(/must be an integer in the range 1\.\.20/)).toBeInTheDocument()

    // Fix 1: clear the SQL file path (empty commit deletes the key) → the XOR
    // issue clears.
    await user.clear(within(row).getByLabelText('SQL file path'))
    await user.tab()
    await waitFor(() =>
      expect(screen.queryByText(/both 'sql' and 'sql_path'/)).not.toBeInTheDocument(),
    )

    // Fix 2: bring the stream count into range → its field issue clears too.
    const streams = screen.getByLabelText('Max concurrent streams')
    await user.clear(streams)
    await user.type(streams, '10')
    await user.tab()
    await waitFor(() =>
      expect(screen.queryByText(/must be an integer in the range 1\.\.20/)).not.toBeInTheDocument(),
    )
  })

  it('an invalid sandbox table_pattern shows an inline error next to the field', async () => {
    serveProject('name: acme\nsandbox:\n  table_pattern: "{namespace}_only"\n')
    await renderProjectForm()

    const field = await screen.findByLabelText('Table pattern')
    expect(field).toHaveAttribute('aria-invalid', 'true')
    const issue = document.getElementById('sandbox-table-pattern-issue')
    expect(issue).not.toBeNull()
    expect(issue!.textContent).toMatch(/is invalid: .*\{table\}/)
  })

  it('a warning surfaces as a warning row and never blocks editing', async () => {
    serveProject('author: someone\n') // no name → warning, not error
    await renderProjectForm()
    const user = userEvent.setup()

    // The missing-name warning renders (a warning row, not an error).
    expect(
      await screen.findByText(/loader falls back to 'unnamed_project'/),
    ).toBeInTheDocument()

    // Editing is never gated on validation — an unrelated edit still commits.
    const description = await screen.findByLabelText('Description')
    await user.type(description, 'a project')
    await user.tab()

    // A warning produces no blocking error summary.
    expect(screen.queryByText(/\d+ errors?/)).not.toBeInTheDocument()
  })

  it('a file with YAML parse errors shows the error card and NO form', async () => {
    serveProject('foo: [unclosed\n')
    await renderProjectForm()

    expect(await screen.findByText(/YAML parse error/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Name')).not.toBeInTheDocument()
  })
})
