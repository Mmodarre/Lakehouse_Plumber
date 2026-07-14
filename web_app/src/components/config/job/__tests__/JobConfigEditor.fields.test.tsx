import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  fetchMock,
  installRadixStubs,
  MONITORING_JOB_CONFIG_PATH,
  renderJobEditor,
  serveJob,
} from './jobFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Field-level behaviors on the wire ────────────────────────

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

const DEFAULTS_ONLY = 'project_defaults:\n  max_concurrent_runs: 1\n'

describe('JobConfigEditor — field behaviors', () => {
  it('queue switch writes queue.enabled nested; reset deletes the whole queue key', async () => {
    const { bufferContent } = serveJob(DEFAULTS_ONLY)
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    // Absent key shows the built-in default (on) — flipping writes it.
    await user.click(screen.getByRole('switch', { name: 'Queue runs' }))
    await waitFor(() =>
      expect(bufferContent()).toBe(DEFAULTS_ONLY + '  queue:\n    enabled: false\n'),
    )

    // Reset to default deletes queue entirely (enabled was its only key).
    await user.click(screen.getByRole('button', { name: 'Reset to default' }))
    await waitFor(() => expect(bufferContent()).toBe(DEFAULTS_ONLY))
  })

  it('email notifications: adding creates the nested lists; removing the last entry deletes the block', async () => {
    const { bufferContent } = serveJob(DEFAULTS_ONLY)
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    // Third email add-row = on_failure (webhook lists share the labels,
    // so target the email placeholder).
    const emailAdds = screen.getAllByPlaceholderText('team@company.com')
    await user.type(emailAdds[2]!, 'oncall@x.com{Enter}')
    await waitFor(() =>
      expect(bufferContent()).toBe(
        DEFAULTS_ONLY + '  email_notifications:\n    on_failure:\n      - oncall@x.com\n',
      ),
    )

    // Pristine absence: removing the only recipient removes on_failure AND
    // the then-empty email_notifications block.
    await user.click(screen.getByRole('button', { name: 'Remove On failure item 1' }))
    await waitFor(() => expect(bufferContent()).toBe(DEFAULTS_ONLY))
  })

  it('webhook notifications: rows edit only the id; entries serialize as {id}', async () => {
    const { bufferContent } = serveJob(DEFAULTS_ONLY)
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    const addInputs = screen.getAllByPlaceholderText('Add webhook id…')
    await user.type(addInputs[2]!, 'pagerduty_alert{Enter}') // on_failure
    await waitFor(() =>
      expect(bufferContent()).toBe(
        DEFAULTS_ONLY + '  webhook_notifications:\n    on_failure:\n      - id: pagerduty_alert\n',
      ),
    )
  })

  it('master-job knobs are editable on defaults and an inert-key note on job docs', async () => {
    const { bufferContent } = serveJob(
      'project_defaults:\n  max_concurrent_runs: 1\n---\njob_name: solo\ngenerate_master_job: false\n',
    )
    renderJobEditor()
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    // Defaults doc: the Master job section is editable.
    expect(screen.getByText('Master job')).toBeInTheDocument()
    await user.type(screen.getByLabelText('Master job name'), 'acme_master')
    await user.tab()
    expect(bufferContent()).toContain('  master_job_name: acme_master\n')

    // Job doc: no section, only the honest inert-key note.
    await user.click(within(nav).getByRole('button', { name: /solo/ }))
    expect(screen.queryByText('Master job')).not.toBeInTheDocument()
    expect(
      screen.getByText(/only takes effect in the project defaults document/),
    ).toBeInTheDocument()
  })

  it('master-job knobs in a monitoring file get the monitoring-specific note', async () => {
    // Monitoring configs never feed master-job generation (read from
    // job_config project_defaults only) and the knobs are excluded from the
    // verbatim passthrough render — "only takes effect in the project
    // defaults document" would wrongly suggest adding one to THIS file.
    serveJob('generate_master_job: false\n', { path: MONITORING_JOB_CONFIG_PATH })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)
    await screen.findByText('Notebook cluster')

    expect(screen.getByText(/has no effect in a monitoring job config/)).toBeInTheDocument()
    expect(
      screen.queryByText(/only takes effect in the project defaults document/),
    ).not.toBeInTheDocument()
  })

  it('notebook_cluster: inert-key note on standard docs, editable on monitoring files', async () => {
    serveJob('project_defaults:\n  notebook_cluster:\n    existing_cluster_id: abc\n')
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    expect(screen.getByText(/only rendered for monitoring jobs/)).toBeInTheDocument()
    expect(screen.queryByText('Notebook cluster')).not.toBeInTheDocument()
  })

  it('monitoring notebook_cluster: new_cluster map edits write through; clearing cascades', async () => {
    const MONITORING =
      'max_concurrent_runs: 1\nnotebook_cluster:\n  new_cluster:\n    num_workers: 2\n'
    const { bufferContent } = serveJob(MONITORING, { path: MONITORING_JOB_CONFIG_PATH })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)
    await screen.findByText('Notebook cluster')
    const user = userEvent.setup()

    // Removing the only new_cluster entry removes new_cluster AND the
    // then-empty notebook_cluster block (pristine absence).
    await user.click(screen.getByRole('button', { name: 'Remove num_workers' }))
    await waitFor(() => expect(bufferContent()).toBe('max_concurrent_runs: 1\n'))
  })
})
