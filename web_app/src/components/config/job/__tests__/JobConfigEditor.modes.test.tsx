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

// ── File-shape classification + rail behavior ────────────────

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

const MULTI = `project_defaults:
  max_concurrent_runs: 1
---
job_name: bronze_job
tags:
  layer: bronze
---
job_name:
  - silver_job
  - gold_job
---
just_a_note: ignored
`

describe('JobConfigEditor — file shapes', () => {
  it('multi-document file → precedence rail with file-order rows', async () => {
    serveJob(MULTI)
    renderJobEditor()

    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const rows = within(nav).getAllByRole('button')
    expect(rows.map((row) => row.textContent)).toEqual([
      expect.stringContaining('Built-in defaults'),
      expect.stringContaining('Project defaults'),
      expect.stringContaining('bronze_job'),
      expect.stringContaining('2 jobs'),
      expect.stringContaining('Unrecognized document'),
      expect.stringContaining('Add job'),
      expect.stringContaining('Add job group'),
    ])
    // Group caption lists the members.
    expect(within(nav).getByText('silver_job, gold_job')).toBeInTheDocument()
  })

  it('built-ins ghost row shows DEFAULT_JOB_CONFIG read-only', async () => {
    serveJob(MULTI)
    renderJobEditor()
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    await user.click(within(nav).getByRole('button', { name: /Built-in defaults/ }))
    const card = await screen.findByTestId('builtin-defaults-card')
    expect(card).toHaveTextContent('max_concurrent_runs')
    expect(card).toHaveTextContent('{"enabled":true}')
    expect(card).toHaveTextContent('generate_master_job')
    expect(within(card).queryByRole('textbox')).not.toBeInTheDocument()
  })

  it('unrecognized document → passthrough-only card, no form controls', async () => {
    serveJob(MULTI)
    renderJobEditor()
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    await user.click(within(nav).getByRole('button', { name: /Unrecognized document/ }))
    expect(await screen.findByText(/skipped by LHP/)).toBeInTheDocument()
    expect(screen.getByText('just_a_note')).toBeInTheDocument()
    expect(screen.getByText(/the loader skips it/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Maximum concurrent runs')).not.toBeInTheDocument()
  })

  it('monitoring flat file → single settings form, caption, no rail, no add-job', async () => {
    serveJob('max_concurrent_runs: 1\ntags:\n  managed_by: lakehouse_plumber\n', {
      path: MONITORING_JOB_CONFIG_PATH,
    })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(await screen.findByText(/Flat single-document format/)).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Settings' })).toBeInTheDocument()
    expect(screen.queryByRole('navigation')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Add job' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Delete document' })).not.toBeInTheDocument()
    // Monitoring-only editor is present; master knobs are defaults-only.
    expect(screen.getByText('Notebook cluster')).toBeInTheDocument()
    expect(screen.queryByText('Master job')).not.toBeInTheDocument()
  })

  it('monitoring file with multiple documents → blocking error, Save disabled', async () => {
    serveJob('max_concurrent_runs: 1\n---\njob_name: nope\n', {
      path: MONITORING_JOB_CONFIG_PATH,
    })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(
      await screen.findByText(/the monitoring loader requires exactly one/),
    ).toBeInTheDocument()
  })

  it('monitoring file with a trailing --- (null second doc) → blocking error, Save disabled', async () => {
    // yaml.safe_load raises on ANY second document, including the null one a
    // trailing `---` produces — the raw document count decides, not the
    // null-filtered list.
    serveJob('max_concurrent_runs: 1\n---\n', { path: MONITORING_JOB_CONFIG_PATH })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(
      await screen.findByText(/has 2 YAML documents — the monitoring loader requires exactly one/),
    ).toBeInTheDocument()
  })

  it("monitoring file: job-loader-only errors don't count against Save", async () => {
    // `project_defaults` must-be-a-mapping is a JobConfigLoader raise; the
    // monitoring loader reads the file flat and renders the key verbatim,
    // so the SaveBar must not report it as a blocking error.
    serveJob('project_defaults: 42\n', { path: MONITORING_JOB_CONFIG_PATH })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(
      await screen.findByText(/'project_defaults' has no meaning in a monitoring job config/),
    ).toBeInTheDocument()
    // The only alert is the misused-key warning; the job-loader shape error
    // (project_defaults must be a mapping) is filtered out for monitoring files.
    expect(screen.getAllByRole('alert')).toHaveLength(1)
  })

  it('monitoring multi-document file counts exactly the one blocking error', async () => {
    // VAL_004 (duplicate job_name) is job-loader semantics — inapplicable
    // here; only the multi-document error itself blocks monitoring files.
    serveJob('job_name: dup\n---\njob_name: dup\n', { path: MONITORING_JOB_CONFIG_PATH })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(
      await screen.findByText(/the monitoring loader requires exactly one/),
    ).toBeInTheDocument()
    // Exactly one blocking alert — VAL_004 (duplicate job_name) is job-loader
    // semantics and is NOT surfaced for a monitoring file.
    expect(screen.getAllByRole('alert')).toHaveLength(1)
  })

  it('monitoring non-mapping document still counts as a blocking error', async () => {
    // A top-level list raises CFG_008 in monitoring_service.py — this one
    // IS applicable, so it surfaces as a blocking error alert.
    serveJob('- not\n- a-mapping\n', { path: MONITORING_JOB_CONFIG_PATH })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(await screen.findByText(/Flat single-document format/)).toBeInTheDocument()
    expect(screen.getByText(/Document 1 must be a mapping/)).toBeInTheDocument()
  })

  it("monitoring file misusing 'project_defaults' → rendered-verbatim warning", async () => {
    serveJob('project_defaults:\n  max_concurrent_runs: 1\n', {
      path: MONITORING_JOB_CONFIG_PATH,
    })
    renderJobEditor(MONITORING_JOB_CONFIG_PATH)

    expect(
      await screen.findByText(/'project_defaults' has no meaning in a monitoring job config/),
    ).toBeInTheDocument()
    // Flat read: project_defaults is a passthrough chip, not unwrapped.
    expect(screen.getByText('project_defaults')).toBeInTheDocument()
  })

  it('duplicate job_name across documents → badges on BOTH rows, Save blocked, fixable', async () => {
    const { bufferContent } = serveJob(
      'job_name: alpha\n---\njob_name:\n  - beta\n  - alpha\n',
    )
    renderJobEditor()
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    // Loader-faithful error list (VAL_004 on the later occurrence only)…
    expect(within(nav).getByLabelText('1 error')).toBeInTheDocument()
    // …but the rail badges EVERY involved document.
    expect(within(nav).getAllByText('duplicate')).toHaveLength(2)

    // Fix: rename the clashing member in the group doc.
    await user.click(within(nav).getByRole('button', { name: /2 jobs/ }))
    const items = await screen.findAllByLabelText(/^Job names item \d+$/)
    await user.clear(items[1]!)
    await user.type(items[1]!, 'gamma')
    await user.tab()

    await waitFor(() => expect(within(nav).queryByText('duplicate')).not.toBeInTheDocument())
    await waitFor(() =>
      expect(bufferContent()).toBe('job_name: alpha\n---\njob_name:\n  - beta\n  - gamma\n'),
    )
  })

  it('empty job-group document blocks Save until a member exists (VAL_003)', async () => {
    const { bufferContent } = serveJob('project_defaults:\n  max_concurrent_runs: 1\n')
    renderJobEditor()
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    await user.click(within(nav).getByRole('button', { name: 'Add job group' }))
    expect(await screen.findByText(/empty job_name list/)).toBeInTheDocument()

    // The add-row input is labelled by the list's Label ("Job names").
    await user.type(screen.getByLabelText('Job names'), 'delta{Enter}')
    await waitFor(() =>
      expect(bufferContent()).toBe(
        'project_defaults:\n  max_concurrent_runs: 1\n---\njob_name:\n  - delta\n',
      ),
    )
  })
})
