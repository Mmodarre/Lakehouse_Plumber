import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { lineDiff } from '../../project/__tests__/projectFormTestSupport'
import { fetchMock, installRadixStubs, renderJobEditor, serveJob } from './jobFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Flagship byte-preservation tests ─────────────────────────
//
// (1) The REAL packaged job-config template (the file `lhp init` ships):
// ~240 lines of comments across 4 YAML documents. Changing ONE first-class
// field in project_defaults must produce a PUT body that differs from the
// template in EXACTLY one line.
// (2) Passthrough fidelity: a file leaning on the toyaml passthrough
// (trigger / run_as / health) keeps those blocks byte-identical through a
// first-class edit.

const TEMPLATE = readFileSync(
  resolve(
    __dirname,
    '../../../../../../src/lhp/templates/init/config/job_config_env.yaml.tmpl',
  ),
  'utf8',
)

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

async function save(user: ReturnType<typeof userEvent.setup>) {
  const saveButton = screen.getByRole('button', { name: 'Save' })
  await waitFor(() => expect(saveButton).toBeEnabled())
  await user.click(saveButton)
}

describe('JobConfigEditor — byte preservation', () => {
  it('changing max_concurrent_runs in project_defaults of the packaged template = one-line diff', async () => {
    const { putBodies } = serveJob(TEMPLATE)
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    // The defaults doc is selected by default.
    const runs = screen.getByLabelText('Maximum concurrent runs')
    expect(runs).toHaveValue('1')
    await user.clear(runs)
    await user.type(runs, '3')
    await user.tab()
    await save(user)

    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const diffs = lineDiff(TEMPLATE, putBodies()[0]!)
    expect(diffs).toHaveLength(1)
    expect(diffs[0]!.before).toBe('  max_concurrent_runs: 1')
    expect(diffs[0]!.after).toBe('  max_concurrent_runs: 3')
  })

  it('passthrough blocks (trigger/run_as/health) are chipped and byte-preserved through edits', async () => {
    const FIXTURE =
      'project_defaults:\n' +
      '  max_concurrent_runs: 1\n' +
      '---\n' +
      'job_name: nightly\n' +
      'trigger:\n' +
      '  file_arrival:\n' +
      '    url: "s3://bucket/landing/"  # watched path\n' +
      'run_as:\n' +
      '  service_principal_name: "abc-123"\n' +
      'health:\n' +
      '  rules:\n' +
      '    - metric: RUN_DURATION_SECONDS\n' +
      '      op: GREATER_THAN\n' +
      '      value: 3600\n'
    const { putBodies } = serveJob(FIXTURE)
    renderJobEditor()
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    await user.click(within(nav).getByRole('button', { name: /nightly/ }))
    // The split is visible: passthrough chips + the honest caption.
    expect(await screen.findByText('Passthrough keys')).toBeInTheDocument()
    for (const key of ['trigger', 'run_as', 'health']) {
      expect(screen.getByText(key)).toBeInTheDocument()
    }
    expect(screen.getByText(/passed through into the Databricks job resource/)).toBeInTheDocument()

    // First-class edit on the same doc: timeout splice only.
    const timeout = screen.getByLabelText('Timeout (seconds)')
    await user.type(timeout, '300')
    await user.tab()
    await save(user)

    await waitFor(() => expect(putBodies()).toHaveLength(1))
    // The whole save is the fixture plus exactly the spliced line — every
    // passthrough byte (comments and quoting included) is identical.
    expect(putBodies()[0]).toBe(FIXTURE + 'timeout_seconds: 300\n')
  })
})
