import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { lineDiff } from '../../project/__tests__/projectFormTestSupport'
import { parseConfigFile, toJS } from '../../../../lib/yaml-doc'
import { fetchMock, installRadixStubs, renderJobEditor, serveJob } from './jobFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Legacy flat job_config — banner + EXPLICIT conversion ────
//
// Plan-locked behavior: a legacy single-document file edits as project
// defaults (that is how the loader reads it), conversion to the
// multi-document layout happens ONLY through the banner's confirm dialog,
// and a save NEVER converts (byte-checked).

const LEGACY = `# my legacy job config
max_concurrent_runs: 2 # keep low
tags:
  managed_by: lakehouse_plumber
queue:
  enabled: true
`

const BANNER_TEXT = /legacy single-document format/
const CONVERT_BUTTON = 'Convert to multi-document format'

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('JobConfigEditor — legacy flat files', () => {
  it('shows the banner and edits the flat doc as project defaults (no rail)', async () => {
    const { bufferContent } = serveJob(LEGACY)
    renderJobEditor()

    expect(await screen.findByText(BANNER_TEXT)).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Project defaults' })).toBeInTheDocument()
    expect(screen.queryByRole('navigation')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Add job' })).not.toBeInTheDocument()

    // Edits target the flat mapping directly — a scalar patch, no wrapping.
    const user = userEvent.setup()
    const runs = screen.getByLabelText('Maximum concurrent runs')
    expect(runs).toHaveValue('2')
    await user.clear(runs)
    await user.type(runs, '5')
    await user.tab()

    const diffs = lineDiff(LEGACY, bufferContent())
    expect(diffs).toHaveLength(1)
    expect(diffs[0]!.before).toBe('max_concurrent_runs: 2 # keep low')
    expect(diffs[0]!.after).toBe('max_concurrent_runs: 5 # keep low')
    expect(bufferContent()).not.toContain('project_defaults')
  })

  it('NEVER converts on save: an edit-and-revert save round-trips byte-identically', async () => {
    const { bufferContent } = serveJob(LEGACY)
    renderJobEditor()
    await screen.findByText(BANNER_TEXT)

    const user = userEvent.setup()
    const runs = screen.getByLabelText('Maximum concurrent runs')
    await user.clear(runs)
    await user.type(runs, '9')
    await user.tab()
    await user.clear(runs)
    await user.type(runs, '2')
    await user.tab()

    expect(bufferContent()).toBe(LEGACY)
  })

  it('convert dialog cancel leaves the file and banner untouched', async () => {
    serveJob(LEGACY)
    renderJobEditor()
    await screen.findByText(BANNER_TEXT)

    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: CONVERT_BUTTON }))
    await screen.findByRole('alertdialog')
    // The dialog is honest about the rewrite…
    expect(screen.getByRole('alertdialog')).toHaveTextContent(/rewritten and reformatted/)
    // …and warns off monitoring configs at unconventional filenames, where
    // the project_defaults wrap is NOT loader-equivalent (the monitoring
    // loader merges the document verbatim).
    expect(screen.getByRole('alertdialog')).toHaveTextContent(/monitoring\.job_config_path/)
    expect(screen.getByRole('alertdialog')).toHaveTextContent(/must stay flat/)
    await user.click(screen.getByRole('button', { name: 'Keep current format' }))

    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
    expect(screen.getByText(BANNER_TEXT)).toBeInTheDocument()
  })

  it('explicit convert wraps under project_defaults, loader-equivalently; banner gone', async () => {
    const { bufferContent } = serveJob(LEGACY)
    renderJobEditor()
    await screen.findByText(BANNER_TEXT)

    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: CONVERT_BUTTON }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Convert file' }))

    // The file re-classifies immediately: banner gone, rail appears.
    await waitFor(() => expect(screen.queryByText(BANNER_TEXT)).not.toBeInTheDocument())
    expect(
      screen.getByRole('navigation', { name: 'Configuration documents' }),
    ).toBeInTheDocument()

    const body = bufferContent()

    // Loader equivalence: the converted doc's project_defaults mapping is
    // deep-equal to what the single-doc path read from the flat file —
    // JobConfigLoader returns the same (project_defaults, {}) tuple for
    // both layouts.
    const before = toJS(parseConfigFile(LEGACY), 0)
    const after = toJS(parseConfigFile(body), 0) as { project_defaults: unknown }
    expect(Object.keys(after)).toEqual(['project_defaults'])
    expect(after.project_defaults).toEqual(before)

    // The rewrite carries the comments along (attachment may shift).
    expect(body).toContain('# my legacy job config')
    expect(body).toContain('# keep low')
  })
})
