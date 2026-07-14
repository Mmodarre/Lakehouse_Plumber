import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  fetchMock,
  installRadixStubs,
  renderPipelineEditor,
  servePipeline,
} from './pipelineFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Precedence rail — semantics + the VAL_006 duplicate matrix ──

const DUP_FIXTURE = `project_defaults:
  serverless: true
---
pipeline: alpha
---
pipeline:
  - alpha
  - beta
  - beta
`

const NO_DEFAULTS_FIXTURE = `pipeline: alpha
catalog: main
`

const UNRECOGNIZED_FIXTURE = `pipeline: alpha
---
foo: 1
bar: 2
`

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

function railRows(): HTMLElement[] {
  return within(screen.getByRole('navigation', { name: 'Configuration documents' })).getAllByRole(
    'button',
  )
}

describe('PipelineConfigEditor — duplicate-name matrix', () => {
  it('same name in doc + group, and twice within one group → badges on ALL involved rows', async () => {
    servePipeline(DUP_FIXTURE)
    await renderPipelineEditor()

    // alpha is in doc 2 (single) and doc 3 (group); beta twice inside the group.
    // Loader raises VAL_006 for each repeated registration; the form badges
    // every involved row (the aggregate count now lives in the shell).
    const alphaRow = screen.getByRole('button', { name: /alpha.*single pipeline/ })
    const groupRow = screen.getByRole('button', { name: /3 pipelines/ })
    expect(within(alphaRow).getByText('duplicate')).toBeInTheDocument()
    expect(within(groupRow).getByText('duplicate')).toBeInTheDocument()
  })

  it('removing the clashing members clears every badge and unblocks Save', async () => {
    servePipeline(DUP_FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: /3 pipelines/ }))
    await user.click(screen.getByRole('button', { name: 'Remove alpha' }))
    const removeBetas = await screen.findAllByRole('button', { name: 'Remove beta' })
    await user.click(removeBetas[0])

    await waitFor(() => expect(screen.queryByText('duplicate')).not.toBeInTheDocument())
    expect(screen.queryByText(/error/)).not.toBeInTheDocument()
  })
})

describe('PipelineConfigEditor — rail semantics', () => {
  it('ghost row shows built-in defaults, read-only', async () => {
    servePipeline(DUP_FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    const ghost = screen.getByRole('button', { name: /Built-in defaults/ })
    expect(within(ghost).getByText('built into LHP')).toBeInTheDocument()

    await user.click(ghost)
    const card = screen.getByTestId('builtin-defaults-card')
    expect(within(card).getByText('serverless')).toBeInTheDocument()
    expect(within(card).getByText('ADVANCED')).toBeInTheDocument()
    expect(within(card).getByText('CURRENT')).toBeInTheDocument()
    // Read-only: no editable controls inside the built-in card.
    expect(within(card).queryByRole('textbox')).not.toBeInTheDocument()
    expect(within(card).queryByRole('switch')).not.toBeInTheDocument()
  })

  it('missing project_defaults → add affordance in the defaults slot', async () => {
    servePipeline(NO_DEFAULTS_FIXTURE)
    await renderPipelineEditor()
    expect(screen.getByRole('button', { name: 'Add project defaults' })).toBeInTheDocument()
  })

  it('pipeline docs keep FILE ORDER in the rail', async () => {
    servePipeline(DUP_FIXTURE)
    await renderPipelineEditor()
    const labels = railRows().map((row) => row.textContent ?? '')
    const alphaAt = labels.findIndex((t) => t.includes('alpha'))
    const groupAt = labels.findIndex((t) => t.includes('3 pipelines'))
    expect(alphaAt).toBeGreaterThan(-1)
    expect(groupAt).toBeGreaterThan(alphaAt)
  })

  it('unrecognized document → passthrough-only card, no form', async () => {
    servePipeline(UNRECOGNIZED_FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: /Unrecognized/ }))
    expect(screen.getByText(/ignored by LHP/)).toBeInTheDocument()
    expect(screen.getByText('foo')).toBeInTheDocument()
    expect(screen.getByText('bar')).toBeInTheDocument()
    // No form fields for an ignored doc — the only switch on the page is
    // the shell's doc-independent "Use for runs" toggle.
    expect(screen.getAllByRole('switch')).toEqual([
      screen.getByRole('switch', { name: 'Use for runs' }),
    ])
    expect(screen.queryByLabelText('Catalog')).not.toBeInTheDocument()
  })
})
