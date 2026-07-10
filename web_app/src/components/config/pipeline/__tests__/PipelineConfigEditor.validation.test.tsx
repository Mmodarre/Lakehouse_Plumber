import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
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

// ── Field-level validation + the remaining editors ───────────

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

describe('configuration map (str→str)', () => {
  it('non-string value: warning row + blocking error; unlock-and-edit coerces to a quoted string', async () => {
    const { putBodies } = servePipeline(
      'pipeline: p1\nconfiguration:\n  spark.x: 42\n  spark.y: "ok"\n',
    )
    await renderPipelineEditor()
    const user = userEvent.setup()

    // Loader hard-fails on non-string configuration values → blocking.
    expect(screen.getByText('1 error')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
    expect(screen.getByText('not text')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Edit spark.x as text' }))
    const input = screen.getByDisplayValue('42')
    await user.clear(input)
    await user.type(input, '43')
    await user.tab()

    await save(user)
    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const body = putBodies()[0]!
    // Coerced to a string (quoted — would re-parse as a number otherwise);
    // the untouched row keeps its exact bytes.
    expect(body).toContain('spark.x: "43"')
    expect(body).toContain('spark.y: "ok"')
  })
})

describe('event_log override', () => {
  it('Disabled mode writes `event_log: false`; Inherit deletes the key', async () => {
    const { putBodies } = servePipeline('pipeline: p1\ncatalog: main\n')
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByLabelText('Event log mode'))
    await user.click(await screen.findByRole('option', { name: 'Disabled for this pipeline' }))
    await save(user)
    await waitFor(() => expect(putBodies()).toHaveLength(1))
    expect(putBodies()[0]).toBe('pipeline: p1\ncatalog: main\nevent_log: false\n')

    await user.click(screen.getByLabelText('Event log mode'))
    await user.click(await screen.findByRole('option', { name: 'Inherit from lhp.yaml' }))
    await save(user)
    await waitFor(() => expect(putBodies()).toHaveLength(2))
    expect(putBodies()[1]).toBe('pipeline: p1\ncatalog: main\n')
  })
})

describe('notifications editor', () => {
  it('add entry + email; the entry serializes with both required lists', async () => {
    const { putBodies } = servePipeline('pipeline: p1\ncatalog: main\n')
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add notification' }))
    const emails = await screen.findByPlaceholderText('team@company.com')
    await user.type(emails, 'ops@acme.com')
    await user.click(screen.getByRole('button', { name: 'Add Email recipients item' }))

    await save(user)
    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const body = putBodies()[0]!
    expect(body).toContain('notifications:')
    expect(body).toContain('- ops@acme.com')
    // Template iterates alerts unconditionally → the key must exist.
    expect(body).toContain('alerts:')
  })
})
