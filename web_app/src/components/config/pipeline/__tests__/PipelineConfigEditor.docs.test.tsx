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

// ── Document management — byte-level guarantees ──────────────
//
// Adding a document appends it after a `---` separator; deleting removes
// only that document's region. In BOTH cases every sibling document must
// stay byte-identical (comments included) — asserted on the serialized buffer.

const FIXTURE = `# Pipeline configuration for acme
project_defaults:
  serverless: true # keep on
  catalog: main
---
# bronze group
pipeline:
  - bronze_a
  - bronze_b
edition: PRO
---
pipeline: silver
schema: silver_schema
`

const NO_DEFAULTS_FIXTURE = `# per-pipeline only
pipeline: silver
schema: silver_schema
`

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('PipelineConfigEditor — document management', () => {
  it('add single pipeline appends exactly the skeleton doc; siblings byte-identical', async () => {
    const { bufferContent } = servePipeline(FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add pipeline' }))

    await waitFor(() =>
      expect(bufferContent()).toBe(FIXTURE + '---\npipeline: new_pipeline\n'),
    )
  })

  it('add group appends `pipeline: []`, focuses membership; adding a member serializes it', async () => {
    const { bufferContent } = servePipeline(FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add group' }))
    // The freshly added group focuses its membership combobox.
    const combo = await screen.findByRole('combobox', { name: 'Add pipeline to group' })
    await waitFor(() => expect(combo).toHaveFocus())

    await user.click(combo)
    await user.type(screen.getByPlaceholderText('Search pipelines…'), 'bronze_c')
    await user.click(await screen.findByText('Add "bronze_c"'))

    await waitFor(() =>
      expect(bufferContent()).toBe(FIXTURE + '---\npipeline:\n  - bronze_c\n'),
    )
  })

  it('add project defaults (when missing) appends `project_defaults: {}`', async () => {
    const { bufferContent } = servePipeline(NO_DEFAULTS_FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add project defaults' }))

    await waitFor(() =>
      expect(bufferContent()).toBe(NO_DEFAULTS_FIXTURE + '---\nproject_defaults: {}\n'),
    )
  })

  it('delete doc removes ONLY that document; the others keep their exact bytes', async () => {
    const { bufferContent } = servePipeline(FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: /silver/ }))
    await user.click(screen.getByRole('button', { name: 'Delete document' }))
    await user.click(await screen.findByRole('button', { name: 'Delete' }))

    const expected = FIXTURE.slice(0, FIXTURE.indexOf('---\npipeline: silver'))
    await waitFor(() => expect(bufferContent()).toBe(expected))
  })
})
