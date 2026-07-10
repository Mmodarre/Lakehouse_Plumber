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

// ── ClustersEditor — via the full editor + real PUT bodies ───

const FIXTURE = `pipeline: p1
serverless: false
clusters:
  - label: default
    node_type_id: m5.xlarge
    spark_conf:
      x: "1"
`

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

describe('ClustersEditor', () => {
  it('unknown cluster keys are preserved as read-only chips (and flagged as not rendered)', async () => {
    servePipeline(FIXTURE)
    await renderPipelineEditor()

    expect(screen.getByText('spark_conf')).toBeInTheDocument()
    expect(
      screen.getByText(/not rendered by LHP's bundle template/),
    ).toBeInTheDocument()
    // No form field for the unknown key.
    expect(screen.queryByLabelText('spark_conf')).not.toBeInTheDocument()
  })

  it('adding autoscale + editing a bound splices only those lines; spark_conf bytes intact', async () => {
    const { putBodies } = servePipeline(FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add autoscale' }))
    const max = await screen.findByLabelText('Max workers')
    await user.clear(max)
    await user.type(max, '8')
    await user.tab()

    await save(user)
    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const body = putBodies()[0]!
    // Unknown key kept byte-identical.
    expect(body).toContain('    spark_conf:\n      x: "1"')
    // Removing exactly the autoscale insertion restores the fixture.
    const withoutAutoscale = body.replace(
      / *autoscale:\n *min_workers: 1\n *max_workers: 8\n/,
      '',
    )
    expect(withoutAutoscale).toBe(FIXTURE)
  })

  it('add cluster appends a labeled entry', async () => {
    const { putBodies } = servePipeline(FIXTURE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add cluster' }))
    expect(screen.getByText('Cluster 2')).toBeInTheDocument()

    await save(user)
    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const body = putBodies()[0]!
    expect(body).toContain('- label: default\n')
    expect((body.match(/- label: default/g) ?? []).length).toBe(2)
    // The original cluster's unknown key survives the structural edit.
    expect(body).toContain('spark_conf:')
  })
})
