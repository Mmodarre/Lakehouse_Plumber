import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { lineDiff } from '../../project/__tests__/projectFormTestSupport'
import {
  fetchMock,
  installRadixStubs,
  renderPipelineEditor,
  servePipeline,
} from './pipelineFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Flagship byte-preservation test ──────────────────────────
//
// Runs against the REAL packaged pipeline-config template (the file
// `lhp init` ships): ~400 lines of comments across 8 YAML documents.
// Toggling ONE switch in project_defaults must produce a PUT body that
// differs from the template in EXACTLY one line — every comment, every
// commented-out example document, byte-identical.

const TEMPLATE = readFileSync(
  resolve(
    __dirname,
    '../../../../../../src/lhp/templates/init/config/pipeline_config_env.yaml.tmpl',
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

describe('PipelineConfigEditor — single-line diff on the packaged template', () => {
  it('toggling serverless in project_defaults changes exactly one line', async () => {
    const { putBodies } = servePipeline(TEMPLATE)
    await renderPipelineEditor()
    const user = userEvent.setup()

    // The defaults doc is selected by default; flip its serverless switch.
    const serverless = await screen.findByRole('switch', { name: 'Serverless' })
    expect(serverless).toBeChecked()
    await user.click(serverless)

    const save = screen.getByRole('button', { name: 'Save' })
    await waitFor(() => expect(save).toBeEnabled())
    await user.click(save)

    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const body = putBodies()[0]!
    const diffs = lineDiff(TEMPLATE, body)
    expect(diffs).toHaveLength(1)
    expect(diffs[0]!.before).toBe('  serverless: true')
    expect(diffs[0]!.after).toBe('  serverless: false')
  })
})
