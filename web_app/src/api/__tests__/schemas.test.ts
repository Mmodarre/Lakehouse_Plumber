import { describe, expect, it, vi, beforeEach } from 'vitest'

// loadSchemaCached wraps fetchSchema (which calls fetchApi) in a module-level
// promise cache. Reset the module registry between tests so the cache starts
// empty each time — both cases exercise the 'project' kind.
vi.mock('../client', () => ({
  fetchApi: vi.fn(),
}))

beforeEach(() => {
  vi.resetModules()
  vi.clearAllMocks()
})

describe('loadSchemaCached', () => {
  it('fetches each kind at most once and shares the resolved document', async () => {
    const { fetchApi } = await import('../client')
    const { loadSchemaCached } = await import('../schemas')

    const doc = { type: 'object', title: 'project' }
    vi.mocked(fetchApi).mockResolvedValue(doc)

    const first = await loadSchemaCached('project')
    const second = await loadSchemaCached('project')

    expect(fetchApi).toHaveBeenCalledTimes(1)
    expect(first).toBe(second)
    expect(first).toBe(doc)
  })

  it('fetches distinct kinds independently', async () => {
    const { fetchApi } = await import('../client')
    const { loadSchemaCached } = await import('../schemas')

    vi.mocked(fetchApi).mockResolvedValue({ type: 'object' })

    await loadSchemaCached('project')
    await loadSchemaCached('job_config')

    expect(fetchApi).toHaveBeenCalledTimes(2)
    expect(fetchApi).toHaveBeenNthCalledWith(1, '/schemas/project')
    expect(fetchApi).toHaveBeenNthCalledWith(2, '/schemas/job_config')
  })
})
