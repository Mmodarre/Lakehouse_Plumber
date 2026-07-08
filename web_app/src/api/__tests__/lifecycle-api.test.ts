import { describe, expect, it, vi, beforeEach } from 'vitest'
import { fetchApi } from '../client'
import { fetchBlueprints } from '../blueprints'
import { fetchRuns, fetchRun } from '../runs'
import { fetchEnvironmentResolved } from '../environments'
import { initProject } from '../project'

// Path-construction tests for the P4-B lifecycle API modules: each helper
// must hit the documented endpoint with correctly encoded query/path parts.

vi.mock('../client', () => ({
  fetchApi: vi.fn().mockResolvedValue({}),
}))

const fetchApiMock = vi.mocked(fetchApi)

beforeEach(() => {
  fetchApiMock.mockClear()
})

describe('fetchBlueprints', () => {
  it('defaults to include_instances=false', async () => {
    await fetchBlueprints()
    expect(fetchApiMock).toHaveBeenCalledWith('/blueprints?include_instances=false')
  })

  it('passes include_instances=true through', async () => {
    await fetchBlueprints(true)
    expect(fetchApiMock).toHaveBeenCalledWith('/blueprints?include_instances=true')
  })
})

describe('fetchRuns / fetchRun', () => {
  it('lists runs with the default limit', async () => {
    await fetchRuns()
    expect(fetchApiMock).toHaveBeenCalledWith('/runs?limit=50')
  })

  it('lists runs with a custom limit', async () => {
    await fetchRuns(10)
    expect(fetchApiMock).toHaveBeenCalledWith('/runs?limit=10')
  })

  it('fetches one run without events by default', async () => {
    await fetchRun('abc-123')
    expect(fetchApiMock).toHaveBeenCalledWith('/runs/abc-123?include_events=false')
  })

  it('URL-encodes the run id and passes include_events', async () => {
    await fetchRun('a/b', true)
    expect(fetchApiMock).toHaveBeenCalledWith('/runs/a%2Fb?include_events=true')
  })
})

describe('fetchEnvironmentResolved', () => {
  it('hits the resolved endpoint with an encoded env name', async () => {
    await fetchEnvironmentResolved('dev env')
    expect(fetchApiMock).toHaveBeenCalledWith('/environments/dev%20env/resolved')
  })
})

describe('initProject', () => {
  it('POSTs the request body to /project/init', async () => {
    await initProject({ project_name: 'demo', bundle: true })
    expect(fetchApiMock).toHaveBeenCalledWith('/project/init', {
      method: 'POST',
      body: JSON.stringify({ project_name: 'demo', bundle: true }),
    })
  })
})
