vi.mock('@/api/help', () => ({ loadHelpCached: vi.fn().mockResolvedValue({ version: 1, entries: [] }) }))
import { useLayoutStore } from '@/store/layoutStore'
import { useUIStore } from '@/store/uiStore'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { cleanup, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useConfigViewStore } from '../configViewState'
import {
  configureFetch,
  installRadixStubs,
  mountConfigFormView,
  resetConfigStores,
  seedConfigBuffer,
} from './configFormTestSupport'

const fetchMock = vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

beforeEach(() => {
  resetConfigStores()
  useConfigViewStore.setState({ views: {} })
  installRadixStubs()
  configureFetch(fetchMock)
  vi.stubGlobal('fetch', fetchMock)
})
afterEach(() => vi.unstubAllGlobals())

describe('Configuration navigation keeps presentation separate from YAML', () => {
  it('collapse/expand and section navigation preserve bytes and clean state', async () => {
    const source = '# keep this comment\nname: acme\nauthor: Data team\n'
    const { bufferContent } = seedConfigBuffer('lhp.yaml', source)
    mountConfigFormView('lhp.yaml', 'project')
    const name = await screen.findByLabelText('Name')
    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: 'Collapse General section' }))
    expect(name).not.toBeVisible()
    await user.selectOptions(
      screen.getByRole('combobox', { name: 'Jump to section' }),
      'config-section-general',
    )
    expect(name).toBeVisible()
    expect(name).toHaveValue('acme')
    await user.click(screen.getByRole('button', { name: 'Collapse all' }))
    expect(name).not.toBeVisible()
    await user.click(screen.getByRole('button', { name: 'Expand all' }))
    expect(name).toBeVisible()
    expect(bufferContent()).toBe(source)
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(false)
  })

  it('search finds YAML keys in absent sections without adding them', async () => {
    const source = 'name: acme\n'
    const { bufferContent } = seedConfigBuffer('lhp.yaml', source)
    mountConfigFormView('lhp.yaml', 'project')
    const user = userEvent.setup()
    await user.type(
      await screen.findByRole('searchbox', { name: 'Search settings' }),
      'checkpoint_path',
    )
    expect(await screen.findByRole('button', { name: 'Add Monitoring section' })).toBeVisible()
    expect(
      screen.queryByRole('button', { name: 'Collapse General section' }),
    ).not.toBeInTheDocument()
    expect(bufferContent()).toBe(source)
    await user.click(screen.getByRole('button', { name: 'Clear settings search' }))
    await user.click(screen.getByRole('checkbox', { name: 'Configured sections only' }))
    expect(screen.queryByRole('button', { name: 'Add Monitoring section' })).not.toBeInTheDocument()
    expect(bufferContent()).toBe(source)
  })

  it('returns to the selected pipeline document after another view unmounts the form', async () => {
    const path = 'config/pipeline_config.yaml'
    seedConfigBuffer(
      path,
      'project_defaults:\n  catalog: base\n---\npipeline: second\ncatalog: second_catalog\n',
    )
    mountConfigFormView(path, 'pipeline')
    const user = userEvent.setup()
    await user.click(await screen.findByRole('button', { name: /second.*single pipeline/ }))
    expect(screen.getByLabelText('Catalog')).toHaveValue('second_catalog')
    cleanup()
    mountConfigFormView(path, 'pipeline')
    expect(await screen.findByLabelText('Catalog')).toHaveValue('second_catalog')
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(false)
  })

  it('uses a selector when the loaded center pane is narrow', async () => {
    vi.stubGlobal(
      'ResizeObserver',
      class {
        callback: ResizeObserverCallback
        constructor(callback: ResizeObserverCallback) {
          this.callback = callback
        }
        observe() {
          this.callback(
            [{ contentRect: { width: 550 } }] as ResizeObserverEntry[],
            this as unknown as ResizeObserver,
          )
        }
        unobserve() {}
        disconnect() {}
      },
    )
    const path = 'config/pipeline_config.yaml'
    seedConfigBuffer(
      path,
      'project_defaults:\n  catalog: base\n---\npipeline: second\ncatalog: second_catalog\n',
    )
    mountConfigFormView(path, 'pipeline')
    const selector = await screen.findByRole('combobox', {
      name: 'Configuration document',
    })
    await userEvent.setup().selectOptions(selector, '1')
    await waitFor(() => expect(screen.getByLabelText('Catalog')).toHaveValue('second_catalog'))
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(false)
  })
})

it('keeps viewer search and saved previews usable while source fields and mutations are disabled', async () => {
  const path = 'config/pipeline_config.yaml'
  const source = 'pipeline: alpha\ncatalog: main\n'
  seedConfigBuffer(path, source)
  useLayoutStore.setState({ viewerMode: true })
  useUIStore.setState({ selectedEnv: 'dev' })
  fetchMock.mockResolvedValue(
    new Response(
      JSON.stringify({
        path,
        kind: 'pipeline',
        env: 'dev',
        target: 'alpha',
        targets: ['alpha'],
        values: { catalog: 'main' },
        tiers: ['Built-in defaults', 'Target override'],
        warnings: ['Saved files only.'],
        source: 'saved',
      }),
      { status: 200 },
    ),
  )
  mountConfigFormView(path, 'pipeline')
  expect(await screen.findByLabelText('Catalog')).toBeDisabled()
  expect(screen.getByRole('button', { name: 'Delete document' })).toBeDisabled()
  expect(screen.queryByRole('button', { name: 'Add pipeline' })).not.toBeInTheDocument()
  const user = userEvent.setup()
  await user.type(screen.getByRole('searchbox', { name: 'Search settings' }), 'catalog')
  expect(screen.getByLabelText('Catalog')).toBeVisible()
  await user.click(screen.getByRole('button', { name: 'Preview effective saved settings' }))
  expect(await screen.findByLabelText('Effective saved settings')).toHaveTextContent('main')
  expect(screen.getByRole('combobox', { name: 'Pipeline target' })).not.toBeDisabled()
  expect(useWorkspaceStore.getState().buffers[0].content).toBe(source)
})

it('retries a failed config load from outside the disabled form fields', async () => {
  seedConfigBuffer('lhp.yaml', '')
  useWorkspaceStore.getState().markLoadFailed('lhp.yaml')
  fetchMock.mockResolvedValue(
    new Response('name: recovered\n', { status: 200, headers: { ETag: 'e1' } }),
  )
  mountConfigFormView('lhp.yaml', 'project')
  const retry = await screen.findByRole('button', { name: 'Retry file' })
  expect(retry.closest('[inert]')).toBeNull()
  await userEvent.setup().click(retry)
  expect(await screen.findByLabelText('Name')).toHaveValue('recovered')
  expect(useWorkspaceStore.getState().buffers[0].loadFailed).toBe(false)
})
