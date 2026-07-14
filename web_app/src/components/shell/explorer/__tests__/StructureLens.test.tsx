import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { StructureLens } from '../StructureLens'
import { useWorkspaceStore } from '../../../../store/workspaceStore'

vi.mock('../../../../hooks/usePipelines', () => ({
  usePipelines: () => ({
    data: {
      pipelines: [
        { name: 'bronze', action_count: 2, flowgroup_count: 1 },
        { name: 'silver', action_count: 1, flowgroup_count: 1 },
      ],
      total: 2,
    },
  }),
}))
vi.mock('../../../../hooks/useFlowgroups', () => ({
  useFlowgroups: () => ({
    data: {
      flowgroups: [
        {
          name: 'customers',
          pipeline: 'bronze',
          action_types: ['load', 'write'],
          action_count: 2,
          presets: [],
          source_file: 'pipelines/bronze/customers.yaml',
          template: null,
        },
        {
          name: 'accounts',
          pipeline: 'silver',
          action_types: ['transform', 'write'],
          action_count: 2,
          presets: [],
          source_file: 'pipelines/silver/accounts.yaml',
          template: null,
        },
      ],
      total: 2,
    },
  }),
}))
vi.mock('../../../../hooks/useFiles', () => ({
  useFileList: () => ({
    data: {
      name: '',
      path: '',
      type: 'directory',
      children: [
        {
          name: 'config',
          path: 'config',
          type: 'directory',
          children: [
            {
              name: 'pipeline_config_dev.yaml',
              path: 'config/pipeline_config_dev.yaml',
              type: 'file',
            },
          ],
        },
        {
          name: 'presets',
          path: 'presets',
          type: 'directory',
          children: [{ name: 'bronze_layer.yaml', path: 'presets/bronze_layer.yaml', type: 'file' }],
        },
      ],
    },
  }),
}))
vi.mock('../../../../hooks/usePresets', () => ({
  usePresets: () => ({ data: { presets: ['bronze_layer'], total: 1 } }),
}))
vi.mock('../../../../hooks/useTemplates', () => ({
  useTemplates: () => ({ data: { templates: [], total: 0 } }),
}))
vi.mock('../../../../hooks/useBlueprints', () => ({
  useBlueprints: () => ({ data: { blueprints: [], total: 0 } }),
}))
vi.mock('../../../../hooks/useEnvironments', () => ({
  useEnvironments: () => ({ data: { environments: ['dev'], total: 1 } }),
}))
// StructureLens now derives severity dots via useMapEnrichment → useTables;
// mock the fetch so the lens renders without a QueryClient provider.
vi.mock('../../../../hooks/useTables', () => ({
  useTables: () => ({ data: { tables: [], total: 0 }, isLoading: false }),
}))
// Sandbox scope is injected directly (item 6): a mutable ref lets each test
// pick the active scope (null = sandbox off ⇒ show everything).
const scopeRef = vi.hoisted(() => ({ current: null as ReadonlySet<string> | null }))
vi.mock('../../../sandbox/useSandboxScope', () => ({
  useSandboxScope: () => scopeRef.current,
}))

beforeEach(() => {
  scopeRef.current = null
  useWorkspaceStore.setState({
    buffers: [],
    tabs: [],
    activePath: null,
    projectRoot: 'x',
    restoredDirtyCount: 0,
  })
})

describe('StructureLens', () => {
  it('opens the project map', async () => {
    render(<StructureLens />)
    await userEvent.click(screen.getByRole('button', { name: /Project map/ }))
    const s = useWorkspaceStore.getState()
    expect(s.tabs.some((t) => t.kind === 'project-map')).toBe(true)
    expect(s.activePath).toBe('project-map')
  })

  it('collapses pipeline groups by default (flowgroups hidden until expanded)', async () => {
    render(<StructureLens />)
    // Collapsed by default → the flowgroup row is not rendered…
    expect(screen.queryByRole('button', { name: 'customers' })).not.toBeInTheDocument()
    // …expanding the pipeline group reveals its flowgroups.
    await userEvent.click(screen.getByRole('button', { name: 'bronze' }))
    expect(screen.getByRole('button', { name: 'customers' })).toBeInTheDocument()
  })

  it('shows a flowgroup under its pipeline with kind dots and opens an entity tab without forcing a view', async () => {
    // StructureLens must NOT pass an explicit `view` — it defers to the store
    // default (the VIEW-MODEL CONTRACT), so we capture the exact call args.
    const openEntityTab = vi.fn()
    useWorkspaceStore.setState({ openEntityTab })
    render(<StructureLens />)
    // Pipeline groups are collapsed by default → expand bronze to reveal its
    // flowgroup, with its own load + write kind dots.
    await userEvent.click(screen.getByRole('button', { name: 'bronze' }))
    const customersRow = screen.getByRole('button', { name: 'customers' })
    expect(within(customersRow).getByTitle('load')).toBeInTheDocument()
    expect(within(customersRow).getByTitle('write')).toBeInTheDocument()

    await userEvent.click(customersRow)
    // Exactly three args — no trailing `{ view: 'form' }` override.
    expect(openEntityTab).toHaveBeenCalledWith(
      'bronze',
      'customers',
      'pipelines/bronze/customers.yaml',
    )
  })

  it('pins Config and Resources below the single scrolling pipelines region', () => {
    render(<StructureLens />)
    const scroll = screen.getByTestId('explorer-pipelines')
    const pinned = screen.getByTestId('explorer-pinned')
    // The pipelines tree is the only content in the scroll region…
    expect(scroll.className).toContain('overflow-auto')
    expect(within(scroll).getByText('Pipelines')).toBeInTheDocument()
    expect(within(scroll).queryByText('Config')).not.toBeInTheDocument()
    expect(within(scroll).queryByText('Resources')).not.toBeInTheDocument()
    // …while Config + Resources live in the pinned bottom region (flex-none;
    // it gains a local overflow fallback only on a very short viewport).
    expect(pinned.className).toContain('flex-none')
    expect(within(pinned).getByText('Config')).toBeInTheDocument()
    expect(within(pinned).getByText('Resources')).toBeInTheDocument()
    expect(within(pinned).getByRole('button', { name: 'Project' })).toBeInTheDocument()
  })

  it('renders a Search pipelines input in the fixed top region', () => {
    render(<StructureLens />)
    const top = screen.getByTestId('explorer-top')
    expect(within(top).getByRole('textbox', { name: 'Search pipelines' })).toBeInTheDocument()
  })

  it('filters the pipelines tree by the search query', async () => {
    render(<StructureLens />)
    // Groups are collapsed by default → the group rows show, flowgroups hidden.
    expect(screen.getByRole('button', { name: 'bronze' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'silver' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'customers' })).not.toBeInTheDocument()

    const input = screen.getByRole('textbox', { name: 'Search pipelines' })
    await userEvent.type(input, 'custom')
    // A search force-expands the matching group and drops non-matching ones.
    expect(screen.getByRole('button', { name: 'customers' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'accounts' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'silver' })).not.toBeInTheDocument()

    // Clearing the query restores both (collapsed) pipeline groups.
    await userEvent.clear(input)
    expect(screen.getByRole('button', { name: 'bronze' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'silver' })).toBeInTheDocument()
  })

  it('shows every pipeline when sandbox scope is null (sandbox off)', () => {
    render(<StructureLens />)
    expect(screen.getByRole('button', { name: 'bronze' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'silver' })).toBeInTheDocument()
  })

  it('narrows the tree to the active sandbox scope, hiding out-of-scope pipelines', () => {
    scopeRef.current = new Set(['bronze'])
    render(<StructureLens />)
    expect(screen.getByRole('button', { name: 'bronze' })).toBeInTheDocument()
    // silver is out of scope → its group and flowgroup are gone entirely.
    expect(screen.queryByRole('button', { name: 'silver' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'accounts' })).not.toBeInTheDocument()
  })

  it('opens lhp.yaml as a structured config tab from the Config Project row', async () => {
    render(<StructureLens />)
    await userEvent.click(screen.getByRole('button', { name: 'Project' }))
    const cfg = useWorkspaceStore.getState().tabs.find((t) => t.kind === 'config')
    expect(cfg).toMatchObject({
      kind: 'config',
      path: 'lhp.yaml',
      configKind: 'project',
      view: 'form',
    })
    expect(useWorkspaceStore.getState().activePath).toBe('config:lhp.yaml')
  })

  it('opens a pipeline config file as a structured config tab (configKind pipeline)', async () => {
    render(<StructureLens />)
    // The Pipeline config group is expanded by default → its file is visible.
    await userEvent.click(screen.getByRole('button', { name: 'pipeline_config_dev.yaml' }))
    const cfg = useWorkspaceStore.getState().tabs.find((t) => t.kind === 'config')
    expect(cfg).toMatchObject({
      kind: 'config',
      path: 'config/pipeline_config_dev.yaml',
      configKind: 'pipeline',
      view: 'form',
    })
  })

  it('opens a preset as a ResourceTab (path resolved from the tree)', async () => {
    render(<StructureLens />)
    await userEvent.click(screen.getByRole('button', { name: /Presets/ }))
    await userEvent.click(screen.getByRole('button', { name: 'bronze_layer' }))
    const res = useWorkspaceStore.getState().tabs.find((t) => t.kind === 'resource')
    expect(res).toMatchObject({
      kind: 'resource',
      resourceKind: 'preset',
      name: 'bronze_layer',
      filePath: 'presets/bronze_layer.yaml',
    })
  })

  it('opens substitutions/<env>.yaml from an Environments row', async () => {
    render(<StructureLens />)
    await userEvent.click(screen.getByRole('button', { name: /Environments/ }))
    await userEvent.click(screen.getByRole('button', { name: 'dev' }))
    expect(
      useWorkspaceStore.getState().buffers.some((b) => b.path === 'substitutions/dev.yaml'),
    ).toBe(true)
  })
})
