import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { TooltipProvider } from '@/components/ui/tooltip'
import { ConfigFormView } from '../ConfigFormView'
import type { ConfigKind } from '../../../store/workspaceStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useDocumentStore } from '../../../store/documentStore'
import { useLayoutStore } from '../../../store/layoutStore'
import { installRadixStubs } from '../../config/shared/__tests__/configFormTestSupport'

vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() } }))

// ── ConfigFormView — config surfaces re-hosted on the document core ──
//
// Drives the REAL stack (ConfigFormView → documentStore → yaml-doc → section
// editors) with realistic commented YAML seeded into a workspace buffer.
// There is no PUT: a field commit pushes byte-surgical serialized text into
// the buffer, so byte-preservation is asserted against buffer.content — what
// a ⌘S would write.

const SCHEMA_KIND: Record<ConfigKind, string> = {
  project: 'project',
  pipeline: 'pipeline_config',
  job: 'job_config',
}

function resetStores(): void {
  useDocumentStore.setState({ docs: {} })
  useWorkspaceStore.setState({
    buffers: [],
    tabs: [],
    activePath: null,
    projectRoot: null,
    restoredDirtyCount: 0,
  })
  useLayoutStore.setState({ viewerMode: false })
}

function bufferContent(path: string): string {
  return useWorkspaceStore.getState().buffers.find((b) => b.path === path)?.content ?? ''
}

function renderView(
  path: string,
  configKind: ConfigKind,
  content: string,
  opts: { viewer?: boolean } = {},
): void {
  resetStores()
  if (opts.viewer) useLayoutStore.setState({ viewerMode: true })
  useWorkspaceStore
    .getState()
    .openBuffer(path, { content, originalContent: content, exists: true, etag: 'e0' })
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  qc.setQueryData(['schema', SCHEMA_KIND[configKind]], {})
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>
      <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
    </QueryClientProvider>
  )
  render(<ConfigFormView tabId={`config:${path}`} path={path} configKind={configKind} />, {
    wrapper,
  })
}

beforeEach(() => {
  installRadixStubs()
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.reject(new Error('no network in ConfigFormView tests'))),
  )
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('ConfigFormView — project surface', () => {
  const PROJECT = `# LakehousePlumber project
name: acme # inline note
version: "1.0"
author: Data Team
`

  it('renders the project sections and round-trips a field edit into the buffer, comments preserved', async () => {
    renderView('lhp.yaml', 'project', PROJECT)
    const author = await screen.findByLabelText('Author')
    expect(author).toHaveValue('Data Team')

    const user = userEvent.setup()
    await user.clear(author)
    await user.type(author, 'Platform Team')
    await user.tab()

    await waitFor(() => expect(bufferContent('lhp.yaml')).toContain('author: Platform Team'))
    const saved = bufferContent('lhp.yaml')
    // Only the author line changed; the header + inline comments survive.
    expect(saved).toContain('# LakehousePlumber project')
    expect(saved).toContain('name: acme # inline note')
    expect(saved).not.toContain('author: Data Team')
  })
})

describe('ConfigFormView — multi-document pipeline_config surface', () => {
  const MULTI = `# defaults doc
project_defaults:
  catalog: main
---
# pipeline doc
pipeline: bronze
schema: raw # keep this
`

  it('lists both documents in the rail and edits docIndex>0 byte-anchored', async () => {
    renderView('config/pipeline_config.yaml', 'pipeline', MULTI)
    const nav = await screen.findByRole('navigation', { name: 'Configuration documents' })
    // Rail lists both docs: the defaults tier and the bronze pipeline.
    expect(within(nav).getByText('Project defaults')).toBeInTheDocument()
    const bronzeRow = within(nav).getByRole('button', { name: /bronze/ })

    const user = userEvent.setup()
    await user.click(bronzeRow)

    // Edit a field on the SECOND document (the bronze pipeline).
    const catalog = await screen.findByLabelText('Catalog')
    await user.type(catalog, 'bronze_cat')
    await user.tab()

    await waitFor(() =>
      expect(bufferContent('config/pipeline_config.yaml')).toContain('catalog: bronze_cat'),
    )
    // Both documents are byte-anchored: doc 0 and every comment are intact,
    // the edit is a pure splice at the end of doc 1.
    expect(bufferContent('config/pipeline_config.yaml')).toBe(MULTI + 'catalog: bronze_cat\n')
  })
})

describe('ConfigFormView — validator issues render but do not block edits', () => {
  const WITH_ISSUE = `pipeline: p1
configuration:
  spark.x: 42
`

  it('shows the per-field issue yet still commits an unrelated edit (no blocking)', async () => {
    renderView('config/pipeline_config.yaml', 'pipeline', WITH_ISSUE)
    // A non-string configuration value renders a per-field issue.
    expect(await screen.findByText('not text')).toBeInTheDocument()
    // No SaveBar exists to disable — saving is the buffer's ⌘S path.
    expect(screen.queryByRole('button', { name: 'Save' })).not.toBeInTheDocument()

    // An unrelated edit still commits despite the validation error.
    const user = userEvent.setup()
    const catalog = await screen.findByLabelText('Catalog')
    await user.type(catalog, 'analytics')
    await user.tab()
    await waitFor(() =>
      expect(bufferContent('config/pipeline_config.yaml')).toContain('catalog: analytics'),
    )
  })
})

describe('ConfigFormView — degraded (parse-error) mode', () => {
  const OK = `name: acme
author: Data Team
`

  it('shows the degraded banner and pauses form editing when the buffer stops parsing', async () => {
    renderView('lhp.yaml', 'project', OK)
    expect(await screen.findByLabelText('Author')).toBeInTheDocument()
    expect(screen.queryByTestId('config-degraded-banner')).not.toBeInTheDocument()

    // Simulate the YAML view pushing invalid text (as Monaco→updateContent would).
    await act(async () => {
      useWorkspaceStore.getState().updateContent('lhp.yaml', 'name: acme\nauthor: [unterminated\n')
    })

    await waitFor(() =>
      expect(screen.getByTestId('config-degraded-banner')).toBeInTheDocument(),
    )
    // The form is gone (editing paused) — YAML is the fix-it surface.
    expect(screen.queryByLabelText('Author')).not.toBeInTheDocument()
  })
})

describe('ConfigFormView — viewer lens', () => {
  const PROJECT = `name: acme
author: Data Team
`

  it('renders read-only: fields are inert and edits never reach the buffer', async () => {
    renderView('lhp.yaml', 'project', PROJECT, { viewer: true })
    const author = await screen.findByLabelText('Author')

    // The re-host wrapper marks the whole body read-only.
    const view = screen.getByTestId('config-form-view')
    expect(view.querySelector('[aria-disabled="true"]')).not.toBeNull()

    const user = userEvent.setup()
    await user.clear(author)
    await user.type(author, 'Someone Else')
    await user.tab()

    // The viewer mutate gate is a no-op: the buffer is untouched.
    expect(bufferContent('lhp.yaml')).toBe(PROJECT)
  })
})
