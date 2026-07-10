import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  fetchMock,
  lineDiff,
  renderProjectForm,
  serveProject,
} from './projectFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Byte-preservation guarantees, user-visible ───────────────
//
// These tests drive the REAL form (fields → mutate → yaml-doc → PUT body)
// and assert the feature's core promise: a save only ever touches the
// lines the user edited; everything else — comments, quoting, unknown
// keys, key order — survives byte-identically.

const FIXTURE = `# LakehousePlumber project configuration
name: acme_lakehouse # the project
version: "1.0"
author: Data Team

# Which pipeline configs to include
include:
  - pipelines/*.yaml

operational_metadata:
  columns:
    _ingested_at: F.current_timestamp()
    _source_file: F.col('_metadata.file_path')
  defaults:
    strategy: append
    retention_days: 30

event_log:
  enabled: true
  catalog: main # governance catalog
  schema: lhp_ops

sandbox:
  table_pattern: "{namespace}_{table}"
`

const COMMENTS = [
  '# LakehousePlumber project configuration',
  '# the project',
  '# Which pipeline configs to include',
  '# governance catalog',
]

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

async function save(user: ReturnType<typeof userEvent.setup>): Promise<void> {
  const saveButton = screen.getByRole('button', { name: 'Save' })
  await waitFor(() => expect(saveButton).toBeEnabled())
  await user.click(saveButton)
  await waitFor(() =>
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === 'PUT')).toBe(true),
  )
}

describe('ProjectConfigForm — byte preservation', () => {
  it('FLAGSHIP: editing one scalar saves a body differing in exactly one line; comments survive', async () => {
    const files = serveProject(FIXTURE)
    await renderProjectForm()
    const user = userEvent.setup()

    const author = await screen.findByLabelText('Author')
    await user.clear(author)
    await user.type(author, 'Platform Team')
    await user.tab()

    await save(user)

    const body = files.putBodies()[0]!
    const diff = lineDiff(FIXTURE, body)
    expect(diff).toHaveLength(1)
    expect(diff[0]!.before).toBe('author: Data Team')
    expect(diff[0]!.after).toBe('author: Platform Team')
    for (const comment of COMMENTS) expect(body).toContain(comment)
  })

  it('editing one bare-string shorthand column leaves the other column byte-identical', async () => {
    const files = serveProject(FIXTURE)
    await renderProjectForm()
    const user = userEvent.setup()

    const column = await screen.findByRole('group', { name: '_source_file' })
    const expression = within(column).getByLabelText('Expression')
    await user.clear(expression)
    await user.type(expression, 'F.col("_metadata.file_name")')
    await user.tab()

    await save(user)

    const body = files.putBodies()[0]!
    const diff = lineDiff(FIXTURE, body)
    expect(diff).toHaveLength(1)
    expect(diff[0]!.before).toContain('_source_file')
    // The untouched shorthand column keeps its exact bytes (still shorthand).
    expect(body).toContain('    _ingested_at: F.current_timestamp()\n')
  })

  it('presence toggle ON splices exactly the minimal skeleton at the end of the root map', async () => {
    const files = serveProject(FIXTURE)
    await renderProjectForm()
    const user = userEvent.setup()

    const toggle = await screen.findByRole('switch', { name: 'Enable UC tagging section' })
    await user.click(toggle)

    await save(user)

    const body = files.putBodies()[0]!
    expect(body).toBe(`${FIXTURE}uc_tagging:\n  enabled: true\n`)
  })

  it('presence toggle OFF deletes the section key with NO residue (no null, no {})', async () => {
    const files = serveProject(FIXTURE)
    await renderProjectForm()
    const user = userEvent.setup()

    const toggle = await screen.findByRole('switch', { name: 'Enable Sandbox section' })
    await user.click(toggle)
    // Confirm the removal dialog.
    const dialog = await screen.findByRole('alertdialog')
    await user.click(within(dialog).getByRole('button', { name: 'Remove section' }))

    await save(user)

    const body = files.putBodies()[0]!
    expect(body).not.toContain('sandbox')
    expect(body).not.toContain('table_pattern')
    expect(body).not.toMatch(/:\s*null/)
    // Comments elsewhere survive the containing-document rewrite.
    for (const comment of COMMENTS) expect(body).toContain(comment)
    expect(body).toContain('_ingested_at: F.current_timestamp()')
  })

  it('key-value map: a non-string value is read-only + warned and stays untouched on save', async () => {
    const files = serveProject(FIXTURE)
    await renderProjectForm()
    const user = userEvent.setup()

    // retention_days (a number) renders locked with a warning badge.
    const retention = await screen.findByLabelText('retention_days value')
    expect(retention).toHaveAttribute('readonly')
    expect(screen.getByText('not text')).toBeInTheDocument()

    // Editing a DIFFERENT row must not coerce it.
    const strategy = screen.getByLabelText('strategy value')
    await user.clear(strategy)
    await user.type(strategy, 'merge')
    await user.tab()

    await save(user)

    const body = files.putBodies()[0]!
    const diff = lineDiff(FIXTURE, body)
    expect(diff).toHaveLength(1)
    expect(diff[0]!.after).toBe('    strategy: merge')
    expect(body).toContain('    retention_days: 30\n')
  })

  it('key-value map: unlocking and editing the non-string row coerces THAT value to a string', async () => {
    const files = serveProject(FIXTURE)
    await renderProjectForm()
    const user = userEvent.setup()

    await user.click(await screen.findByRole('button', { name: 'Edit retention_days as text' }))
    const retention = screen.getByLabelText('retention_days value')
    expect(retention).not.toHaveAttribute('readonly')
    await user.clear(retention)
    await user.type(retention, '45')
    await user.tab()

    await save(user)

    const body = files.putBodies()[0]!
    // Coerced to a string — and quoted, since a plain 45 would re-parse
    // as a number under PyYAML.
    expect(body).toContain('retention_days: "45"')
  })

  it('a bare-null section (`monitoring:`) becomes a map on the first field write', async () => {
    // `monitoring:` with no value is valid (all defaults). Writing a field
    // must replace the null with a map — setIn cannot descend into a null
    // scalar, which is exactly what ProjectFormApi.setField guards.
    const files = serveProject('name: acme\nmonitoring:\n')
    await renderProjectForm()
    const user = userEvent.setup()

    // Disabling monitoring also silences its enabled-requires-event_log
    // errors, so Save is permitted for this fixture.
    await user.click(await screen.findByRole('switch', { name: 'Enabled' }))

    await save(user)

    expect(files.putBodies()[0]).toBe('name: acme\nmonitoring:\n  enabled: false\n')
  })

  it('an empty lhp.yaml gains a document on first edit and saves only what was set', async () => {
    const files = serveProject('')
    await renderProjectForm()
    const user = userEvent.setup()

    const name = await screen.findByLabelText('Name')
    await user.type(name, 'fresh_project')
    await user.tab()

    await save(user)

    expect(files.putBodies()[0]).toBe('name: fresh_project\n')
  })
})
