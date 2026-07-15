import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { Mock } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { FieldSpec } from '../specs/types'
import type { DesignerMutator } from '../formModel'
import type { CompanionStatus } from '../useCompanionFile'

// OneOfToggle reaches outside the DOM only through the file-backing branch
// (FileRefField → companion-file existence) and the optional `tokenComplete`
// env resolver. Mock all three at the hook boundary — exactly as the
// FieldRenderer / FileRefField suites do — so the widget needs no provider
// tree or network.
vi.mock('@/hooks/useEnvironments', () => ({ useEnvironmentResolved: vi.fn() }))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))

const useCompanionFileMock = vi.fn()
vi.mock('../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../useCompanionFile')>()
  return { ...actual, useCompanionFile: (path: string | null) => useCompanionFileMock(path) }
})

import { useEnvironmentResolved } from '@/hooks/useEnvironments'
import { OneOfToggle } from '../OneOfToggle'
import { listActions, parseFlowgroupFile, selectFlowgroupAt } from '@/lib/flowgroup-doc'
import type { FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import { readPath } from '../specs/helpers'

const resolvedMock = useEnvironmentResolved as unknown as ReturnType<typeof vi.fn>

beforeEach(() => {
  vi.clearAllMocks()
  resolvedMock.mockReturnValue({ data: { tokens: {} } })
  useCompanionFileMock.mockReturnValue({ status: 'unavailable' as CompanionStatus, create: vi.fn() })
})

// A 2-option inline-SQL ⊕ file toggle. Neither branch has a backing
// discriminator key; the toggle mode is derived from which key is present.
const TWO_OPTION: FieldSpec = {
  path: ['__sqlSource'],
  label: 'SQL source',
  widget: 'oneOfToggle',
  oneOf: {
    options: [
      { value: 'inline', label: 'Inline SQL', path: ['sql'], backing: 'inline', language: 'sql' },
      { value: 'file', label: 'From file', path: ['sql_path'], backing: 'file', accept: ['.sql'] },
    ],
  },
}

// A 3-option materialized-view-style source ⊕ sql ⊕ sql_path toggle.
const THREE_OPTION: FieldSpec = {
  path: ['__mvSource'],
  label: 'Materialized view source',
  widget: 'oneOfToggle',
  oneOf: {
    options: [
      { value: 'source', label: 'From source', path: ['source'], backing: 'text' },
      { value: 'sql', label: 'Inline SQL', path: ['sql'], backing: 'inline', language: 'sql' },
      { value: 'sql_path', label: 'SQL file', path: ['sql_path'], backing: 'file', accept: ['.sql'] },
    ],
  },
}

const SQL_YAML = `pipeline: p
flowgroup: fg
actions:
  - name: t
    type: transform
    transform_type: sql
    sql: SELECT 1
`

const MV_YAML = `pipeline: p
flowgroup: fg
actions:
  - name: mv
    type: transform
    transform_type: sql
    source: v_src
    sql: SELECT 1
    sql_path: q/x.sql
`

const freshDoc = (yaml: string): FlowgroupDocHandle =>
  selectFlowgroupAt(parseFlowgroupFile(yaml), 0) as FlowgroupDocHandle

function renderToggle(
  spec: FieldSpec,
  raw: Record<string, unknown>,
  opts?: { actionId?: string; commit?: Mock<(mutator: DesignerMutator) => void> },
) {
  const commit = opts?.commit ?? vi.fn<(mutator: DesignerMutator) => void>()
  render(
    <OneOfToggle
      spec={spec}
      raw={raw}
      actionId={opts?.actionId ?? 't'}
      commit={commit}
      onEditCode={vi.fn()}
      disabled={false}
    />,
  )
  return commit
}

describe('OneOfToggle — mode derivation + branch controls', () => {
  it('derives the active mode from the branch whose key is present', () => {
    renderToggle(TWO_OPTION, { transform_type: 'sql', sql: 'SELECT 1' })

    // 'Inline SQL' segment is selected because `sql` is present.
    expect(screen.getByRole('radio', { name: 'Inline SQL' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('radio', { name: 'From file' })).toHaveAttribute('data-state', 'off')
    // The inline code field shows the present value.
    expect(screen.getByRole('textbox', { name: 'Inline SQL' })).toHaveValue('SELECT 1')
  })

  it('defaults to the first option when no branch key is present', () => {
    renderToggle(TWO_OPTION, { transform_type: 'sql' })
    expect(screen.getByRole('radio', { name: 'Inline SQL' })).toHaveAttribute('data-state', 'on')
  })
})

describe('OneOfToggle — switch prunes the inactive branch (replay on fresh doc)', () => {
  it('inline → file: commits a mutator that DELETES sql and writes nothing spurious', async () => {
    const commit = renderToggle(TWO_OPTION, { transform_type: 'sql', sql: 'SELECT 1' })
    await userEvent.setup().click(screen.getByRole('radio', { name: 'From file' }))

    expect(commit).toHaveBeenCalledTimes(1)
    const doc = freshDoc(SQL_YAML)
    commit.mock.calls[0][0](doc)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, ['sql'])).toBeUndefined() // pruned
    expect(readPath(raw, ['sql_path'])).toBeUndefined() // new branch owns it, unset
    expect(raw.transform_type).toBe('sql') // untouched sibling survives

    // The file-backing control is now rendered.
    expect(screen.getByRole('button', { name: /browse/i })).toBeInTheDocument()
  })

  it('3-way source/sql/sql_path → sql: prunes source AND sql_path, keeps sql', async () => {
    const commit = renderToggle(
      THREE_OPTION,
      { transform_type: 'sql', source: 'v_src', sql: 'SELECT 1', sql_path: 'q/x.sql' },
      { actionId: 'mv' },
    )
    // Three segments render; the first present branch ('source') is active.
    expect(screen.getAllByRole('radio')).toHaveLength(3)
    expect(screen.getByRole('radio', { name: 'From source' })).toHaveAttribute('data-state', 'on')

    await userEvent.setup().click(screen.getByRole('radio', { name: 'Inline SQL' }))

    expect(commit).toHaveBeenCalledTimes(1)
    const doc = freshDoc(MV_YAML)
    commit.mock.calls[0][0](doc)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, ['sql'])).toBe('SELECT 1') // kept (new active branch)
    expect(readPath(raw, ['source'])).toBeUndefined() // pruned
    expect(readPath(raw, ['sql_path'])).toBeUndefined() // pruned
  })
})

describe('OneOfToggle — active-branch value edits', () => {
  it('editing the inline field commits at the active branch path', () => {
    const commit = renderToggle(TWO_OPTION, { transform_type: 'sql', sql: 'SELECT 1' })
    const textbox = screen.getByRole('textbox', { name: 'Inline SQL' })
    fireEvent.change(textbox, { target: { value: 'SELECT 2' } })
    fireEvent.blur(textbox)

    expect(commit).toHaveBeenCalledTimes(1)
    const doc = freshDoc(SQL_YAML)
    commit.mock.calls[0][0](doc)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, ['sql'])).toBe('SELECT 2')
  })
})
