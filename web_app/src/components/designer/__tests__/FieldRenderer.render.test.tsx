import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { Mock } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { FieldSpec } from '../specs/types'
import type { DesignerMutator } from '../formModel'
import type { CompanionStatus } from '../useCompanionFile'

// The four extended primitives reach outside the DOM: `tokenComplete` resolves
// env tokens through the ui store + react-query env resolver, and the file-ref
// path checks companion-file existence. Mock all three at the hook boundary —
// exactly as the TokenAutocomplete / DualSourceField / FileRefField suites do —
// so a bare FieldRenderer needs no provider tree or network.
vi.mock('@/hooks/useEnvironments', () => ({ useEnvironmentResolved: vi.fn() }))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))

// Control the companion status per test while keeping the REAL
// `companionCheckablePath`, so a project-relative ref still resolves to a path.
const useCompanionFileMock = vi.fn()
vi.mock('../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../useCompanionFile')>()
  return { ...actual, useCompanionFile: (path: string | null) => useCompanionFileMock(path) }
})

import { useEnvironmentResolved } from '@/hooks/useEnvironments'
import { FieldRenderer } from '../FieldRenderer'
import { listActions, parseFlowgroupFile, selectFlowgroupAt } from '@/lib/flowgroup-doc'
import type { FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import { readPath } from '../specs/helpers'

const resolvedMock = useEnvironmentResolved as unknown as ReturnType<typeof vi.fn>

beforeEach(() => {
  vi.clearAllMocks()
  resolvedMock.mockReturnValue({ data: { tokens: { bronze_schema: 'acme_bronze' } } })
  useCompanionFileMock.mockReturnValue({ status: 'unavailable' as CompanionStatus, create: vi.fn() })
  // The default (non-segmented) enum branch renders a Radix Select that leans on
  // pointer-capture / scroll APIs jsdom lacks; stubbing them keeps the RED render
  // of the un-extended enum case from crashing before the assertion runs.
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

function renderField(
  field: FieldSpec,
  raw: Record<string, unknown>,
  extra?: { tokenComplete?: boolean },
) {
  return render(
    <FieldRenderer
      field={field}
      raw={raw}
      actionId="a1"
      commit={vi.fn()}
      issue={undefined}
      disabled={false}
      saving={false}
      onEditCode={vi.fn()}
      tokenComplete={extra?.tokenComplete}
    />,
  )
}

describe('FieldRenderer — extended widget dispatch', () => {
  it('enum with display:"segmented" renders a radiogroup, not a combobox', () => {
    renderField(
      {
        path: ['mode'],
        label: 'Write mode',
        widget: 'enum',
        display: 'segmented',
        options: ['append', 'overwrite', 'merge'],
      },
      { mode: 'append' },
    )
    expect(screen.getByRole('radiogroup', { name: 'Write mode' })).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).toBeNull()
  })

  it('dualSource renders two prefilled labeled inputs (Table A / Table B)', () => {
    renderField({ path: ['source'], label: 'Sources', widget: 'dualSource' }, { source: ['a', 'b'] })
    expect(screen.getByLabelText('Table A')).toHaveValue('a')
    expect(screen.getByLabelText('Table B')).toHaveValue('b')
  })

  it('a file-ref field renders FileRefField (Browse / New), not the inline editor', () => {
    renderField(
      { path: ['module_path'], label: 'Module', widget: 'text', fileRef: { accept: ['.py'] } },
      { module_path: 'transforms/x.py' },
    )
    expect(screen.getByRole('button', { name: /browse/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /new/i })).toBeInTheDocument()
  })

  it('threads tokenComplete to a text field: the $-token trigger is present only when true', () => {
    const field: FieldSpec = { path: ['comment'], label: 'Comment', widget: 'text' }

    const { unmount } = renderField(field, {}, { tokenComplete: true })
    expect(screen.getByRole('button', { name: /insert token/i })).toBeInTheDocument()
    unmount()

    renderField(field, {})
    expect(screen.queryByRole('button', { name: /insert token/i })).toBeNull()
  })
})

// ── enum discriminator prune wiring (Task 3.1c, Part 1) ──────
//
// A raw carrying `mode: quarantine` + a `quarantine:` block; the enum field's
// onSet is exercised via the segmented display (deterministic radio clicks),
// and the captured mutator is replayed against a fresh parsed doc — mirroring
// the shell's record-and-replay so the same seam is proven for both callers.
describe('FieldRenderer — enum discriminator prune wiring', () => {
  const RAW_YAML = `pipeline: p
flowgroup: fg
actions:
  - name: dq
    type: transform
    transform_type: data_quality
    mode: quarantine
    quarantine:
      dlq_table: cat.sch.dlq
      source_table: cat.sch.src
`
  const freshDoc = (): FlowgroupDocHandle =>
    selectFlowgroupAt(parseFlowgroupFile(RAW_YAML), 0) as FlowgroupDocHandle

  const dqRaw = () => ({
    mode: 'quarantine',
    quarantine: { dlq_table: 'cat.sch.dlq', source_table: 'cat.sch.src' },
  })

  function renderMode(field: FieldSpec, commit: Mock<(mutator: DesignerMutator) => void>) {
    return render(
      <FieldRenderer
        field={field}
        raw={dqRaw()}
        actionId="dq"
        commit={commit}
        issue={undefined}
        disabled={false}
        saving={false}
        onEditCode={vi.fn()}
      />,
    )
  }

  it('branchPaths set → commits applyDiscriminatorChange: sets the value AND prunes the inactive branch', async () => {
    const commit = vi.fn<(mutator: DesignerMutator) => void>()
    renderMode(
      {
        path: ['mode'],
        label: 'Mode',
        widget: 'enum',
        display: 'segmented',
        options: ['dqe', 'quarantine'],
        branchPaths: { dqe: [], quarantine: [['quarantine']] },
      },
      commit,
    )
    await userEvent.setup().click(screen.getByRole('radio', { name: 'dqe' }))

    expect(commit).toHaveBeenCalledTimes(1)
    const doc = freshDoc()
    commit.mock.calls[0][0](doc)
    const raw = listActions(doc)[0].raw
    expect(raw.mode).toBe('dqe')
    expect(readPath(raw, ['quarantine'])).toBeUndefined()
  })

  it('branchPaths absent → still commits a plain set (no prune of the stale branch)', async () => {
    const commit = vi.fn<(mutator: DesignerMutator) => void>()
    renderMode(
      { path: ['mode'], label: 'Mode', widget: 'enum', display: 'segmented', options: ['dqe', 'quarantine'] },
      commit,
    )
    await userEvent.setup().click(screen.getByRole('radio', { name: 'dqe' }))

    expect(commit).toHaveBeenCalledTimes(1)
    const doc = freshDoc()
    commit.mock.calls[0][0](doc)
    const raw = listActions(doc)[0].raw
    expect(raw.mode).toBe('dqe')
    expect(readPath(raw, ['quarantine'])).toBeDefined()
  })
})
