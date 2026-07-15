import { describe, expect, it } from 'vitest'
import { applyDiscriminatorChange } from '../formModel'
import { readPath } from '../specs/helpers'
import type { BranchPathMap } from '../specs/types'
import {
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
  setActionField,
} from '@/lib/flowgroup-doc'

// A data_quality transform whose `mode` discriminator owns a `quarantine:`
// block. Switching mode away from 'quarantine' must prune that block (the
// Python validator rejects a stray `quarantine:` when mode != 'quarantine').
const QUARANTINE_YAML = `pipeline: perf
flowgroup: fg_test
actions:
  - name: dq_check
    type: transform
    transform_type: data_quality
    mode: quarantine
    quarantine:
      dlq_table: cat.sch.dlq
      source_table: cat.sch.src
`

// A sql transform holding inline `sql`. The inline⊕file toggle has no backing
// key: switching inline→file prunes `sql`; the new branch owns `sql_path`,
// which stays absent until the user fills it.
const INLINE_SQL_YAML = `pipeline: perf
flowgroup: fg_test
actions:
  - name: sql_step
    type: transform
    transform_type: sql
    sql: SELECT * FROM v_src
`

// A discriminator where the inactive branch and the new branch SHARE a path:
// the shared key must survive the switch (only branch-EXCLUSIVE keys prune).
const SHARED_PATH_YAML = `pipeline: perf
flowgroup: fg_test
actions:
  - name: w
    type: write
    write_target:
      type: streaming_table
      mode: cdc
      cdc_config:
        keys: [id]
      shared_note: keep-me
`

/** Re-serialize, re-parse, and read a named action's raw mapping back. */
function reparsedAction(
  handle: ReturnType<typeof parseFlowgroupFile>,
  name: string,
): Record<string, unknown> {
  const fg = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), 'fg_test')
  return listActions(fg!).find((a) => a.name === name)!.raw
}

describe('formModel — applyDiscriminatorChange (prune inactive branch on switch)', () => {
  it('Case A: N-way enum — switching mode quarantine→dqe writes mode and deletes the quarantine block', () => {
    const handle = parseFlowgroupFile(QUARANTINE_YAML)
    expect(handle.errors).toEqual([])
    const fg = selectFlowgroup(handle, 'fg_test')

    const branchPaths: BranchPathMap = {
      quarantine: [['quarantine']],
      dqe: [],
    }
    applyDiscriminatorChange(fg!, 'dq_check', ['mode'], 'dqe', branchPaths)

    const raw = reparsedAction(handle, 'dq_check')
    expect(raw.mode).toBe('dqe')
    expect(readPath(raw, ['quarantine'])).toBeUndefined()
    // Untouched siblings survive.
    expect(raw.transform_type).toBe('data_quality')
  })

  it('Case B: inline⊕file toggle — switching inline→file deletes sql and writes nothing spurious', () => {
    const handle = parseFlowgroupFile(INLINE_SQL_YAML)
    expect(handle.errors).toEqual([])
    const fg = selectFlowgroup(handle, 'fg_test')

    const branchPaths: BranchPathMap = {
      inline: [['sql']],
      file: [['sql_path']],
    }
    // No backing discriminator key → fieldPath is undefined (prune-only).
    applyDiscriminatorChange(fg!, 'sql_step', undefined, 'file', branchPaths)

    const raw = reparsedAction(handle, 'sql_step')
    expect(readPath(raw, ['sql'])).toBeUndefined()
    // The new branch owns sql_path but nothing is written for it.
    expect(readPath(raw, ['sql_path'])).toBeUndefined()
    expect(raw.transform_type).toBe('sql')
  })

  it('a path owned by BOTH the inactive and the new branch is NOT pruned', () => {
    const handle = parseFlowgroupFile(SHARED_PATH_YAML)
    expect(handle.errors).toEqual([])
    const fg = selectFlowgroup(handle, 'fg_test')

    const shared: (string | number)[] = ['write_target', 'shared_note']
    const branchPaths: BranchPathMap = {
      cdc: [['write_target', 'cdc_config'], shared],
      standard: [shared],
    }
    applyDiscriminatorChange(fg!, 'w', ['write_target', 'mode'], 'standard', branchPaths)

    const raw = reparsedAction(handle, 'w')
    expect(readPath(raw, ['write_target', 'mode'])).toBe('standard')
    // Branch-exclusive cdc_config is pruned…
    expect(readPath(raw, ['write_target', 'cdc_config'])).toBeUndefined()
    // …but the path shared with the new 'standard' branch survives.
    expect(readPath(raw, ['write_target', 'shared_note'])).toBe('keep-me')
  })

  it('is consumable by a commit-style closure over the doc handle', () => {
    const handle = parseFlowgroupFile(QUARANTINE_YAML)
    const fg = selectFlowgroup(handle, 'fg_test')
    // Mirrors the shell's `commit((doc) => …)` usage.
    const mutate = (doc: NonNullable<typeof fg>) => {
      setActionField(doc, 'dq_check', ['description'], 'noted')
      applyDiscriminatorChange(doc, 'dq_check', ['mode'], 'dqe', { quarantine: [['quarantine']] })
    }
    mutate(fg!)

    const raw = reparsedAction(handle, 'dq_check')
    expect(raw.mode).toBe('dqe')
    expect(readPath(raw, ['quarantine'])).toBeUndefined()
    expect(raw.description).toBe('noted')
  })
})
