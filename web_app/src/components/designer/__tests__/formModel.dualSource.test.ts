import { describe, expect, it } from 'vitest'
import { dualSourceSlots, dualSourceWrite } from '../formModel'
import { readPath } from '../specs/helpers'
import {
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
  setActionField,
  deleteActionField,
} from '@/lib/flowgroup-doc'

const ROW_COUNT_YAML = `pipeline: perf
flowgroup: fg_test
actions:
  - name: rc_compare
    type: test
    test_type: row_count
    source:
      - t_a
      - t_b
`

/** Re-parse from serialized text and read the action's raw mapping back. */
function reparsedSource(handle: ReturnType<typeof parseFlowgroupFile>): unknown {
  const fg = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), 'fg_test')
  const action = listActions(fg!).find((a) => a.name === 'rc_compare')
  return readPath(action!.raw, ['source'])
}

describe('formModel — dualSource slots (read)', () => {
  it('exposes the two elements of the list at the path', () => {
    expect(dualSourceSlots({ source: ['t_a', 't_b'] }, ['source'])).toEqual(['t_a', 't_b'])
  })

  it('a missing / non-array value yields two empty slots', () => {
    expect(dualSourceSlots({}, ['source'])).toEqual([undefined, undefined])
    expect(dualSourceSlots({ source: 'not_a_list' }, ['source'])).toEqual([undefined, undefined])
  })

  it('a length-1 array fills slot 0 only', () => {
    expect(dualSourceSlots({ source: ['t_a'] }, ['source'])).toEqual(['t_a', undefined])
  })

  it('round-trip fidelity: a legacy 3-item list surfaces only slots 0 and 1 without throwing', () => {
    expect(dualSourceSlots({ source: ['t_a', 't_b', 't_c'] }, ['source'])).toEqual(['t_a', 't_b'])
  })

  it('non-string elements degrade to undefined (type-honest slots)', () => {
    expect(dualSourceSlots({ source: [1, 't_b'] }, ['source'])).toEqual([undefined, 't_b'])
  })
})

describe('formModel — dualSource write (compose whole pair / prune-when-both-empty)', () => {
  it('setting one slot composes the whole two-item array', () => {
    expect(dualSourceWrite(['t_a', 't_b'], 1, 't_c')).toEqual({ kind: 'set', value: ['t_a', 't_c'] })
    expect(dualSourceWrite(['t_a', 't_b'], 0, 't_z')).toEqual({ kind: 'set', value: ['t_z', 't_b'] })
  })

  it('setting the second slot from an empty pair fills slot 0 with an empty placeholder', () => {
    expect(dualSourceWrite([undefined, undefined], 1, 't_b')).toEqual({
      kind: 'set',
      value: ['', 't_b'],
    })
  })

  it('clearing the last non-empty slot deletes the key', () => {
    expect(dualSourceWrite(['t_a', undefined], 0, '')).toEqual({ kind: 'delete' })
    expect(dualSourceWrite(['', 't_b'], 1, '')).toEqual({ kind: 'delete' })
    expect(dualSourceWrite([undefined, undefined], 0, '')).toEqual({ kind: 'delete' })
  })
})

describe('formModel — dualSource is consumable by the shell mutators (real doc round-trip)', () => {
  it("set writes the whole 2-item array; slot 1 → 't_c' yields ['t_a','t_c']", () => {
    const handle = parseFlowgroupFile(ROW_COUNT_YAML)
    expect(handle.errors).toEqual([])
    const fg = selectFlowgroup(handle, 'fg_test')
    const before = dualSourceSlots(
      listActions(fg!).find((a) => a.name === 'rc_compare')!.raw,
      ['source'],
    )
    expect(before).toEqual(['t_a', 't_b'])

    const write = dualSourceWrite(before, 1, 't_c')
    expect(write.kind).toBe('set')
    if (write.kind === 'set') setActionField(fg!, 'rc_compare', ['source'], write.value)

    expect(reparsedSource(handle)).toEqual(['t_a', 't_c'])
  })

  it('delete removes the key when both slots resolve empty', () => {
    const handle = parseFlowgroupFile(ROW_COUNT_YAML)
    const fg = selectFlowgroup(handle, 'fg_test')
    // First clear slot 0, leaving ['', 't_b'].
    setActionField(fg!, 'rc_compare', ['source'], ['', 't_b'])
    // Then clear slot 1 → both empty → delete signal.
    const write = dualSourceWrite(['', 't_b'], 1, '')
    expect(write.kind).toBe('delete')
    if (write.kind === 'delete') deleteActionField(fg!, 'rc_compare', ['source'])

    expect(reparsedSource(handle)).toBeUndefined()
  })
})
