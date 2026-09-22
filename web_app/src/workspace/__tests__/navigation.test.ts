import { beforeEach, describe, expect, it } from 'vitest'
import { decodeTab, encodeTab, useNavigationStore } from '../navigation'

describe('workspace navigation', () => {
  beforeEach(() => useNavigationStore.getState().reset())
  it('preserves entity view identity in a shareable link', () => {
    const tab = { kind: 'entity', pipeline: 'silver', flowgroup: 'customers', filePath: 'pipelines/customers.yaml', docKind: 'flowgroup', view: 'code' } as const
    expect(decodeTab(encodeTab(tab))).toEqual(tab)
  })
  it('rejects malformed and unsupported links', () => {
    for (const link of ['null', '[]', '{bad', '{"kind":"config","path":4}', '{"kind":"unknown"}']) expect(decodeTab(link)).toBeNull()
  })
  it('keeps forward history while revisiting, then truncates it on new navigation', () => {
    const s = useNavigationStore.getState()
    s.visit({ kind: 'project-map' }); s.visit({ kind: 'pipeline-dag', pipeline: 'one' }); s.visit({ kind: 'table-detail', fqn: 'db.t' })
    const previous = s.move(-1)!
    s.visit(previous)
    expect(useNavigationStore.getState().entries).toHaveLength(3)
    s.visit({ kind: 'file', path: 'README.md' })
    expect(s.move(1)).toBeNull()
    expect(useNavigationStore.getState().entries.at(-1)).toEqual({ kind: 'file', path: 'README.md' })
  })
})
