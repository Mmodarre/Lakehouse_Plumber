import { describe, expect, it } from 'vitest'
import type { ActionSubTypeSpec, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { ucTagsStub, ucTagsSuggestPath, ucTagsTableName } from '../ucTagsFile'
import { writeStreamingTableSpec } from '../write-streaming-table'
import { writeMaterializedViewSpec } from '../write-materialized-view'

const key = (path: YamlPath) => JSON.stringify(path)
function tagsFileField(s: ActionSubTypeSpec): FieldSpec | undefined {
  return s.groups
    .flatMap((g) => g.fields)
    .find((f) => key(f.path) === key(['write_target', 'tags_file']))
}

describe('ucTagsTableName', () => {
  it('returns a clean, trimmed table name', () => {
    expect(ucTagsTableName({ write_target: { table: 'orders' } })).toBe('orders')
    expect(ucTagsTableName({ write_target: { table: '  orders  ' } })).toBe('orders')
  })

  it('returns null for an absent, empty, or non-string table', () => {
    expect(ucTagsTableName({})).toBeNull()
    expect(ucTagsTableName({ write_target: {} })).toBeNull()
    expect(ucTagsTableName({ write_target: { table: '   ' } })).toBeNull()
    expect(ucTagsTableName({ write_target: { table: 42 } })).toBeNull()
  })

  it('returns null for a substitution / template token', () => {
    expect(ucTagsTableName({ write_target: { table: '${bronze_table}' } })).toBeNull()
    expect(ucTagsTableName({ write_target: { table: '{{ table_name }}' } })).toBeNull()
    expect(ucTagsTableName({ write_target: { table: '%{table}' } })).toBeNull()
  })
})

describe('ucTagsSuggestPath', () => {
  it('proposes uc_tags/<table>.yaml for a clean table', () => {
    expect(ucTagsSuggestPath({ write_target: { table: 'orders' } })).toBe('uc_tags/orders.yaml')
  })

  it('returns null when there is no clean table', () => {
    expect(ucTagsSuggestPath({})).toBeNull()
    expect(ucTagsSuggestPath({ write_target: { table: '${t}' } })).toBeNull()
  })
})

describe('ucTagsStub', () => {
  it('prefills the table line when the table is a clean string', () => {
    const stub = ucTagsStub({ write_target: { table: 'orders' } })
    expect(stub).toContain('version: 1.0.0')
    expect(stub).toContain('table: orders')
    expect(stub).toContain('# columns:')
    expect(stub).toContain('#   - name: email')
  })

  it('emits a placeholder comment (never a raw token) when the table is unusable', () => {
    const stub = ucTagsStub({ write_target: { table: '${t}' } })
    expect(stub).toContain('version: 1.0.0')
    expect(stub).toContain("table:  # set to the write target's table name")
    expect(stub).not.toContain('${t}')
  })

  it('never seeds a managed-empty `tags: {}` (that would delete all table tags)', () => {
    const stub = ucTagsStub({ write_target: { table: 'orders' } })
    expect(stub).not.toContain('tags: {}')
  })
})

describe('tags_file field wiring on both write specs', () => {
  it('the streaming_table tags_file placeholder proposes an uc_tags/ path', () => {
    const field = tagsFileField(writeStreamingTableSpec)!
    expect(field.placeholder?.startsWith('uc_tags/')).toBe(true)
    expect(field.fileRef?.suggestPath).toBe(ucTagsSuggestPath)
    expect(field.fileRef?.stub).toBe(ucTagsStub)
  })

  it('the materialized_view tags_file placeholder proposes an uc_tags/ path', () => {
    const field = tagsFileField(writeMaterializedViewSpec)!
    expect(field.placeholder?.startsWith('uc_tags/')).toBe(true)
    expect(field.fileRef?.suggestPath).toBe(ucTagsSuggestPath)
    expect(field.fileRef?.stub).toBe(ucTagsStub)
  })
})
