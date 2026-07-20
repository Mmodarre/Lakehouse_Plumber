import { describe, expect, it } from 'vitest'
import type { ActionSubTypeSpec, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { schemaFileTableName, schemaStub, schemaSuggestPath } from '../schemaFile'
import { writeStreamingTableSpec } from '../write-streaming-table'
import { writeMaterializedViewSpec } from '../write-materialized-view'

const key = (path: YamlPath) => JSON.stringify(path)
function tagsFileField(s: ActionSubTypeSpec): FieldSpec | undefined {
  return s.groups
    .flatMap((g) => g.fields)
    .find((f) => key(f.path) === key(['write_target', 'tags_file']))
}

describe('schemaFileTableName', () => {
  it('returns a clean, trimmed table name', () => {
    expect(schemaFileTableName({ write_target: { table: 'orders' } })).toBe('orders')
    expect(schemaFileTableName({ write_target: { table: '  orders  ' } })).toBe('orders')
  })

  it('returns null for an absent, empty, or non-string table', () => {
    expect(schemaFileTableName({})).toBeNull()
    expect(schemaFileTableName({ write_target: {} })).toBeNull()
    expect(schemaFileTableName({ write_target: { table: '   ' } })).toBeNull()
    expect(schemaFileTableName({ write_target: { table: 42 } })).toBeNull()
  })

  it('returns null for a substitution / template token', () => {
    expect(schemaFileTableName({ write_target: { table: '${bronze_table}' } })).toBeNull()
    expect(schemaFileTableName({ write_target: { table: '{{ table_name }}' } })).toBeNull()
    expect(schemaFileTableName({ write_target: { table: '%{table}' } })).toBeNull()
  })
})

describe('schemaSuggestPath', () => {
  it('proposes schemas/<table>.yaml for a clean table', () => {
    expect(schemaSuggestPath({ write_target: { table: 'orders' } })).toBe('schemas/orders.yaml')
  })

  it('returns null when there is no clean table', () => {
    expect(schemaSuggestPath({})).toBeNull()
    expect(schemaSuggestPath({ write_target: { table: '${t}' } })).toBeNull()
  })
})

describe('schemaStub', () => {
  it('prefills the table line and a unified columns skeleton for a clean table', () => {
    const stub = schemaStub({ write_target: { table: 'orders' } })
    expect(stub).not.toContain('version')
    expect(stub).toContain('table: orders')
    expect(stub).toContain('columns:')
    expect(stub).toContain('- name: example_column')
    expect(stub).toContain('type: STRING')
    // the per-column tags example stays commented (opting one in is explicit)
    expect(stub).toContain('# tags:')
  })

  it('emits a placeholder comment (never a raw token) when the table is unusable', () => {
    const stub = schemaStub({ write_target: { table: '${t}' } })
    expect(stub).not.toContain('version')
    expect(stub).toContain("table:  # set to the write target's table name")
    expect(stub).not.toContain('${t}')
  })

  it('never seeds a managed-empty `tags: {}` (that would delete all table tags)', () => {
    const stub = schemaStub({ write_target: { table: 'orders' } })
    expect(stub).not.toContain('tags: {}')
  })
})

describe('tags_file field wiring on both write specs', () => {
  it('the streaming_table tags_file placeholder proposes a schemas/ path', () => {
    const field = tagsFileField(writeStreamingTableSpec)!
    expect(field.placeholder?.startsWith('schemas/')).toBe(true)
    expect(field.fileRef?.suggestPath).toBe(schemaSuggestPath)
    expect(field.fileRef?.stub).toBe(schemaStub)
  })

  it('the materialized_view tags_file placeholder proposes a schemas/ path', () => {
    const field = tagsFileField(writeMaterializedViewSpec)!
    expect(field.placeholder?.startsWith('schemas/')).toBe(true)
    expect(field.fileRef?.suggestPath).toBe(schemaSuggestPath)
    expect(field.fileRef?.stub).toBe(schemaStub)
  })
})

describe('table_schema file branch wiring (streaming_table)', () => {
  it('the table_schema oneOfToggle "file" branch offers the unified schemas/ stub', () => {
    const toggle = writeStreamingTableSpec.groups
      .flatMap((g) => g.fields)
      .find((f) => key(f.path) === key(['write_target', '__table_schema']))!
    const fileOption = toggle.oneOf!.options.find((o) => o.value === 'file')!
    expect(fileOption.backing).toBe('file')
    expect(fileOption.placeholder?.startsWith('schemas/')).toBe(true)
    expect(fileOption.stub).toBe(schemaStub)
    expect(fileOption.suggestPath).toBe(schemaSuggestPath)
  })
})
