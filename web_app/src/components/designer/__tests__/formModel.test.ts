import { describe, expect, it } from 'vitest'
import { coerceValue, computeIssues, enumOptions, pathKey, visibleGroups } from '../formModel'
import type { FieldSpec } from '../specs/types'
import { loadCloudfilesSpec } from '../specs/load-cloudfiles'
import { transformSqlSpec } from '../specs/transform-sql'
import { writeStreamingTableSpec } from '../specs/write-streaming-table'

const titles = (raw: Record<string, unknown>): (string | undefined)[] =>
  visibleGroups(writeStreamingTableSpec, raw).map((group) => group.title)

describe('formModel — visibility (mode discriminator)', () => {
  it('standard mode: Source shown, CDC + Snapshot hidden', () => {
    const shown = titles({ write_target: {} })
    expect(shown).toContain('Source')
    expect(shown).not.toContain('CDC configuration')
    expect(shown).not.toContain('Snapshot CDC configuration')
  })

  it('cdc mode: CDC configuration shown', () => {
    const shown = titles({ write_target: { mode: 'cdc' } })
    expect(shown).toContain('CDC configuration')
    expect(shown).toContain('Source')
    expect(shown).not.toContain('Snapshot CDC configuration')
  })

  it('snapshot_cdc mode: Snapshot shown, Source hidden', () => {
    const shown = titles({ write_target: { mode: 'snapshot_cdc' } })
    expect(shown).toContain('Snapshot CDC configuration')
    expect(shown).not.toContain('Source')
    expect(shown).not.toContain('CDC configuration')
  })
})

describe('formModel — soft validation hints', () => {
  it('sql XOR sql_path: both set and neither set both hint (on the toggle); exactly one is clean', () => {
    // sql ⊕ sql_path now render as one oneOfToggle, so the xor hint surfaces at
    // the toggle's synthetic visible path (both branch keys re-key there).
    const both = computeIssues(transformSqlSpec, {
      sql: 'x',
      sql_path: 'y',
      source: 'v',
      target: 't',
    })
    expect(both.get(pathKey(['__sql_source']))).toMatch(/exactly one/i)

    const neither = computeIssues(transformSqlSpec, { source: 'v', target: 't' })
    expect(neither.get(pathKey(['__sql_source']))).toMatch(/exactly one/i)

    const one = computeIssues(transformSqlSpec, { sql: 'x', source: 'v', target: 't' })
    expect(one.has(pathKey(['__sql_source']))).toBe(false)
  })

  it('required-missing hints on absent required fields', () => {
    const issues = computeIssues(transformSqlSpec, { sql: 'x' })
    expect(issues.get(pathKey(['source']))).toMatch(/required/i)
    expect(issues.get(pathKey(['target']))).toMatch(/required/i)
  })

  it('mutuallyExclusive: cluster_columns + cluster_by_auto hints', () => {
    const issues = computeIssues(writeStreamingTableSpec, {
      source: 'v',
      write_target: {
        catalog: 'c',
        schema: 's',
        table: 't',
        cluster_columns: ['a'],
        cluster_by_auto: true,
      },
    })
    expect(issues.get(pathKey(['write_target', 'cluster_columns']))).toMatch(/only one/i)
  })

  it('cluster_by_auto: false does not trip the exclusion', () => {
    const issues = computeIssues(writeStreamingTableSpec, {
      source: 'v',
      write_target: {
        catalog: 'c',
        schema: 's',
        table: 't',
        cluster_columns: ['a'],
        cluster_by_auto: false,
      },
    })
    expect(issues.has(pathKey(['write_target', 'cluster_columns']))).toBe(false)
  })

  it('cdc rules hint only when the cdc group is visible', () => {
    const hidden = computeIssues(writeStreamingTableSpec, {
      write_target: { cdc_config: { column_list: ['a'], except_column_list: ['b'] } },
    })
    expect(hidden.has(pathKey(['write_target', 'cdc_config', 'column_list']))).toBe(false)

    const shown = computeIssues(writeStreamingTableSpec, {
      write_target: {
        mode: 'cdc',
        cdc_config: { column_list: ['a'], except_column_list: ['b'] },
      },
    })
    expect(shown.get(pathKey(['write_target', 'cdc_config', 'column_list']))).toMatch(/only one/i)
  })

  it('custom rule: apply_as_truncates with SCD Type 2', () => {
    const issues = computeIssues(writeStreamingTableSpec, {
      write_target: { mode: 'cdc', cdc_config: { apply_as_truncates: 'x', scd_type: 2 } },
    })
    expect(issues.get(pathKey(['write_target', 'cdc_config', 'apply_as_truncates']))).toMatch(
      /scd type 2/i,
    )
  })

  it('custom rule: cloudfiles readMode must be stream', () => {
    const issues = computeIssues(loadCloudfilesSpec, {
      source: { path: 'p', format: 'json', readMode: 'batch' },
      target: 'v',
    })
    expect(issues.get(pathKey(['source', 'readMode']))).toMatch(/stream/i)
  })

  it('snapshot source XOR source_function', () => {
    // source ⊕ source_function now render as one oneOfToggle (Task 4.3a), so the
    // xor hint surfaces at the toggle's synthetic visible path (both branch keys
    // re-key there via computeIssues toggle-ownership) — mirroring sql/sql_path.
    const both = computeIssues(writeStreamingTableSpec, {
      write_target: {
        mode: 'snapshot_cdc',
        catalog: 'c',
        schema: 's',
        table: 't',
        snapshot_cdc_config: { source: 't', source_function: { file: 'f', function: 'g' } },
      },
    })
    expect(both.get(pathKey(['write_target', 'snapshot_cdc_config', '__source']))).toMatch(
      /exactly one/i,
    )
  })
})

describe('formModel — token tolerance + coercion', () => {
  const enumField: FieldSpec = { path: ['x'], label: 'X', widget: 'enum', options: ['1', '2'] }

  it('enumOptions appends a token value so it renders as a custom entry', () => {
    expect(enumOptions(enumField, '${scd}')).toEqual(['1', '2', '${scd}'])
    expect(enumOptions(enumField, '2')).toEqual(['1', '2'])
    expect(enumOptions(enumField, undefined)).toEqual(['1', '2'])
  })

  it('coerceValue: integer enum coerces to number but never coerces a token', () => {
    const intField: FieldSpec = { ...enumField, valueType: 'integer' }
    expect(coerceValue(intField, '2')).toBe(2)
    expect(coerceValue(intField, '${scd}')).toBe('${scd}')
    expect(coerceValue(enumField, '2')).toBe('2')
  })
})
