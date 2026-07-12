import { describe, expect, it } from 'vitest'
import {
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
  setActionField,
} from '@/lib/flowgroup-doc'
import type { ActionSubTypeSpec, FieldSpec } from '../types'
import { readPath } from '../helpers'
import { coerceValue } from '../../formModel'
import { getActionSpec } from '../registry'
import { loadCloudfilesSpec } from '../load-cloudfiles'
import { transformSqlSpec } from '../transform-sql'
import { writeStreamingTableSpec } from '../write-streaming-table'

const LOAD_YAML = `pipeline: perf
flowgroup: fg_load
actions:
  - name: load_dd
    type: load
    source:
      type: cloudfiles
      path: "\${landing_volume}/dd/*.parquet"
      format: parquet
      options:
        cloudFiles.maxFilesPerTrigger: 50
    target: v_dd_raw
`

const TRANSFORM_YAML = `pipeline: perf
flowgroup: fg_t
actions:
  - name: t_orders
    type: transform
    transform_type: sql
    source: v_raw
    sql: "SELECT * FROM v_raw"
    target: v_orders
`

const WRITE_YAML = `pipeline: perf
flowgroup: fg_w
actions:
  - name: w_stock
    type: write
    source: v_stock_bronze
    write_target:
      type: streaming_table
      mode: cdc
      catalog: "\${catalog}"
      schema: silver
      table: stock_prices
      cdc_config:
        keys:
          - ticker
          - trade_date
        sequence_by: _seq
        scd_type: 2
        except_column_list:
          - _seq
`

/** A representative probe value to write for a field of each widget kind. */
function probe(field: FieldSpec): unknown {
  switch (field.widget) {
    case 'number':
      return 42
    case 'bool':
      return true
    case 'enum':
      return coerceValue(field, field.options?.[0] ?? 'x')
    case 'stringList':
      return ['probe_a', 'probe_b']
    case 'keyValue':
      return { probe_key: 'probe_value' }
    case 'objectList': {
      // A row built from the item fields (objectList is now form-editable).
      const row: Record<string, unknown> = {}
      for (const itemField of field.itemFields ?? []) {
        row[String(itemField.path[0])] = probe(itemField)
      }
      return [row]
    }
    default:
      return 'probe_value'
  }
}

/**
 * Every field path must be writable through flowgroup-doc and read back
 * unchanged from a re-parsed, re-serialized file — i.e. the path is a valid
 * surgical address against a realistic fixture.
 */
function assertFieldsRoundTrip(spec: ActionSubTypeSpec, yaml: string, actionName: string): void {
  const fields = spec.groups.flatMap((group) => group.fields)
  for (const field of fields) {
    const value = probe(field)
    if (value === undefined) continue

    // Each field is tested in isolation against the pristine fixture.
    const handle = parseFlowgroupFile(yaml)
    expect(handle.errors).toEqual([])
    const fg = selectFlowgroup(handle, flowgroupOf(yaml))
    expect(fg).toBeDefined()
    setActionField(fg!, actionName, field.path, value)

    const reparsed = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), flowgroupOf(yaml))
    const action = listActions(reparsed!).find((a) => a.name === actionName)
    expect(action, `action ${actionName} after writing ${JSON.stringify(field.path)}`).toBeDefined()
    expect(
      readPath(action!.raw, field.path),
      `round-trip of ${JSON.stringify(field.path)} (${field.widget})`,
    ).toEqual(value)
  }
}

/** Pull the flowgroup name out of a fixture (first `flowgroup:` line). */
function flowgroupOf(yaml: string): string {
  return /flowgroup:\s*(\S+)/.exec(yaml)![1]
}

describe('registry', () => {
  it('resolves the three pilot specs', () => {
    expect(getActionSpec('load', 'cloudfiles')).toBe(loadCloudfilesSpec)
    expect(getActionSpec('transform', 'sql')).toBe(transformSqlSpec)
    expect(getActionSpec('write', 'streaming_table')).toBe(writeStreamingTableSpec)
  })

  it('returns undefined for unknown sub-types and missing discriminators', () => {
    // After batch B every real (kind, subType) resolves, so the negative case
    // uses invalid sub-types (test/row_count + write/materialized_view are now
    // built) plus missing discriminators.
    expect(getActionSpec('test', 'no_such_test')).toBeUndefined()
    expect(getActionSpec('write', 'no_such_target')).toBeUndefined()
    expect(getActionSpec('load', 'no_such_source')).toBeUndefined()
    expect(getActionSpec(undefined, undefined)).toBeUndefined()
    expect(getActionSpec('load', undefined)).toBeUndefined()
  })
})

describe('load/cloudfiles spec', () => {
  it('reads fields present in a realistic fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(LOAD_YAML), 'fg_load')!
    const action = listActions(doc).find((a) => a.name === 'load_dd')!
    expect(readPath(action.raw, ['source', 'format'])).toBe('parquet')
    expect(readPath(action.raw, ['source', 'options', 'cloudFiles.maxFilesPerTrigger'])).toBe(50)
    expect(readPath(action.raw, ['target'])).toBe('v_dd_raw')
  })

  it('every field path round-trips', () => {
    assertFieldsRoundTrip(loadCloudfilesSpec, LOAD_YAML, 'load_dd')
  })
})

describe('transform/sql spec', () => {
  it('reads fields present in a realistic fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(TRANSFORM_YAML), 'fg_t')!
    const action = listActions(doc).find((a) => a.name === 't_orders')!
    expect(readPath(action.raw, ['source'])).toBe('v_raw')
    expect(readPath(action.raw, ['sql'])).toBe('SELECT * FROM v_raw')
    expect(readPath(action.raw, ['target'])).toBe('v_orders')
  })

  it('every field path round-trips', () => {
    assertFieldsRoundTrip(transformSqlSpec, TRANSFORM_YAML, 't_orders')
  })
})

describe('write/streaming_table spec', () => {
  it('reads cdc fields (incl. typed scd_type) present in a realistic fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(WRITE_YAML), 'fg_w')!
    const action = listActions(doc).find((a) => a.name === 'w_stock')!
    expect(readPath(action.raw, ['write_target', 'mode'])).toBe('cdc')
    expect(readPath(action.raw, ['write_target', 'cdc_config', 'scd_type'])).toBe(2)
    expect(readPath(action.raw, ['write_target', 'cdc_config', 'keys'])).toEqual([
      'ticker',
      'trade_date',
    ])
  })

  it('every field path (standard + cdc + snapshot) round-trips', () => {
    assertFieldsRoundTrip(writeStreamingTableSpec, WRITE_YAML, 'w_stock')
  })

  it('writing scd_type as an integer keeps it unquoted', () => {
    const handle = parseFlowgroupFile(WRITE_YAML)
    const fg = selectFlowgroup(handle, 'fg_w')!
    setActionField(fg, 'w_stock', ['write_target', 'cdc_config', 'scd_type'], 1)
    expect(serializeFlowgroupFile(handle)).toContain('scd_type: 1')
  })
})
