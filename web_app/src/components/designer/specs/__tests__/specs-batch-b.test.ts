// Batch B specs (write mv/sink + test ×9): registry resolution, per-spec
// set+delete round-trip on realistic fixtures, cross-field rule firing, PLUS
// the objectList engine tests (add/edit/delete row + comment preservation).
// Self-contained (own helper copies), mirroring specs-batch-a.test.ts.

import { describe, expect, it } from 'vitest'
import {
  deleteActionField,
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
  setActionField,
} from '@/lib/flowgroup-doc'
import type { FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import type { ActionKind, ActionSubTypeSpec, FieldSpec, YamlPath } from '../types'
import { readPath } from '../helpers'
import { coerceValue, computeIssues, objectListRows, pathKey } from '../../formModel'
import { getActionSpec } from '../registry'
import { writeMaterializedViewSpec } from '../write-materialized-view'
import { writeSinkSpec } from '../write-sink'
import { testRowCountSpec } from '../test-row-count'
import { testUniquenessSpec } from '../test-uniqueness'
import { testReferentialIntegritySpec } from '../test-referential-integrity'
import { testCompletenessSpec } from '../test-completeness'
import { testRangeSpec } from '../test-range'
import { testSchemaMatchSpec } from '../test-schema-match'
import { testAllLookupsFoundSpec } from '../test-all-lookups-found'
import { testCustomSqlSpec } from '../test-custom-sql'
import { testCustomExpectationsSpec } from '../test-custom-expectations'

// ── realistic fixtures (grounded in tests/e2e/fixtures + perf pipelines) ──

const MV_YAML = `pipeline: perf
flowgroup: fg_mv
actions:
  - name: w_customer_summary
    type: write
    source: v_customer_orders
    write_target:
      type: materialized_view
      catalog: "\${catalog}"
      schema: "\${gold_schema}"
      table: customer_summary
`

const SINK_YAML = `pipeline: perf
flowgroup: fg_sink
actions:
  - name: w_delta_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_type: delta
      sink_name: analytics_delta_export
      comment: "Stream metrics to analytics"
      options:
        tableName: "\${catalog}.edw_analytics.daily_order_metrics"
        checkpointLocation: "/Volumes/\${catalog}/_meta/chk"
`

const ROW_COUNT_YAML = `pipeline: perf
flowgroup: fg_rc
actions:
  - name: ta_row_count
    type: test
    test_type: row_count
    source:
      - "\${catalog}.\${raw_schema}.customers"
      - "\${catalog}.\${bronze_schema}.customers"
    tolerance: 0
    on_violation: warn
`

const UNIQUENESS_YAML = `pipeline: perf
flowgroup: fg_uniq
actions:
  - name: ta_uniqueness
    type: test
    test_type: uniqueness
    source: "\${catalog}.\${silver_schema}.fact_orders"
    columns: [order_id]
    on_violation: warn
`

const REF_INT_YAML = `pipeline: perf
flowgroup: fg_ri
actions:
  - name: ta_orders_customer_fk
    type: test
    test_type: referential_integrity
    source: "\${catalog}.\${silver_schema}.fact_orders"
    reference: "\${catalog}.\${silver_schema}.dim_customer"
    source_columns: [customer_id]
    reference_columns: [customer_id]
    on_violation: warn
`

const COMPLETENESS_YAML = `pipeline: perf
flowgroup: fg_comp
actions:
  - name: ta_completeness
    type: test
    test_type: completeness
    source: "\${catalog}.\${silver_schema}.fact_orders"
    required_columns: [customer_id, order_date]
    on_violation: warn
`

const RANGE_YAML = `pipeline: perf
flowgroup: fg_range
actions:
  - name: ta_range
    type: test
    test_type: range
    source: "\${catalog}.\${silver_schema}.fact_orders"
    column: total_price
    min_value: 0
    max_value: 1000000
    on_violation: warn
`

const SCHEMA_MATCH_YAML = `pipeline: perf
flowgroup: fg_sm
actions:
  - name: ta_schema_match
    type: test
    test_type: schema_match
    source: "\${catalog}.\${silver_schema}.fact_orders"
    reference: "\${catalog}.\${gold_schema}.fact_orders_expected"
    on_violation: warn
`

const ALL_LOOKUPS_YAML = `pipeline: perf
flowgroup: fg_alf
actions:
  - name: ta_all_lookups
    type: test
    test_type: all_lookups_found
    source: "\${catalog}.\${silver_schema}.fact_orders"
    lookup_table: "\${catalog}.\${gold_schema}.dim_date"
    lookup_columns: [order_date]
    lookup_result_columns: [date_key]
    on_violation: warn
`

const CUSTOM_SQL_YAML = `pipeline: perf
flowgroup: fg_csql
actions:
  - name: ta_custom_sql
    type: test
    test_type: custom_sql
    source: "\${catalog}.\${gold_schema}.monthly_revenue"
    sql: |
      SELECT month, pct_difference FROM \${catalog}.\${gold_schema}.revenue_comparison
    expectations:
      - name: revenue_matches
        expression: "pct_difference < 0.5"
        on_violation: warn
`

const CUSTOM_EXP_YAML = `pipeline: perf
flowgroup: fg_ce
actions:
  - name: ta_business_rules
    type: test
    test_type: custom_expectations
    source: "\${catalog}.\${silver_schema}.fact_orders"
    expectations:
      # first business rule
      - name: positive_amount
        expression: "total_price > 0"
        on_violation: warn
      - name: reasonable_discount
        expression: "discount_percent <= 50"
        on_violation: warn
`

interface Case {
  spec: ActionSubTypeSpec
  kind: ActionKind
  subType: string
  yaml: string
  action: string
  flowgroup: string
}

const CASES: Case[] = [
  { spec: writeMaterializedViewSpec, kind: 'write', subType: 'materialized_view', yaml: MV_YAML, action: 'w_customer_summary', flowgroup: 'fg_mv' },
  { spec: writeSinkSpec, kind: 'write', subType: 'sink', yaml: SINK_YAML, action: 'w_delta_sink', flowgroup: 'fg_sink' },
  { spec: testRowCountSpec, kind: 'test', subType: 'row_count', yaml: ROW_COUNT_YAML, action: 'ta_row_count', flowgroup: 'fg_rc' },
  { spec: testUniquenessSpec, kind: 'test', subType: 'uniqueness', yaml: UNIQUENESS_YAML, action: 'ta_uniqueness', flowgroup: 'fg_uniq' },
  { spec: testReferentialIntegritySpec, kind: 'test', subType: 'referential_integrity', yaml: REF_INT_YAML, action: 'ta_orders_customer_fk', flowgroup: 'fg_ri' },
  { spec: testCompletenessSpec, kind: 'test', subType: 'completeness', yaml: COMPLETENESS_YAML, action: 'ta_completeness', flowgroup: 'fg_comp' },
  { spec: testRangeSpec, kind: 'test', subType: 'range', yaml: RANGE_YAML, action: 'ta_range', flowgroup: 'fg_range' },
  { spec: testSchemaMatchSpec, kind: 'test', subType: 'schema_match', yaml: SCHEMA_MATCH_YAML, action: 'ta_schema_match', flowgroup: 'fg_sm' },
  { spec: testAllLookupsFoundSpec, kind: 'test', subType: 'all_lookups_found', yaml: ALL_LOOKUPS_YAML, action: 'ta_all_lookups', flowgroup: 'fg_alf' },
  { spec: testCustomSqlSpec, kind: 'test', subType: 'custom_sql', yaml: CUSTOM_SQL_YAML, action: 'ta_custom_sql', flowgroup: 'fg_csql' },
  { spec: testCustomExpectationsSpec, kind: 'test', subType: 'custom_expectations', yaml: CUSTOM_EXP_YAML, action: 'ta_business_rules', flowgroup: 'fg_ce' },
]

/** A representative probe value for a field of each widget kind. */
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

/** Every declared field path must set + read back + delete against a fixture. */
function assertFieldsRoundTrip(spec: ActionSubTypeSpec, yaml: string, actionName: string, flowgroup: string): void {
  const fields = spec.groups.flatMap((group) => group.fields)
  for (const field of fields) {
    const value = probe(field)

    // set → read back
    const handle = parseFlowgroupFile(yaml)
    expect(handle.errors).toEqual([])
    const fg = selectFlowgroup(handle, flowgroup)
    expect(fg).toBeDefined()
    setActionField(fg!, actionName, field.path, value)

    const afterSet = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), flowgroup)
    const setAction = listActions(afterSet!).find((a) => a.name === actionName)
    expect(setAction, `action ${actionName} after writing ${JSON.stringify(field.path)}`).toBeDefined()
    expect(
      readPath(setAction!.raw, field.path),
      `round-trip of ${JSON.stringify(field.path)} (${field.widget})`,
    ).toEqual(value)

    // delete → absent
    deleteActionField(fg!, actionName, field.path)
    const afterDelete = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), flowgroup)
    const delAction = listActions(afterDelete!).find((a) => a.name === actionName)
    expect(
      readPath(delAction!.raw, field.path),
      `delete of ${JSON.stringify(field.path)} (${field.widget})`,
    ).toBeUndefined()
  }
}

describe('registry — batch B specs resolve by (kind, subType)', () => {
  for (const c of CASES) {
    it(`resolves ${c.kind}/${c.subType}`, () => {
      expect(getActionSpec(c.kind, c.subType)).toBe(c.spec)
    })
  }

  it('all 24 action variants now resolve (fallback unreachable for real actions)', () => {
    const variants: [ActionKind, string][] = [
      ['load', 'cloudfiles'], ['load', 'delta'], ['load', 'sql'], ['load', 'python'],
      ['load', 'jdbc'], ['load', 'custom_datasource'], ['load', 'kafka'],
      ['transform', 'sql'], ['transform', 'python'], ['transform', 'data_quality'],
      ['transform', 'temp_table'], ['transform', 'schema'],
      ['write', 'streaming_table'], ['write', 'materialized_view'], ['write', 'sink'],
      ['test', 'row_count'], ['test', 'uniqueness'], ['test', 'referential_integrity'],
      ['test', 'completeness'], ['test', 'range'], ['test', 'schema_match'],
      ['test', 'all_lookups_found'], ['test', 'custom_sql'], ['test', 'custom_expectations'],
    ]
    expect(variants).toHaveLength(24)
    for (const [kind, subType] of variants) {
      expect(getActionSpec(kind, subType), `${kind}/${subType}`).toBeDefined()
    }
  })
})

describe('batch B specs — every field path set+deletes on a realistic fixture', () => {
  for (const c of CASES) {
    it(`${c.kind}/${c.subType}`, () => {
      const handle = parseFlowgroupFile(c.yaml)
      expect(handle.errors).toEqual([])
      assertFieldsRoundTrip(c.spec, c.yaml, c.action, c.flowgroup)
    })
  }
})

describe('write/materialized_view spec — reads present fields', () => {
  it('reads catalog/schema/table/source from a realistic fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(MV_YAML), 'fg_mv')!
    const action = listActions(doc).find((a) => a.name === 'w_customer_summary')!
    expect(readPath(action.raw, ['write_target', 'catalog'])).toBe('${catalog}')
    expect(readPath(action.raw, ['write_target', 'table'])).toBe('customer_summary')
    expect(readPath(action.raw, ['source'])).toBe('v_customer_orders')
  })
})

describe('write/sink spec — reads present fields', () => {
  it('reads sink_type/sink_name/options from a realistic delta-sink fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(SINK_YAML), 'fg_sink')!
    const action = listActions(doc).find((a) => a.name === 'w_delta_sink')!
    expect(readPath(action.raw, ['write_target', 'sink_type'])).toBe('delta')
    expect(readPath(action.raw, ['write_target', 'sink_name'])).toBe('analytics_delta_export')
    expect(readPath(action.raw, ['write_target', 'options', 'tableName'])).toBe(
      '${catalog}.edw_analytics.daily_order_metrics',
    )
  })
})

describe('test specs — read present fields', () => {
  it('row_count: source is a two-element list + tolerance', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(ROW_COUNT_YAML), 'fg_rc')!
    const action = listActions(doc).find((a) => a.name === 'ta_row_count')!
    expect(readPath(action.raw, ['source'])).toHaveLength(2)
    expect(readPath(action.raw, ['tolerance'])).toBe(0)
  })

  it('custom_expectations: expectations is a list of {name, expression, on_violation}', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(CUSTOM_EXP_YAML), 'fg_ce')!
    const action = listActions(doc).find((a) => a.name === 'ta_business_rules')!
    expect(readPath(action.raw, ['expectations', 0, 'name'])).toBe('positive_amount')
    expect(readPath(action.raw, ['expectations', 1, 'expression'])).toBe('discount_percent <= 50')
  })
})

describe('batch B specs — cross-field rules', () => {
  it('mv: requiredOneOf(source, sql, sql_path) + mutuallyExclusive when 2+', () => {
    // Query source is now a oneOfToggle owning source/sql/sql_path, so the
    // requiredOneOf + mutuallyExclusive hints re-surface on the toggle's
    // synthetic path (computeIssues toggle-ownership, Task 4.2b).
    const QS = ['__query_source']
    const none = computeIssues(writeMaterializedViewSpec, {
      write_target: { catalog: 'c', schema: 's', table: 't' },
    })
    expect(none.get(pathKey(QS))).toMatch(/source view.*inline SQL.*SQL file/i)

    const one = computeIssues(writeMaterializedViewSpec, {
      source: 'v',
      write_target: { catalog: 'c', schema: 's', table: 't' },
    })
    expect(one.has(pathKey(QS))).toBe(false)

    const two = computeIssues(writeMaterializedViewSpec, {
      source: 'v',
      write_target: { catalog: 'c', schema: 's', table: 't', sql: 'SELECT 1' },
    })
    expect(two.get(pathKey(QS))).toMatch(/only one query source/i)
  })

  it('mv: cluster_columns XOR cluster_by_auto', () => {
    const both = computeIssues(writeMaterializedViewSpec, {
      source: 'v',
      write_target: { catalog: 'c', schema: 's', table: 't', cluster_columns: ['a'], cluster_by_auto: true },
    })
    expect(both.get(pathKey(['write_target', 'cluster_columns']))).toMatch(/only one/i)
  })

  it('sink/delta: options must set exactly one of tableName / path', () => {
    const neither = computeIssues(writeSinkSpec, {
      source: 'v',
      write_target: { sink_type: 'delta', sink_name: 's', options: { mergeSchema: 'true' } },
    })
    expect(neither.get(pathKey(['write_target', 'options']))).toMatch(/tableName or path/i)

    const both = computeIssues(writeSinkSpec, {
      source: 'v',
      write_target: { sink_type: 'delta', sink_name: 's', options: { tableName: 'c.s.t', path: '/p' } },
    })
    expect(both.get(pathKey(['write_target', 'options']))).toMatch(/cannot set both/i)

    const one = computeIssues(writeSinkSpec, {
      source: 'v',
      write_target: { sink_type: 'delta', sink_name: 's', options: { tableName: 'c.s.t' } },
    })
    expect(one.has(pathKey(['write_target', 'options']))).toBe(false)
  })

  it('sink/foreachbatch: module_path XOR batch_handler + single-string source', () => {
    // foreachbatch module_path ⊕ batch_handler is now a oneOfToggle, so the XOR
    // hint re-surfaces on the toggle's synthetic path (Task 4.2b).
    const both = computeIssues(writeSinkSpec, {
      source: 'v',
      write_target: { sink_type: 'foreachbatch', sink_name: 's', module_path: 'm.py', batch_handler: 'df' },
    })
    expect(both.get(pathKey(['write_target', '__handler']))).toMatch(/exactly one/i)

    const listSource = computeIssues(writeSinkSpec, {
      source: ['a', 'b'],
      write_target: { sink_type: 'foreachbatch', sink_name: 's', batch_handler: 'df' },
    })
    expect(listSource.get(pathKey(['source']))).toMatch(/single source/i)

    // Delta sink with a list source does NOT trip the foreachbatch rule.
    const deltaList = computeIssues(writeSinkSpec, {
      source: ['a', 'b'],
      write_target: { sink_type: 'delta', sink_name: 's', options: { tableName: 'c.s.t' } },
    })
    expect(deltaList.has(pathKey(['source']))).toBe(false)
  })

  it('referential_integrity: source_columns / reference_columns equal length', () => {
    const unequal = computeIssues(testReferentialIntegritySpec, {
      source: 's',
      reference: 'c.s.t',
      source_columns: ['a', 'b'],
      reference_columns: ['a'],
    })
    expect(unequal.get(pathKey(['source_columns']))).toMatch(/same length/i)

    const equal = computeIssues(testReferentialIntegritySpec, {
      source: 's',
      reference: 'c.s.t',
      source_columns: ['a', 'b'],
      reference_columns: ['a', 'b'],
    })
    expect(equal.has(pathKey(['source_columns']))).toBe(false)
  })

  it('all_lookups_found: lookup_columns / lookup_result_columns equal length', () => {
    const unequal = computeIssues(testAllLookupsFoundSpec, {
      source: 's',
      lookup_table: 'c.s.t',
      lookup_columns: ['a'],
      lookup_result_columns: ['a', 'b'],
    })
    expect(unequal.get(pathKey(['lookup_columns']))).toMatch(/same length/i)
  })

  it('range: requiredOneOf(min_value, max_value)', () => {
    const none = computeIssues(testRangeSpec, { source: 's', column: 'x' })
    expect(none.get(pathKey(['min_value']))).toMatch(/at least one/i)

    const one = computeIssues(testRangeSpec, { source: 's', column: 'x', min_value: 0 })
    expect(one.has(pathKey(['min_value']))).toBe(false)
  })

  it('row_count: source must be exactly two entries', () => {
    const three = computeIssues(testRowCountSpec, { source: ['a', 'b', 'c'] })
    expect(three.get(pathKey(['source']))).toMatch(/exactly two/i)

    const two = computeIssues(testRowCountSpec, { source: ['a', 'b'] })
    expect(two.has(pathKey(['source']))).toBe(false)
  })
})

// ── objectList engine (Part 1) — these mirror ActionForm.renderObjectList's
// add/remove callbacks (append at [...path, len] / create [{}]; delete last
// row deletes the key) so the exact YAML mutations are asserted here without a
// React render. See ActionForm.objectList.test.tsx for the rendered PUT path.

const EXPECTATIONS: YamlPath = ['expectations']

/** Mirror of renderObjectList's add-row callback. */
function addRow(doc: FlowgroupDocHandle, action: string, path: YamlPath, rowCount: number): void {
  if (rowCount > 0) setActionField(doc, action, [...path, rowCount], {})
  else setActionField(doc, action, path, [{}])
}

/** Mirror of renderObjectList's remove-row callback. */
function removeRow(doc: FlowgroupDocHandle, action: string, path: YamlPath, index: number, rowCount: number): void {
  if (rowCount <= 1) deleteActionField(doc, action, path)
  else deleteActionField(doc, action, [...path, index])
}

describe('objectList — objectListRows helper', () => {
  it('reads the row objects from a fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(CUSTOM_EXP_YAML), 'fg_ce')!
    const action = listActions(doc).find((a) => a.name === 'ta_business_rules')!
    const rows = objectListRows(action.raw, EXPECTATIONS)
    expect(rows).toHaveLength(2)
    expect(rows[0].name).toBe('positive_amount')
  })

  it('normalizes absent / non-array / non-object to []/{} ', () => {
    expect(objectListRows({}, EXPECTATIONS)).toEqual([])
    expect(objectListRows({ expectations: 'nope' }, EXPECTATIONS)).toEqual([])
    expect(objectListRows({ expectations: ['x', { name: 'y' }] }, EXPECTATIONS)).toEqual([{}, { name: 'y' }])
  })
})

describe('objectList — add / edit / delete row mutations', () => {
  const CE_ACTION = 'ta_business_rules'

  it('adds a first row into an action that has no expectations key', () => {
    const empty = `pipeline: perf
flowgroup: fg_ce
actions:
  - name: ta_business_rules
    type: test
    test_type: custom_expectations
    source: v_orders
`
    const handle = parseFlowgroupFile(empty)
    const fg = selectFlowgroup(handle, 'fg_ce')!
    addRow(fg, CE_ACTION, EXPECTATIONS, 0)
    setActionField(fg, CE_ACTION, [...EXPECTATIONS, 0, 'name'], 'positive_amount')
    setActionField(fg, CE_ACTION, [...EXPECTATIONS, 0, 'expression'], 'total_price > 0')

    const reparsed = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), 'fg_ce')!
    const action = listActions(reparsed).find((a) => a.name === CE_ACTION)!
    expect(objectListRows(action.raw, EXPECTATIONS)).toEqual([
      { name: 'positive_amount', expression: 'total_price > 0' },
    ])
  })

  it('appends a row to an existing list', () => {
    const handle = parseFlowgroupFile(CUSTOM_EXP_YAML)
    const fg = selectFlowgroup(handle, 'fg_ce')!
    addRow(fg, CE_ACTION, EXPECTATIONS, 2)
    setActionField(fg, CE_ACTION, [...EXPECTATIONS, 2, 'name'], 'third_rule')

    const reparsed = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), 'fg_ce')!
    const action = listActions(reparsed).find((a) => a.name === CE_ACTION)!
    expect(objectListRows(action.raw, EXPECTATIONS)).toHaveLength(3)
    expect(readPath(action.raw, [...EXPECTATIONS, 2, 'name'])).toBe('third_rule')
  })

  it('edits one item field in place and preserves sibling comments', () => {
    const handle = parseFlowgroupFile(CUSTOM_EXP_YAML)
    const fg = selectFlowgroup(handle, 'fg_ce')!
    setActionField(fg, CE_ACTION, [...EXPECTATIONS, 0, 'expression'], 'total_price >= 1')

    const text = serializeFlowgroupFile(handle)
    expect(text).toContain('total_price >= 1')
    // The row comment survives a surgical scalar patch.
    expect(text).toContain('# first business rule')
    // The untouched second row is unchanged.
    const reparsed = selectFlowgroup(parseFlowgroupFile(text), 'fg_ce')!
    const action = listActions(reparsed).find((a) => a.name === CE_ACTION)!
    expect(readPath(action.raw, [...EXPECTATIONS, 1, 'name'])).toBe('reasonable_discount')
  })

  it('deletes a middle row (keeps the rest)', () => {
    const handle = parseFlowgroupFile(CUSTOM_EXP_YAML)
    const fg = selectFlowgroup(handle, 'fg_ce')!
    removeRow(fg, CE_ACTION, EXPECTATIONS, 0, 2)

    const reparsed = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), 'fg_ce')!
    const action = listActions(reparsed).find((a) => a.name === CE_ACTION)!
    const rows = objectListRows(action.raw, EXPECTATIONS)
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('reasonable_discount')
  })

  it('deleting the last row removes the whole expectations key (delete-on-clear)', () => {
    const single = `pipeline: perf
flowgroup: fg_ce
actions:
  - name: ta_business_rules
    type: test
    test_type: custom_expectations
    source: v_orders
    expectations:
      - name: only_rule
        expression: "x > 0"
`
    const handle = parseFlowgroupFile(single)
    const fg = selectFlowgroup(handle, 'fg_ce')!
    removeRow(fg, CE_ACTION, EXPECTATIONS, 0, 1)

    const reparsed = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(handle)), 'fg_ce')!
    const action = listActions(reparsed).find((a) => a.name === CE_ACTION)!
    expect(readPath(action.raw, EXPECTATIONS)).toBeUndefined()
  })
})
