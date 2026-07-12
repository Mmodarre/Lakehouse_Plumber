/**
 * Test suite for the flowgroup document model (Designer, plan item F-2).
 *
 * Load-bearing guarantees under test:
 *   1. All three file forms (single / multi-doc / flowgroups-array) parse,
 *      enumerate, and round-trip byte-identically when unmutated —
 *      including real flowgroup files from Example_Projects.
 *   2. Sub-type discriminator defaults match the CLI dispatch
 *      (action_dispatch.py:364-379).
 *   3. deriveGraph implements the audited wiring semantics: view-name
 *      matching, fan-in/fan-out, terminal writes, MV-sql / snapshot_cdc
 *      no-inbound, externals for unmatched refs, depends_on edges.
 *   4. Every mutator is comment-preserving: byte-exact outside the mutated
 *      span for surgical edits, comment survival for structural rewrites,
 *      and sibling documents / array entries untouched.
 */
/// <reference types="node" />
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

import {
  addAction,
  deleteAction,
  deleteActionField,
  deleteFlowgroupField,
  deriveGraph,
  duplicateAction,
  listActions,
  listFlowgroups,
  parseFlowgroupFile,
  readFlowgroupMeta,
  readFlowgroupValue,
  renameAction,
  selectFlowgroup,
  selectFlowgroupAt,
  serializeFlowgroupFile,
  setActionField,
  setFlowgroupField,
} from '../flowgroup-doc'
import type { FlowgroupDocHandle, FlowgroupFileHandle } from '../flowgroup-doc'

const REPO_ROOT = resolve(import.meta.dirname, '../../../..')
const EXAMPLE_PIPELINES = resolve(
  REPO_ROOT,
  'Example_Projects/performance_testing/pipelines',
)

function readExample(relPath: string): string {
  return readFileSync(resolve(EXAMPLE_PIPELINES, relPath), 'utf8')
}

function mustSelect(
  source: string,
  name: string,
): { handle: FlowgroupFileHandle; doc: FlowgroupDocHandle } {
  const handle = parseFlowgroupFile(source)
  const doc = selectFlowgroup(handle, name)
  if (doc === undefined) throw new Error(`flowgroup '${name}' not found in fixture`)
  return { handle, doc }
}

/** Assert `after` differs from `before` in exactly one line; return its index. */
function expectSingleLineDiff(before: string, after: string): number {
  const a = before.split('\n')
  const b = after.split('\n')
  expect(b.length).toBe(a.length)
  const changed: number[] = []
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) changed.push(i)
  expect(changed).toHaveLength(1)
  return changed[0]
}

/**
 * Pin of the yaml-doc rewrite caveat for the SINGLE fixture: structural
 * edits (deletes, sequence inserts) re-emit the containing document via
 * `Document.toString()`, which pads flow-sequence brackets ('[x]' ->
 * '[ x ]') in that one document — same caveat class as the comment
 * whitespace normalization pinned in yaml-doc.test.ts. Surgical edits
 * (scalar patch / new-key splice) never do this.
 */
function flowNormalized(source: string): string {
  return source
    .replace('depends_on: [main.ops.calendar]', 'depends_on: [ main.ops.calendar ]')
    .replace('columns: [payroll_key]', 'columns: [ payroll_key ]')
}

/**
 * Assert `after` is `before` with lines purely INSERTED (every original
 * line byte-exact and in order); return the inserted lines.
 */
function expectPureInsertion(before: string, after: string): string[] {
  const a = before.split('\n')
  const b = after.split('\n')
  expect(b.length).toBeGreaterThan(a.length)
  let prefix = 0
  while (prefix < a.length && a[prefix] === b[prefix]) prefix++
  let suffix = 0
  while (suffix < a.length - prefix && a[a.length - 1 - suffix] === b[b.length - 1 - suffix]) {
    suffix++
  }
  expect(a.length - prefix - suffix).toBe(0)
  return b.slice(prefix, b.length - suffix)
}

// ---------------------------------------------------------------------------
// Fixtures (module scope so template literals carry exact bytes, no indent)
// ---------------------------------------------------------------------------

const SINGLE = `# Payroll bronze ingestion
pipeline: hr_bronze
flowgroup: payroll_bronze
presets:
  - default_delta_properties
actions:
  # Load raw payroll rows
  - name: payroll_raw_load
    type: load
    readMode: stream
    source:
      type: delta
      catalog: main
      schema: raw
      table: payroll_raw
    target: v_payroll_raw
    description: "Load payroll from raw schema"

  - name: payroll_cleanse
    type: transform
    source: v_payroll_raw
    target: v_payroll_cleaned
    sql: |
      SELECT * FROM v_payroll_raw

  - name: payroll_dqe
    type: transform
    transform_type: data_quality
    source: v_payroll_cleaned
    target: v_payroll_dqe
    expectations_file: "expectations/payroll.json"

  - name: write_payroll
    type: write
    source: v_payroll_dqe
    depends_on: [main.ops.calendar]
    write_target:
      type: streaming_table
      catalog: main
      schema: bronze
      table: payroll

  - name: tst_payroll_uniqueness
    type: test
    test_type: uniqueness
    source: v_payroll_cleaned
    columns: [payroll_key]
    on_violation: warn
`

const MULTI = `# hr silver ingestion
pipeline: hr_silver
flowgroup: employee_silver
actions:
  - name: employee_load
    type: load
    source:
      type: delta
      catalog: main
      schema: bronze
      table: employee
    target: v_employee

  - name: write_employee
    type: write
    source: v_employee
    write_target:
      type: streaming_table
      table: employee_dim
---
# department flowgroup
pipeline: hr_silver
flowgroup: department_silver
actions:
  - name: department_load
    type: load
    source:
      type: delta
      catalog: main
      schema: bronze
      table: department
    target: v_department

  - name: write_department
    type: write
    source: v_department
    write_target:
      type: streaming_table
      table: department_dim
      mode: cdc
      cdc_config:
        keys: [department_key]
        sequence_by: _ts
`

const ARRAY = `# Domain A csv ingestion
pipeline: domain_a_raw
use_template: csv_ingestion_template
presets:
  - default_delta_properties
flowgroups:
  # first tenant
  - flowgroup: shipment_ingestion
    template_parameters:
      table_name: shipment_raw
      landing_folder: shipment
    actions:
      - name: tst_shipment_completeness
        type: test
        source: v_shipment_raw_cloudfiles
        required_columns: [_ts]
        on_violation: warn

  - flowgroup: refund_ingestion
    pipeline: domain_a_raw_two
    template_parameters:
      table_name: refund_raw
      landing_folder: refund
    actions:
      - name: tst_refund_completeness
        type: test
        test_type: completeness
        source: v_refund_raw_cloudfiles
        required_columns: [_ts]
        on_violation: warn
`

const FAN = `pipeline: p
flowgroup: fan
actions:
  - name: load_a
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS id"
    target: v_a
  - name: load_b
    type: load
    source: "SELECT 2 AS id"
    target: v_b
  - name: union_ab
    type: transform
    source: [v_a, v_b]
    target: v_union
  - name: enrich_a
    type: transform
    source: v_a
    target: v_enriched
  - name: write_union
    type: write
    source: v_union
    write_target:
      type: streaming_table
      table: t_union
`

const CDC = `pipeline: p
flowgroup: cdc_writes
actions:
  - name: staging
    type: transform
    source: v_in
    target: v_staged
  - name: write_cdc
    type: write
    write_target:
      type: streaming_table
      table: dim
      mode: cdc
      cdc_config:
        source: v_staged
        keys: [id]
        sequence_by: _ts
  - name: write_snap_fn
    type: write
    source: v_should_be_ignored
    write_target:
      type: streaming_table
      table: snap1
      mode: snapshot_cdc
      snapshot_cdc_config:
        source_function:
          file: snapshots/fn.py
          function: next_snapshot
        keys: [id]
  - name: write_snap_table
    type: write
    write_target:
      type: streaming_table
      table: snap2
      mode: snapshot_cdc
      snapshot_cdc_config:
        source: main.ops.snapshots
        keys: [id]
`

const MV = `pipeline: p
flowgroup: mv
actions:
  - name: prep
    type: transform
    source: v_src
    target: v_prepped
  - name: mv_from_sql
    type: write
    write_target:
      type: materialized_view
      table: summary
      sql: "SELECT count(*) FROM bronze.payroll"
  - name: mv_from_view
    type: write
    source: v_prepped
    write_target:
      type: materialized_view
      table: summary2
`

const TESTS_REF = `pipeline: p
flowgroup: quality
actions:
  - name: customers_load
    type: load
    source:
      type: delta
      table: customers
    target: v_customers
  - name: orders_load
    type: load
    source:
      type: delta
      table: orders
    target: v_orders
  - name: tst_ref_integrity
    type: test
    test_type: referential_integrity
    source: v_orders
    reference: v_customers
    source_columns: [customer_id]
    reference_columns: [customer_id]
  - name: tst_row_count
    type: test
    test_type: row_count
    source: [v_orders, v_customers]
  - name: tst_lookups
    type: test
    test_type: all_lookups_found
    source: v_orders
    lookup_table: main.ref.countries
    lookup_columns: [country_code]
`

const DUP = `pipeline: p
flowgroup: dup
actions:
  - name: load_x
    type: load
    source:
      type: delta
      table: t1
    target: v_x1
  - name: load_x
    type: load
    source:
      type: delta
      table: t2
    target: v_x2
  - name: mystery
    type: quantum
    source: v_x1
`

const MIXED = `flowgroup: solo
pipeline: p
actions: []
---
pipeline: p
flowgroups:
  - flowgroup: in_array
    actions: []
`

const BARE = `pipeline: p
flowgroup: fresh
`

const MALFORMED = `broken: [1, 2
next: : :
`

// ---------------------------------------------------------------------------
// 1. File forms + enumeration
// ---------------------------------------------------------------------------

describe('parseFlowgroupFile forms', () => {
  it('single-doc form yields one flowgroup', () => {
    const handle = parseFlowgroupFile(SINGLE)
    expect(handle.errors).toHaveLength(0)
    expect(handle.warnings).toHaveLength(0)
    expect(handle.flowgroups).toEqual([
      { name: 'payroll_bronze', pipeline: 'hr_bronze', form: 'single', index: 0 },
    ])
  })

  it('multi-doc form yields one flowgroup per document', () => {
    const handle = parseFlowgroupFile(MULTI)
    expect(handle.errors).toHaveLength(0)
    expect(handle.flowgroups).toEqual([
      { name: 'employee_silver', pipeline: 'hr_silver', form: 'multi', index: 0 },
      { name: 'department_silver', pipeline: 'hr_silver', form: 'multi', index: 1 },
    ])
  })

  it('array form yields one flowgroup per entry with root pipeline inherited', () => {
    const handle = parseFlowgroupFile(ARRAY)
    expect(handle.errors).toHaveLength(0)
    expect(handle.flowgroups).toEqual([
      { name: 'shipment_ingestion', pipeline: 'domain_a_raw', form: 'array', index: 0 },
      { name: 'refund_ingestion', pipeline: 'domain_a_raw_two', form: 'array', index: 1 },
    ])
  })

  it('mixed syntax enumerates both but carries the VAL_014 warning', () => {
    const handle = parseFlowgroupFile(MIXED)
    expect(handle.flowgroups.map((f) => [f.name, f.form])).toEqual([
      ['solo', 'multi'],
      ['in_array', 'array'],
    ])
    expect(handle.warnings.join('\n')).toMatch(/Mixed flowgroup syntax/)
  })

  it('duplicate flowgroup names carry the VAL_013 warning', () => {
    const handle = parseFlowgroupFile('flowgroup: fg\n---\nflowgroup: fg\n')
    expect(handle.flowgroups).toHaveLength(2)
    expect(handle.warnings.join('\n')).toMatch(/Duplicate flowgroup name 'fg'/)
  })

  it('blueprint instance documents are skipped with a warning', () => {
    const handle = parseFlowgroupFile('use_blueprint:\n  name: bp\n')
    expect(handle.flowgroups).toHaveLength(0)
    expect(handle.warnings.join('\n')).toMatch(/blueprint instance/)
  })

  it('blueprint definition documents are skipped with a warning', () => {
    const handle = parseFlowgroupFile(
      'parameters:\n  - name: x\nflowgroups:\n  - flowgroup: fg\n',
    )
    expect(handle.flowgroups).toHaveLength(0)
    expect(handle.warnings.join('\n')).toMatch(/blueprint definition/)
  })

  it('non-mapping and empty documents are tolerated', () => {
    const handle = parseFlowgroupFile('- a\n- b\n')
    expect(handle.flowgroups).toHaveLength(0)
    expect(handle.warnings.join('\n')).toMatch(/not a mapping/)
    expect(parseFlowgroupFile('').flowgroups).toHaveLength(0)
  })

  it('every fixture round-trips byte-identically when unmutated', () => {
    for (const src of [SINGLE, MULTI, ARRAY, FAN, CDC, MV, TESTS_REF, DUP, MIXED, BARE]) {
      expect(serializeFlowgroupFile(parseFlowgroupFile(src))).toBe(src)
    }
  })

  it('propagates parse errors and blocks mutation', () => {
    const handle = parseFlowgroupFile(MALFORMED)
    expect(handle.errors.length).toBeGreaterThan(0)
    const doc = selectFlowgroupAt(handle, 0)
    expect(doc).toBeDefined()
    expect(() => setFlowgroupField(doc as FlowgroupDocHandle, ['pipeline'], 'p')).toThrow(
      /parse error/,
    )
  })
})

describe('selectFlowgroup', () => {
  it('selects by name in every form and misses cleanly', () => {
    expect(selectFlowgroup(parseFlowgroupFile(SINGLE), 'payroll_bronze')).toBeDefined()
    expect(selectFlowgroup(parseFlowgroupFile(MULTI), 'department_silver')).toBeDefined()
    expect(selectFlowgroup(parseFlowgroupFile(ARRAY), 'refund_ingestion')).toBeDefined()
    expect(selectFlowgroup(parseFlowgroupFile(SINGLE), 'nope')).toBeUndefined()
  })

  it('selectFlowgroupAt addresses by ordinal', () => {
    const handle = parseFlowgroupFile(ARRAY)
    expect(selectFlowgroupAt(handle, 1)?.info.name).toBe('refund_ingestion')
    expect(selectFlowgroupAt(handle, 9)).toBeUndefined()
  })

  it('listFlowgroups re-enumerates after a rename; the snapshot stays', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    setFlowgroupField(doc, ['flowgroup'], 'payroll_bronze_v2')
    expect(handle.flowgroups[0].name).toBe('payroll_bronze')
    expect(listFlowgroups(handle)[0].name).toBe('payroll_bronze_v2')
    expect(selectFlowgroup(handle, 'payroll_bronze_v2')).toBeDefined()
  })
})

// ---------------------------------------------------------------------------
// 2. Read accessors
// ---------------------------------------------------------------------------

describe('readFlowgroupMeta', () => {
  it('reads direct fields on a single-doc flowgroup', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    const meta = readFlowgroupMeta(doc)
    expect(meta.pipeline).toBe('hr_bronze')
    expect(meta.flowgroup).toBe('payroll_bronze')
    expect(meta.presets).toEqual(['default_delta_properties'])
    expect(meta.job_name).toBeUndefined()
    expect(meta.inherited).toEqual([])
  })

  it('array form inherits root fields unless the entry overrides them', () => {
    const shipment = readFlowgroupMeta(mustSelect(ARRAY, 'shipment_ingestion').doc)
    expect(shipment.pipeline).toBe('domain_a_raw')
    expect(shipment.use_template).toBe('csv_ingestion_template')
    expect(shipment.presets).toEqual(['default_delta_properties'])
    expect(shipment.template_parameters).toEqual({
      table_name: 'shipment_raw',
      landing_folder: 'shipment',
    })
    expect([...shipment.inherited].sort()).toEqual(['pipeline', 'presets', 'use_template'])

    const refund = readFlowgroupMeta(mustSelect(ARRAY, 'refund_ingestion').doc)
    expect(refund.pipeline).toBe('domain_a_raw_two')
    expect([...refund.inherited].sort()).toEqual(['presets', 'use_template'])
  })

  it('readFlowgroupValue reaches raw nested values', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    expect(readFlowgroupValue(doc, ['actions', 0, 'source', 'table'])).toBe('payroll_raw')
    expect(readFlowgroupValue(doc, ['actions', 99, 'name'])).toBeUndefined()
  })
})

describe('listActions', () => {
  it('resolves kinds, sub-type defaults, targets, sources, and depends_on', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    const actions = listActions(doc)
    expect(actions.map((a) => [a.name, a.kind, a.subType])).toEqual([
      ['payroll_raw_load', 'load', 'delta'],
      ['payroll_cleanse', 'transform', 'sql'], // transform_type absent -> 'sql'
      ['payroll_dqe', 'transform', 'data_quality'],
      ['write_payroll', 'write', 'streaming_table'],
      ['tst_payroll_uniqueness', 'test', 'uniqueness'],
    ])
    expect(actions[0].target).toBe('v_payroll_raw')
    expect(actions[0].sources).toEqual(['main.raw.payroll_raw']) // catalog.schema.table
    expect(actions[0].raw.readMode).toBe('stream')
    expect(actions[3].sources).toEqual(['v_payroll_dqe'])
    expect(actions[3].dependsOn).toEqual(['main.ops.calendar'])
    expect(actions[3].writeMode).toBe('standard') // mode absent -> 'standard'
    expect(actions[4].sources).toEqual(['v_payroll_cleaned'])
    expect(actions.map((a) => a.index)).toEqual([0, 1, 2, 3, 4])
  })

  it('applies the remaining discriminator defaults', () => {
    const { doc } = mustSelect(ARRAY, 'shipment_ingestion')
    expect(listActions(doc)[0].subType).toBe('row_count') // test_type absent

    const fan = mustSelect(FAN, 'fan').doc
    const fanActions = listActions(fan)
    expect(fanActions[0].subType).toBe('sql') // dict source with type: sql
    expect(fanActions[1].subType).toBe('sql') // plain-string source -> sql

    const noType = mustSelect(
      'flowgroup: f\npipeline: p\nactions:\n  - name: w\n    type: write\n    source: v_x\n    write_target:\n      table: t\n',
      'f',
    ).doc
    expect(listActions(noType)[0].subType).toBe('streaming_table') // write_target.type absent
  })

  it("a load's plain-string source is SQL text, not a view reference", () => {
    const { doc } = mustSelect(FAN, 'fan')
    expect(listActions(doc)[1].sources).toEqual([])
  })

  it('CDC write source precedence mirrors extract_cdc_sources', () => {
    const cdc = listActions(mustSelect(CDC, 'cdc_writes').doc)
    expect(cdc[1].sources).toEqual(['v_staged']) // cdc_config.source wins
    expect(cdc[1].writeMode).toBe('cdc')
    expect(cdc[2].sources).toEqual([]) // source_function: self-contained, action.source ignored
    expect(cdc[3].sources).toEqual(['main.ops.snapshots'])

    const fallback = listActions(mustSelect(MULTI, 'department_silver').doc)
    expect(fallback[1].sources).toEqual(['v_department']) // cdc_config without source -> action.source
  })

  it('test actions also reference `reference` and `lookup_table`', () => {
    const actions = listActions(mustSelect(TESTS_REF, 'quality').doc)
    expect(actions[2].sources).toEqual(['v_orders', 'v_customers'])
    expect(actions[3].sources).toEqual(['v_orders', 'v_customers']) // list source
    expect(actions[4].sources).toEqual(['v_orders', 'main.ref.countries'])
  })
})

// ---------------------------------------------------------------------------
// 3. Graph derivation
// ---------------------------------------------------------------------------

describe('deriveGraph', () => {
  it('derives the linear chain with externals and depends_on', () => {
    const graph = deriveGraph(mustSelect(SINGLE, 'payroll_bronze').doc)
    expect(graph.warnings).toEqual([])
    expect(graph.nodes.map((n) => n.id)).toEqual([
      'payroll_raw_load',
      'payroll_cleanse',
      'payroll_dqe',
      'write_payroll',
      'tst_payroll_uniqueness',
    ])
    expect(graph.edges).toContainEqual({
      from: 'ext:main.raw.payroll_raw',
      to: 'payroll_raw_load',
      viewName: 'main.raw.payroll_raw',
      kind: 'data',
    })
    expect(graph.edges).toContainEqual({
      from: 'payroll_raw_load',
      to: 'payroll_cleanse',
      viewName: 'v_payroll_raw',
      kind: 'data',
    })
    expect(graph.edges).toContainEqual({
      from: 'payroll_dqe',
      to: 'write_payroll',
      viewName: 'v_payroll_dqe',
      kind: 'data',
    })
    expect(graph.edges).toContainEqual({
      from: 'ext:main.ops.calendar',
      to: 'write_payroll',
      viewName: 'main.ops.calendar',
      kind: 'depends_on',
    })
    expect(graph.edges).toContainEqual({
      from: 'payroll_cleanse',
      to: 'tst_payroll_uniqueness',
      viewName: 'v_payroll_cleaned',
      kind: 'data',
    })
    expect(graph.externals.map((e) => e.label).sort()).toEqual([
      'main.ops.calendar',
      'main.raw.payroll_raw',
    ])
    // Writes are terminal.
    expect(graph.edges.every((e) => e.from !== 'write_payroll')).toBe(true)
  })

  it('derives fan-in from a list source and fan-out from view reuse', () => {
    const graph = deriveGraph(mustSelect(FAN, 'fan').doc)
    const into = (id: string) => graph.edges.filter((e) => e.to === id)
    expect(into('union_ab').map((e) => e.from).sort()).toEqual(['load_a', 'load_b'])
    const fromA = graph.edges.filter((e) => e.viewName === 'v_a')
    expect(fromA.map((e) => e.to).sort()).toEqual(['enrich_a', 'union_ab'])
    // SQL-text load sources derive no externals.
    expect(graph.externals).toEqual([])
    expect(graph.edges).toHaveLength(4)
  })

  it('MV: sql-sourced has no inbound edge; view-sourced connects normally', () => {
    const graph = deriveGraph(mustSelect(MV, 'mv').doc)
    expect(graph.edges.filter((e) => e.to === 'mv_from_sql')).toEqual([])
    expect(graph.edges).toContainEqual({
      from: 'prep',
      to: 'mv_from_view',
      viewName: 'v_prepped',
      kind: 'data',
    })
    expect(graph.externals.map((e) => e.label)).toEqual(['v_src'])
  })

  it('snapshot_cdc: source_function has no inbound; explicit source is external', () => {
    const graph = deriveGraph(mustSelect(CDC, 'cdc_writes').doc)
    expect(graph.edges.filter((e) => e.to === 'write_snap_fn')).toEqual([])
    expect(JSON.stringify(graph)).not.toContain('v_should_be_ignored')
    expect(graph.edges).toContainEqual({
      from: 'ext:main.ops.snapshots',
      to: 'write_snap_table',
      viewName: 'main.ops.snapshots',
      kind: 'data',
    })
    // cdc_config.source resolves against in-flowgroup producers.
    expect(graph.edges).toContainEqual({
      from: 'staging',
      to: 'write_cdc',
      viewName: 'v_staged',
      kind: 'data',
    })
    expect(graph.externals.map((e) => e.label).sort()).toEqual([
      'main.ops.snapshots',
      'v_in',
    ])
  })

  it('test reference/lookup_table wire to producers or externals', () => {
    const graph = deriveGraph(mustSelect(TESTS_REF, 'quality').doc)
    const into = (id: string) => graph.edges.filter((e) => e.to === id)
    expect(into('tst_ref_integrity').map((e) => e.from).sort()).toEqual([
      'customers_load',
      'orders_load',
    ])
    expect(into('tst_row_count')).toHaveLength(2)
    expect(into('tst_lookups').map((e) => e.from).sort()).toEqual([
      'ext:main.ref.countries',
      'orders_load',
    ])
  })

  it('duplicate action names get deterministic #N ids and a warning', () => {
    const graph = deriveGraph(mustSelect(DUP, 'dup').doc)
    expect(graph.nodes.map((n) => n.id)).toEqual(['load_x', 'load_x#2', 'mystery'])
    expect(graph.warnings.join('\n')).toMatch(/Duplicate action name 'load_x'/)
    expect(graph.nodes[2].kind).toBe('unknown')
    expect(graph.nodes[2].subType).toBe('unknown')
    expect(graph.edges).toContainEqual({
      from: 'load_x',
      to: 'mystery',
      viewName: 'v_x1',
      kind: 'data',
    })
  })
})

// ---------------------------------------------------------------------------
// 4. Mutators — comment preservation is the core contract
// ---------------------------------------------------------------------------

describe('surgical mutators', () => {
  it('setActionField on an existing scalar changes exactly one line', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    setActionField(doc, 'write_payroll', ['write_target', 'table'], 'payroll_v2')
    expect(serializeFlowgroupFile(handle)).toBe(
      SINGLE.replace('      table: payroll\n', '      table: payroll_v2\n'),
    )
  })

  it('setActionField adding a new key is a pure insertion', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    setActionField(doc, 'payroll_cleanse', ['description'], 'Cleanse payroll')
    const inserted = expectPureInsertion(SINGLE, serializeFlowgroupFile(handle))
    expect(inserted).toEqual(['    description: Cleanse payroll'])
  })

  it('setFlowgroupField adding a new key is a pure insertion', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    setFlowgroupField(doc, ['job_name'], 'nightly_hr')
    const inserted = expectPureInsertion(SINGLE, serializeFlowgroupFile(handle))
    expect(inserted).toEqual(['job_name: nightly_hr'])
  })

  it('renameAction changes exactly the name line and keeps graph ids in sync', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    renameAction(doc, 'payroll_cleanse', 'payroll_scrub')
    const out = serializeFlowgroupFile(handle)
    const line = expectSingleLineDiff(SINGLE, out)
    expect(out.split('\n')[line]).toBe('  - name: payroll_scrub')
    expect(deriveGraph(doc).nodes.map((n) => n.id)).toContain('payroll_scrub')
  })

  it('renameAction rejects collisions and empty names', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    expect(() => renameAction(doc, 'payroll_cleanse', 'payroll_dqe')).toThrow(/already exists/)
    expect(() => renameAction(doc, 'payroll_cleanse', '')).toThrow(/empty/)
  })

  it('mutating document 2 of a multi-doc file leaves document 1 byte-exact', () => {
    const { handle, doc } = mustSelect(MULTI, 'department_silver')
    setActionField(doc, 'write_department', ['write_target', 'table'], 'dept_dim_v2')
    expect(serializeFlowgroupFile(handle)).toBe(
      MULTI.replace('      table: department_dim\n', '      table: dept_dim_v2\n'),
    )
  })

  it('mutating array entry 2 leaves entry 1 byte-exact', () => {
    const { handle, doc } = mustSelect(ARRAY, 'refund_ingestion')
    setFlowgroupField(doc, ['template_parameters', 'schema_file'], 'refund_schema')
    const inserted = expectPureInsertion(ARRAY, serializeFlowgroupFile(handle))
    expect(inserted).toEqual(['      schema_file: refund_schema'])
  })

  it('deleteActionField removes exactly the addressed line', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    deleteActionField(doc, 'payroll_raw_load', ['description'])
    expect(serializeFlowgroupFile(handle)).toBe(
      flowNormalized(SINGLE.replace('    description: "Load payroll from raw schema"\n', '')),
    )
  })

  it('deleteFlowgroupField removes the addressed subtree and no-ops when missing', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    deleteFlowgroupField(doc, ['nope'])
    expect(serializeFlowgroupFile(handle)).toBe(SINGLE) // no-op: still byte-exact
    deleteFlowgroupField(doc, ['presets'])
    expect(serializeFlowgroupFile(handle)).toBe(
      flowNormalized(SINGLE.replace('presets:\n  - default_delta_properties\n', '')),
    )
  })

  it('addresses the Nth duplicate via the #N node id', () => {
    const { handle, doc } = mustSelect(DUP, 'dup')
    setActionField(doc, 'load_x#2', ['description'], 'second copy')
    const inserted = expectPureInsertion(DUP, serializeFlowgroupFile(handle))
    expect(inserted).toEqual(['    description: second copy'])
    expect(listActions(doc)[1].raw.description).toBe('second copy')
    expect(listActions(doc)[0].raw.description).toBeUndefined()
  })

  it('throws on unknown action names', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    expect(() => setActionField(doc, 'nope', ['x'], 1)).toThrow(/not found/)
    expect(() => deleteAction(doc, 'nope')).toThrow(/not found/)
    expect(() => addAction(doc, { name: 'n' }, 'nope')).toThrow(/not found/)
  })
})

describe('structural mutators (document rewrite path)', () => {
  it('addAction appends and keeps every comment and sibling action', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    addAction(doc, {
      name: 'tst_new',
      type: 'test',
      test_type: 'row_count',
      source: ['v_payroll_raw', 'v_payroll_cleaned'],
    })
    const out = serializeFlowgroupFile(handle)
    expect(listActions(doc).map((a) => a.name)).toEqual([
      'payroll_raw_load',
      'payroll_cleanse',
      'payroll_dqe',
      'write_payroll',
      'tst_payroll_uniqueness',
      'tst_new',
    ])
    // Comments, quoting, and the block scalar survive the rewrite.
    expect(out).toContain('# Payroll bronze ingestion')
    expect(out).toContain('# Load raw payroll rows')
    expect(out).toContain('description: "Load payroll from raw schema"')
    expect(out).toContain('sql: |')
    expect(out).toContain('SELECT * FROM v_payroll_raw')
    // The re-parsed file agrees with the handle.
    const reparsed = mustSelect(out, 'payroll_bronze').doc
    expect(listActions(reparsed)).toHaveLength(6)
    expect(listActions(reparsed)[5].sources).toEqual(['v_payroll_raw', 'v_payroll_cleaned'])
  })

  it('addAction(after) inserts at the right position', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    addAction(
      doc,
      { name: 'mid', type: 'transform', source: 'v_payroll_raw', target: 'v_mid' },
      'payroll_cleanse',
    )
    expect(listActions(doc).map((a) => a.name)).toEqual([
      'payroll_raw_load',
      'payroll_cleanse',
      'mid',
      'payroll_dqe',
      'write_payroll',
      'tst_payroll_uniqueness',
    ])
  })

  it('addAction creates the actions list on a bare flowgroup', () => {
    const { handle, doc } = mustSelect(BARE, 'fresh')
    addAction(doc, { name: 'first', type: 'load', target: 'v_first' })
    expect(listActions(doc).map((a) => a.name)).toEqual(['first'])
    const reparsed = parseFlowgroupFile(serializeFlowgroupFile(handle))
    expect(reparsed.errors).toHaveLength(0)
  })

  it('addAction into a multi-doc file leaves the sibling document byte-exact', () => {
    const { handle, doc } = mustSelect(MULTI, 'department_silver')
    addAction(doc, { name: 'extra', type: 'transform', source: 'v_department', target: 'v_x' })
    const out = serializeFlowgroupFile(handle)
    const [firstDocBefore] = MULTI.split('---\n')
    const [firstDocAfter] = out.split('---\n')
    expect(firstDocAfter).toBe(firstDocBefore)
    expect(listActions(doc).map((a) => a.name)).toEqual([
      'department_load',
      'write_department',
      'extra',
    ])
  })

  it('deleteAction removes the action with its comments; siblings survive', () => {
    const { handle, doc } = mustSelect(SINGLE, 'payroll_bronze')
    deleteAction(doc, 'payroll_dqe')
    const out = serializeFlowgroupFile(handle)
    expect(listActions(doc).map((a) => a.name)).toEqual([
      'payroll_raw_load',
      'payroll_cleanse',
      'write_payroll',
      'tst_payroll_uniqueness',
    ])
    expect(out).not.toContain('- name: payroll_dqe')
    expect(out).not.toContain('expectations_file')
    expect(out).toContain('# Load raw payroll rows')
    expect(out).toContain('description: "Load payroll from raw schema"')
    expect(mustSelect(out, 'payroll_bronze').doc).toBeDefined()
  })

  it('duplicateAction deep-copies under a deterministic unique name', () => {
    const { doc } = mustSelect(SINGLE, 'payroll_bronze')
    expect(duplicateAction(doc, 'payroll_cleanse')).toBe('payroll_cleanse_copy')
    expect(duplicateAction(doc, 'payroll_cleanse')).toBe('payroll_cleanse_copy2')
    const names = listActions(doc).map((a) => a.name)
    expect(names).toEqual([
      'payroll_raw_load',
      'payroll_cleanse',
      'payroll_cleanse_copy2',
      'payroll_cleanse_copy',
      'payroll_dqe',
      'write_payroll',
      'tst_payroll_uniqueness',
    ])
    const original = listActions(doc)[1].raw
    const copy = listActions(doc)[3].raw
    expect(copy).toEqual({ ...original, name: 'payroll_cleanse_copy' })
  })

  it('mutations on an array-form entry never touch the other entry', () => {
    const { handle, doc } = mustSelect(ARRAY, 'refund_ingestion')
    addAction(doc, { name: 'tst_extra', type: 'test', source: 'v_refund_raw_cloudfiles' })
    deleteAction(doc, 'tst_refund_completeness')
    const out = serializeFlowgroupFile(handle)
    // The shipment entry's lines are still present verbatim.
    for (const line of [
      '  - flowgroup: shipment_ingestion',
      '      table_name: shipment_raw',
      '      - name: tst_shipment_completeness',
      '  # first tenant',
    ]) {
      expect(out).toContain(line)
    }
    expect(listActions(mustSelect(out, 'refund_ingestion').doc).map((a) => a.name)).toEqual([
      'tst_extra',
    ])
    expect(
      listActions(mustSelect(out, 'shipment_ingestion').doc).map((a) => a.name),
    ).toEqual(['tst_shipment_completeness'])
  })
})

// ---------------------------------------------------------------------------
// 5. Real flowgroup files from Example_Projects/performance_testing
// ---------------------------------------------------------------------------

describe('real project files', () => {
  const REAL_FILES = [
    ['02_bronze/domain_b/payroll_bronze.yaml', 'single'],
    ['01_raw/domain_a/domain_a_csv_batch.yaml', 'array'],
    ['03_silver/domain_a/domain_a_silver_batch_1.yaml', 'multi'],
  ] as const

  it.each(REAL_FILES)('%s round-trips byte-identically', (relPath) => {
    const src = readExample(relPath)
    const handle = parseFlowgroupFile(src)
    expect(handle.errors).toHaveLength(0)
    expect(serializeFlowgroupFile(handle)).toBe(src)
  })

  it.each(REAL_FILES)('%s enumerates as %s form', (relPath, form) => {
    const handle = parseFlowgroupFile(readExample(relPath))
    expect(handle.flowgroups.length).toBeGreaterThan(0)
    expect(handle.flowgroups.every((f) => f.form === form)).toBe(true)
    expect(handle.flowgroups.every((f) => f.name !== '')).toBe(true)
  })

  it('derives a graph with no warnings for payroll_bronze.yaml', () => {
    const src = readExample('02_bronze/domain_b/payroll_bronze.yaml')
    const handle = parseFlowgroupFile(src)
    const doc = selectFlowgroup(handle, 'payroll_bronze')
    expect(doc).toBeDefined()
    const graph = deriveGraph(doc as FlowgroupDocHandle)
    expect(graph.warnings).toEqual([])
    expect(graph.nodes.length).toBe(listActions(doc as FlowgroupDocHandle).length)
    expect(graph.edges).toContainEqual({
      from: 'payroll_raw_load',
      to: 'payroll_bronze_cleanse',
      viewName: 'v_payroll_raw',
      kind: 'data',
    })
  })
})
