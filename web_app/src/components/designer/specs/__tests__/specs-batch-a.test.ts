// Batch A specs (load ×6 + transform ×4): registry resolution, per-spec
// set+delete round-trip on realistic fixtures, and cross-field rule firing.
// Self-contained (own helper copies), mirroring specs.test.ts.

import { describe, expect, it } from 'vitest'
import {
  deleteActionField,
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
  setActionField,
} from '@/lib/flowgroup-doc'
import type { ActionKind, ActionSubTypeSpec, FieldSpec } from '../types'
import { readPath } from '../helpers'
import { coerceValue, computeIssues, pathKey } from '../../formModel'
import { getActionSpec } from '../registry'
import { loadDeltaSpec } from '../load-delta'
import { loadSqlSpec } from '../load-sql'
import { loadPythonSpec } from '../load-python'
import { loadJdbcSpec } from '../load-jdbc'
import { loadCustomDatasourceSpec } from '../load-custom-datasource'
import { loadKafkaSpec } from '../load-kafka'
import { transformPythonSpec } from '../transform-python'
import { transformDataQualitySpec } from '../transform-data-quality'
import { transformTempTableSpec } from '../transform-temp-table'
import { transformSchemaSpec } from '../transform-schema'

// ── realistic fixtures (grounded in real project pipelines) ──

const DELTA_YAML = `pipeline: perf
flowgroup: fg_delta
actions:
  - name: load_delta
    type: load
    readMode: stream
    source:
      type: delta
      catalog: "\${catalog}"
      schema: "\${bronze_schema}"
      table: orders
      options:
        readChangeFeed: "true"
    target: v_orders_raw
`

const LOAD_SQL_YAML = `pipeline: perf
flowgroup: fg_load_sql
actions:
  - name: load_sql
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS id, 'a' AS name"
    target: v_seed
`

const LOAD_PY_YAML = `pipeline: perf
flowgroup: fg_load_py
actions:
  - name: load_py
    type: load
    readMode: batch
    source:
      type: python
      module_path: "loaders/external.py"
      function_name: get_df
      parameters:
        region: us
    target: v_external
`

const JDBC_YAML = `pipeline: perf
flowgroup: fg_jdbc
actions:
  - name: load_jdbc
    type: load
    readMode: batch
    source:
      type: jdbc
      url: "jdbc:postgresql://host:5432/db"
      driver: "org.postgresql.Driver"
      user: "\${secret:db/user}"
      password: "\${secret:db/password}"
      table: "public.products"
    target: v_products
`

const CDS_YAML = `pipeline: perf
flowgroup: fg_cds
actions:
  - name: load_cds
    type: load
    readMode: stream
    source:
      type: custom_datasource
      module_path: "sources/yahoo.py"
      custom_datasource_class: YahooSource
      options:
        tickers: "AAPL,MSFT"
    target: v_stock_raw
`

// The kafka example (from Example_Projects/performance_testing) authors readMode
// UNDER source; the spec authors it top-level (both are honored by the generator).
const KAFKA_YAML = `pipeline: perf
flowgroup: fg_kafka
actions:
  - name: load_kafka
    type: load
    source:
      type: kafka
      bootstrap_servers: "\${kafka_bootstrap_servers}"
      subscribe: "\${kafka_source_topic}"
      readMode: stream
      options:
        kafka.security.protocol: "SASL_SSL"
        startingOffsets: "latest"
    target: v_kafka_events_raw
`

const T_PY_YAML = `pipeline: perf
flowgroup: fg_t_py
actions:
  - name: t_py
    type: transform
    transform_type: python
    source: v_seed
    module_path: "transforms/enrich.py"
    function_name: enrich
    parameters:
      lookup: regions
    target: v_enriched
`

const DQ_YAML = `pipeline: perf
flowgroup: fg_dq
actions:
  - name: t_dq
    type: transform
    transform_type: data_quality
    source: v_cleaned
    target: v_validated
    readMode: stream
    expectations_file: "expectations/quality.yaml"
    mode: dqe
`

const TEMP_TABLE_YAML = `pipeline: perf
flowgroup: fg_tt
actions:
  - name: t_tt
    type: transform
    transform_type: temp_table
    source: v_raw_orders
    target: staging_orders
    readMode: stream
    sql: "SELECT * FROM {source}"
`

const SCHEMA_YAML = `pipeline: perf
flowgroup: fg_sch
actions:
  - name: t_sch
    type: transform
    transform_type: schema
    source: v_bronze
    enforcement: permissive
    schema_inline: |
      columns:
        - "customer_id -> customer_id: BIGINT"
    target: v_typed
    readMode: stream
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
  { spec: loadDeltaSpec, kind: 'load', subType: 'delta', yaml: DELTA_YAML, action: 'load_delta', flowgroup: 'fg_delta' },
  { spec: loadSqlSpec, kind: 'load', subType: 'sql', yaml: LOAD_SQL_YAML, action: 'load_sql', flowgroup: 'fg_load_sql' },
  { spec: loadPythonSpec, kind: 'load', subType: 'python', yaml: LOAD_PY_YAML, action: 'load_py', flowgroup: 'fg_load_py' },
  { spec: loadJdbcSpec, kind: 'load', subType: 'jdbc', yaml: JDBC_YAML, action: 'load_jdbc', flowgroup: 'fg_jdbc' },
  {
    spec: loadCustomDatasourceSpec,
    kind: 'load',
    subType: 'custom_datasource',
    yaml: CDS_YAML,
    action: 'load_cds',
    flowgroup: 'fg_cds',
  },
  { spec: loadKafkaSpec, kind: 'load', subType: 'kafka', yaml: KAFKA_YAML, action: 'load_kafka', flowgroup: 'fg_kafka' },
  { spec: transformPythonSpec, kind: 'transform', subType: 'python', yaml: T_PY_YAML, action: 't_py', flowgroup: 'fg_t_py' },
  {
    spec: transformDataQualitySpec,
    kind: 'transform',
    subType: 'data_quality',
    yaml: DQ_YAML,
    action: 't_dq',
    flowgroup: 'fg_dq',
  },
  {
    spec: transformTempTableSpec,
    kind: 'transform',
    subType: 'temp_table',
    yaml: TEMP_TABLE_YAML,
    action: 't_tt',
    flowgroup: 'fg_tt',
  },
  { spec: transformSchemaSpec, kind: 'transform', subType: 'schema', yaml: SCHEMA_YAML, action: 't_sch', flowgroup: 'fg_sch' },
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
    case 'objectList':
      return undefined // not settable via the form yet
    default:
      return 'probe_value'
  }
}

/** Every declared field path must set + read back + delete against a realistic fixture. */
function assertFieldsRoundTrip(spec: ActionSubTypeSpec, yaml: string, actionName: string, flowgroup: string): void {
  const fields = spec.groups.flatMap((group) => group.fields)
  for (const field of fields) {
    const value = probe(field)
    if (value === undefined) continue

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

describe('registry — batch A specs resolve by (kind, subType)', () => {
  for (const c of CASES) {
    it(`resolves ${c.kind}/${c.subType}`, () => {
      expect(getActionSpec(c.kind, c.subType)).toBe(c.spec)
    })
  }
})

describe('batch A specs — every field path set+deletes on a realistic fixture', () => {
  for (const c of CASES) {
    it(`${c.kind}/${c.subType}`, () => {
      const handle = parseFlowgroupFile(c.yaml)
      expect(handle.errors).toEqual([])
      assertFieldsRoundTrip(c.spec, c.yaml, c.action, c.flowgroup)
    })
  }
})

describe('load/delta spec — reads present fields', () => {
  it('reads catalog/table/options from a realistic fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(DELTA_YAML), 'fg_delta')!
    const action = listActions(doc).find((a) => a.name === 'load_delta')!
    expect(readPath(action.raw, ['source', 'table'])).toBe('orders')
    expect(readPath(action.raw, ['source', 'options', 'readChangeFeed'])).toBe('true')
    expect(readPath(action.raw, ['target'])).toBe('v_orders_raw')
  })
})

describe('load/kafka spec — reads present fields', () => {
  it('reads bootstrap_servers/subscribe/target from the perf kafka fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(KAFKA_YAML), 'fg_kafka')!
    const action = listActions(doc).find((a) => a.name === 'load_kafka')!
    expect(readPath(action.raw, ['source', 'bootstrap_servers'])).toBe('${kafka_bootstrap_servers}')
    expect(readPath(action.raw, ['source', 'subscribe'])).toBe('${kafka_source_topic}')
    expect(readPath(action.raw, ['target'])).toBe('v_kafka_events_raw')
  })
})

describe('load/data_quality spec — reads present fields', () => {
  it('reads expectations_file/mode/source from a realistic fixture', () => {
    const doc = selectFlowgroup(parseFlowgroupFile(DQ_YAML), 'fg_dq')!
    const action = listActions(doc).find((a) => a.name === 't_dq')!
    expect(readPath(action.raw, ['source'])).toBe('v_cleaned')
    expect(readPath(action.raw, ['expectations_file'])).toBe('expectations/quality.yaml')
    expect(readPath(action.raw, ['mode'])).toBe('dqe')
  })
})

describe('batch A specs — cross-field rules', () => {
  it('load/sql: xor(source.sql, source.sql_path)', () => {
    const both = computeIssues(loadSqlSpec, { source: { type: 'sql', sql: 'x', sql_path: 'y' }, target: 't' })
    expect(both.get(pathKey(['source', 'sql']))).toMatch(/exactly one/i)
    expect(both.get(pathKey(['source', 'sql_path']))).toMatch(/exactly one/i)

    const one = computeIssues(loadSqlSpec, { source: { type: 'sql', sql: 'x' }, target: 't' })
    expect(one.has(pathKey(['source', 'sql']))).toBe(false)
    expect(one.has(pathKey(['source', 'sql_path']))).toBe(false)
  })

  it('load/jdbc: xor(source.table, source.query)', () => {
    const both = computeIssues(loadJdbcSpec, {
      source: { url: 'u', driver: 'd', user: 'x', password: 'y', table: 't', query: 'q' },
      target: 'v',
    })
    expect(both.get(pathKey(['source', 'table']))).toMatch(/exactly one/i)

    const one = computeIssues(loadJdbcSpec, {
      source: { url: 'u', driver: 'd', user: 'x', password: 'y', table: 't' },
      target: 'v',
    })
    expect(one.has(pathKey(['source', 'table']))).toBe(false)
    expect(one.has(pathKey(['source', 'query']))).toBe(false)
  })

  it('load/kafka: xor(subscribe/subscribePattern/assign) + readMode must be stream', () => {
    const twoSubs = computeIssues(loadKafkaSpec, {
      source: { bootstrap_servers: 'b', subscribe: 'a', assign: 'z' },
      target: 'v',
    })
    expect(twoSubs.get(pathKey(['source', 'subscribe']))).toMatch(/exactly one/i)
    expect(twoSubs.get(pathKey(['source', 'assign']))).toMatch(/exactly one/i)

    const oneSub = computeIssues(loadKafkaSpec, {
      source: { bootstrap_servers: 'b', subscribe: 'a' },
      target: 'v',
    })
    expect(oneSub.has(pathKey(['source', 'subscribe']))).toBe(false)

    const batch = computeIssues(loadKafkaSpec, {
      readMode: 'batch',
      source: { bootstrap_servers: 'b', subscribe: 'a' },
      target: 'v',
    })
    expect(batch.get(pathKey(['readMode']))).toMatch(/stream/i)

    const stream = computeIssues(loadKafkaSpec, {
      readMode: 'stream',
      source: { bootstrap_servers: 'b', subscribe: 'a' },
      target: 'v',
    })
    expect(stream.has(pathKey(['readMode']))).toBe(false)
  })

  it('transform/data_quality: readMode must be stream + quarantine fields only required when visible', () => {
    const batch = computeIssues(transformDataQualitySpec, {
      source: 'v',
      expectations_file: 'f',
      readMode: 'batch',
      target: 't',
    })
    expect(batch.get(pathKey(['readMode']))).toMatch(/stream/i)

    // mode dqe → quarantine group hidden → no required hints on its fields
    const dqe = computeIssues(transformDataQualitySpec, {
      source: 'v',
      expectations_file: 'f',
      target: 't',
    })
    expect(dqe.has(pathKey(['quarantine', 'dlq_table']))).toBe(false)

    // mode quarantine → group visible → dlq_table/source_table required-missing
    const quarantine = computeIssues(transformDataQualitySpec, {
      source: 'v',
      expectations_file: 'f',
      mode: 'quarantine',
      target: 't',
    })
    expect(quarantine.get(pathKey(['quarantine', 'dlq_table']))).toMatch(/required/i)
    expect(quarantine.get(pathKey(['quarantine', 'source_table']))).toMatch(/required/i)
  })

  it('transform/schema: xor(schema_inline, schema_file)', () => {
    const both = computeIssues(transformSchemaSpec, {
      source: 'v',
      schema_inline: 'x',
      schema_file: 'y',
      target: 't',
    })
    expect(both.get(pathKey(['schema_inline']))).toMatch(/exactly one/i)
    expect(both.get(pathKey(['schema_file']))).toMatch(/exactly one/i)

    const one = computeIssues(transformSchemaSpec, { source: 'v', schema_inline: 'x', target: 't' })
    expect(one.has(pathKey(['schema_inline']))).toBe(false)
    expect(one.has(pathKey(['schema_file']))).toBe(false)
  })
})
