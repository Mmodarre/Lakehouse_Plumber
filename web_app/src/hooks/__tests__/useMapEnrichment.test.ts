import { describe, expect, it } from 'vitest'
import { buildMapEnrichment } from '../useMapEnrichment'
import type { TableSummary, ValidationIssue } from '../../types/api'

function issue(over: Partial<ValidationIssue> = {}): ValidationIssue {
  return {
    code: 'LHP-VAL-001',
    category: 'config',
    severity: 'warning',
    title: 't',
    details: null,
    pipeline_name: null,
    flowgroup_name: null,
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
    ...over,
  }
}

function table(over: Partial<TableSummary> = {}): TableSummary {
  return {
    full_name: 'main.bronze.customers',
    target_type: 'streaming_table',
    pipeline: 'bronze',
    flowgroup: 'bronze_customers',
    write_mode: null,
    scd_type: null,
    source_file: 'pipelines/bronze/customers.yaml',
    ...over,
  }
}

describe('buildMapEnrichment — severity join', () => {
  it('takes the max severity per flowgroup (error > warning)', () => {
    const e = buildMapEnrichment(
      [
        issue({ pipeline_name: 'bronze', flowgroup_name: 'fg', severity: 'warning' }),
        issue({ pipeline_name: 'bronze', flowgroup_name: 'fg', severity: 'error' }),
      ],
      [],
    )
    expect(e.severityFor('bronze', 'fg')).toBe('error')
  })

  it('rolls a pipeline node up to the max over its flowgroups', () => {
    const e = buildMapEnrichment(
      [
        issue({ pipeline_name: 'bronze', flowgroup_name: 'a', severity: 'warning' }),
        issue({ pipeline_name: 'bronze', flowgroup_name: 'b', severity: 'error' }),
      ],
      [],
    )
    expect(e.severityFor('bronze')).toBe('error')
    expect(e.severityFor('bronze', 'a')).toBe('warning')
  })

  it('excludes issues without a flowgroup_name (Problems-only)', () => {
    const e = buildMapEnrichment(
      [issue({ pipeline_name: 'bronze', flowgroup_name: null, severity: 'error' })],
      [],
    )
    expect(e.severityFor('bronze')).toBeUndefined()
    expect(e.severityFor('bronze', 'fg')).toBeUndefined()
  })

  it('returns undefined for an unaffected (pipeline, flowgroup)', () => {
    const e = buildMapEnrichment([], [])
    expect(e.severityFor('x', 'y')).toBeUndefined()
  })
})

describe('buildMapEnrichment — FQN hints', () => {
  it('gives the single full_name when one dataset is produced', () => {
    const e = buildMapEnrichment([], [table({ full_name: 'main.bronze.customers' })])
    expect(e.fqnHintFor('bronze', 'bronze_customers')).toBe('main.bronze.customers')
  })

  it('appends +n when a flowgroup produces more than one dataset', () => {
    const e = buildMapEnrichment(
      [],
      [
        table({ full_name: 'main.bronze.customers' }),
        table({ full_name: 'main.bronze.customers_audit' }),
        table({ full_name: 'main.bronze.customers_errors' }),
      ],
    )
    expect(e.fqnHintFor('bronze', 'bronze_customers')).toBe('main.bronze.customers +2')
  })

  it('is undefined when the flowgroup produces nothing', () => {
    const e = buildMapEnrichment([], [table()])
    expect(e.fqnHintFor('bronze', 'other')).toBeUndefined()
  })
})

describe('buildMapEnrichment — enrichNode routing', () => {
  it('routes a pipeline node to the pipeline-wide max + pipeline FQN', () => {
    const e = buildMapEnrichment(
      [issue({ pipeline_name: 'bronze', flowgroup_name: 'a', severity: 'error' })],
      [table({ full_name: 'main.bronze.customers' })],
    )
    expect(e.enrichNode({ nodeType: 'pipeline', pipeline: 'bronze' })).toEqual({
      severity: 'error',
      fqn: 'main.bronze.customers',
    })
  })

  it('routes a flowgroup node to its own (pipeline, flowgroup)', () => {
    const e = buildMapEnrichment(
      [issue({ pipeline_name: 'bronze', flowgroup_name: 'bronze_customers', severity: 'warning' })],
      [table()],
    )
    expect(e.enrichNode({ nodeType: 'flowgroup', pipeline: 'bronze', flowgroup: 'bronze_customers' })).toEqual({
      severity: 'warning',
      fqn: 'main.bronze.customers',
    })
  })

  it('returns an empty enrichment for a node with no pipeline', () => {
    const e = buildMapEnrichment([issue({ pipeline_name: 'bronze', flowgroup_name: 'a' })], [table()])
    expect(e.enrichNode({ nodeType: 'external' })).toEqual({})
  })
})
