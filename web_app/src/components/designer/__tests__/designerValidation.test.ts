import { describe, expect, it } from 'vitest'
import { mapIssuesToNodes, textMentionsName } from '../designerValidation'
import type { ValidationNode } from '../designerValidation'
import type { ValidationIssue } from '@/types/api'

function issue(partial: Partial<ValidationIssue>): ValidationIssue {
  return {
    code: 'LHP-000',
    category: 'general',
    severity: 'error',
    title: '',
    details: null,
    pipeline_name: 'p',
    flowgroup_name: 'fg',
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
    ...partial,
  }
}

const NODES: ValidationNode[] = [
  { id: 'load_orders', name: 'load_orders' },
  { id: 'orders', name: 'orders' },
  { id: 'orders_clean', name: 'orders_clean' },
]

describe('textMentionsName', () => {
  it('matches a whole-token occurrence', () => {
    expect(textMentionsName("view 'orders' is missing", 'orders')).toBe(true)
    expect(textMentionsName('the orders.', 'orders')).toBe(true)
  })

  it('does not match a substring of a longer identifier', () => {
    expect(textMentionsName('orders_clean has no target', 'orders')).toBe(false)
  })

  it('is false for an empty name', () => {
    expect(textMentionsName('anything', '')).toBe(false)
  })
})

describe('mapIssuesToNodes', () => {
  it('attributes a name-mentioning issue to its node and counts it', () => {
    const result = mapIssuesToNodes(
      [issue({ severity: 'error', title: "Action 'load_orders' has no target" })],
      NODES,
      'fg',
      'p',
    )
    expect(result.perNode.get('load_orders')).toEqual({ errors: 1, warnings: 0 })
    expect(result.issues).toHaveLength(1)
    expect(result.issues[0].nodeId).toBe('load_orders')
    expect(result.errorCount).toBe(1)
    expect(result.warningCount).toBe(0)
  })

  it('leaves an unmappable issue flowgroup-level (nodeId null) but still counts it', () => {
    const result = mapIssuesToNodes(
      [issue({ severity: 'warning', title: 'Pipeline has no output', details: 'nothing here' })],
      NODES,
      'fg',
      'p',
    )
    expect(result.perNode.size).toBe(0)
    expect(result.issues[0].nodeId).toBeNull()
    expect(result.warningCount).toBe(1)
  })

  it('prefers the longest (most specific) matching name', () => {
    const result = mapIssuesToNodes(
      [issue({ title: "orders_clean references missing 'orders'" })],
      NODES,
      'fg',
      'p',
    )
    expect(result.issues[0].nodeId).toBe('orders_clean')
    expect(result.perNode.get('orders_clean')).toEqual({ errors: 1, warnings: 0 })
  })

  it('excludes other flowgroups but keeps pipeline-level (null-flowgroup) issues', () => {
    const result = mapIssuesToNodes(
      [
        issue({ flowgroup_name: 'other', title: "load_orders broken" }),
        issue({ flowgroup_name: null, pipeline_name: 'p', title: 'pipeline problem' }),
      ],
      NODES,
      'fg',
      'p',
    )
    // The other-flowgroup issue is dropped; the pipeline-level one is kept.
    expect(result.issues).toHaveLength(1)
    expect(result.issues[0].issue.title).toBe('pipeline problem')
    expect(result.issues[0].nodeId).toBeNull()
    expect(result.errorCount).toBe(1)
  })

  it('accumulates multiple issues onto the same node', () => {
    const result = mapIssuesToNodes(
      [
        issue({ severity: 'error', title: "load_orders failed" }),
        issue({ severity: 'warning', title: "load_orders is slow" }),
      ],
      NODES,
      'fg',
      'p',
    )
    expect(result.perNode.get('load_orders')).toEqual({ errors: 1, warnings: 1 })
  })
})
