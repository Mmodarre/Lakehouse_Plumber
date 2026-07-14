import { describe, expect, it } from 'vitest'
import { scopeIssues } from '../scopeIssues'
import type { ValidationIssue } from '../../../../types/api'
import type { WorkspaceTabRef } from '../../../../store/workspaceStore'

function issue(overrides: Partial<ValidationIssue> = {}): ValidationIssue {
  return {
    code: 'LHP-VAL-001',
    category: 'validation',
    severity: 'error',
    title: 'Something is wrong',
    details: null,
    pipeline_name: null,
    flowgroup_name: null,
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
    ...overrides,
  }
}

const bronzeCustomers = issue({
  pipeline_name: 'bronze',
  flowgroup_name: 'customers',
  file_path: 'pipelines/bronze/customers.yaml',
})
const bronzeOrders = issue({
  code: 'SUB-007',
  pipeline_name: 'bronze',
  flowgroup_name: 'orders',
  file_path: 'pipelines/bronze/orders.yaml',
})
const projectWide = issue({ code: 'GEN-000', pipeline_name: null, flowgroup_name: null })
const all = [bronzeCustomers, bronzeOrders, projectWide]

describe('scopeIssues', () => {
  it('returns the whole project for a null active tab', () => {
    const { issues, scope } = scopeIssues(all, null)
    expect(issues).toHaveLength(3)
    expect(scope).toEqual({ label: 'project', projectWide: true })
  })

  it('returns the whole project for the project-map tab', () => {
    const tab: WorkspaceTabRef = { kind: 'project-map' }
    const { issues, scope } = scopeIssues(all, tab)
    expect(issues).toHaveLength(3)
    expect(scope.projectWide).toBe(true)
  })

  it('returns the whole project for a table-detail tab', () => {
    const tab: WorkspaceTabRef = { kind: 'table-detail', fqn: 'main.bronze.customers' }
    expect(scopeIssues(all, tab).scope.projectWide).toBe(true)
  })

  it('narrows an entity tab to its pipeline + flowgroup', () => {
    const tab: WorkspaceTabRef = {
      kind: 'entity',
      pipeline: 'bronze',
      flowgroup: 'orders',
      filePath: 'pipelines/bronze/orders.yaml',
      docKind: 'flowgroup',
      view: 'graph',
    }
    const { issues, scope } = scopeIssues(all, tab)
    expect(issues).toEqual([bronzeOrders])
    expect(scope).toEqual({ label: 'orders', projectWide: false })
  })

  it('narrows a legacy designer tab like an entity', () => {
    const tab: WorkspaceTabRef = {
      kind: 'designer',
      id: 'designer:bronze/customers',
      pipeline: 'bronze',
      flowgroup: 'customers',
      filePath: 'pipelines/bronze/customers.yaml',
    }
    const { issues, scope } = scopeIssues(all, tab)
    expect(issues).toEqual([bronzeCustomers])
    expect(scope.label).toBe('customers')
  })

  it('narrows a file tab to issues on that file (matching a leading slash)', () => {
    const tab: WorkspaceTabRef = { kind: 'file', path: 'pipelines/bronze/orders.yaml' }
    const withSlash = issue({ code: 'X', file_path: '/pipelines/bronze/orders.yaml' })
    const { issues, scope } = scopeIssues([...all, withSlash], tab)
    expect(issues).toEqual([bronzeOrders, withSlash])
    expect(scope).toEqual({ label: 'orders.yaml', projectWide: false })
  })

  it('falls back to the whole project when a file tab has no file-scoped issues', () => {
    const tab: WorkspaceTabRef = { kind: 'file', path: 'substitutions/dev.yaml' }
    const { issues, scope } = scopeIssues(all, tab)
    expect(issues).toHaveLength(3)
    expect(scope.projectWide).toBe(true)
  })

  it('scopes a config tab by its file path', () => {
    const cfgIssue = issue({ code: 'CFG-1', file_path: 'lhp.yaml' })
    const tab: WorkspaceTabRef = { kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'form' }
    const { issues, scope } = scopeIssues([...all, cfgIssue], tab)
    expect(issues).toEqual([cfgIssue])
    expect(scope.label).toBe('lhp.yaml')
  })

  it('scopes a resource tab by its file path', () => {
    const resIssue = issue({ code: 'PRE-1', file_path: 'presets/bronze.yaml' })
    const tab: WorkspaceTabRef = {
      kind: 'resource',
      resourceKind: 'preset',
      name: 'bronze',
      filePath: 'presets/bronze.yaml',
    }
    expect(scopeIssues([...all, resIssue], tab).issues).toEqual([resIssue])
  })
})
