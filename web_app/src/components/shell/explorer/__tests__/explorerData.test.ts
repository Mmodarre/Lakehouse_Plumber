import { describe, expect, it } from 'vitest'
import type { FileNode, TableSummary } from '../../../../types/api'
import {
  clampExplorerWidth,
  configFilesByKind,
  flattenFilePaths,
  groupTables,
  isSinkFqn,
  orderedKinds,
  resolveResourceFilePath,
  tableGroupLabel,
  tableLeafName,
} from '../explorerData'

describe('clampExplorerWidth', () => {
  it('clamps below/above the 200–420 range and rounds', () => {
    expect(clampExplorerWidth(50)).toBe(200)
    expect(clampExplorerWidth(999)).toBe(420)
    expect(clampExplorerWidth(300.6)).toBe(301)
  })
})

describe('orderedKinds', () => {
  it('emits canonical order regardless of input order, dedupes', () => {
    expect(orderedKinds(['write', 'load', 'load', 'test'])).toEqual(['load', 'write', 'test'])
  })
  it('appends unknown kinds sorted after the known ones', () => {
    expect(orderedKinds(['zeta', 'transform', 'alpha'])).toEqual(['transform', 'alpha', 'zeta'])
  })
})

const tree: FileNode = {
  name: '',
  path: '',
  type: 'directory',
  children: [
    {
      name: 'config',
      path: 'config',
      type: 'directory',
      children: [
        { name: 'pipeline_config_dev.yaml', path: 'config/pipeline_config_dev.yaml', type: 'file' },
        { name: 'job_config_dev.yaml', path: 'config/job_config_dev.yaml', type: 'file' },
        { name: 'monitoring_job_config.yaml', path: 'config/monitoring_job_config.yaml', type: 'file' },
        { name: 'shared.yaml', path: 'config/shared.yaml', type: 'file' },
        { name: 'notes.txt', path: 'config/notes.txt', type: 'file' },
      ],
    },
    {
      name: 'presets',
      path: 'presets',
      type: 'directory',
      children: [{ name: 'bronze_layer.yml', path: 'presets/bronze_layer.yml', type: 'file' }],
    },
  ],
}

describe('flattenFilePaths', () => {
  it('returns every file path depth-first and nothing for undefined', () => {
    expect(flattenFilePaths(undefined)).toEqual([])
    expect(flattenFilePaths(tree)).toContain('config/pipeline_config_dev.yaml')
    expect(flattenFilePaths(tree)).toContain('presets/bronze_layer.yml')
    expect(flattenFilePaths(tree)).not.toContain('config') // directories excluded
  })
})

describe('configFilesByKind', () => {
  it('partitions config/*.yaml by preferred kind; ignores non-yaml + non-config', () => {
    const groups = configFilesByKind(flattenFilePaths(tree))
    expect(groups.pipeline).toEqual(['config/pipeline_config_dev.yaml'])
    expect(groups.job).toEqual([
      'config/job_config_dev.yaml',
      'config/monitoring_job_config.yaml',
    ])
    expect(groups.other).toEqual(['config/shared.yaml'])
  })
})

describe('resolveResourceFilePath', () => {
  it('matches a tree file by stem regardless of extension', () => {
    const paths = flattenFilePaths(tree)
    expect(resolveResourceFilePath(paths, 'presets', 'bronze_layer')).toBe('presets/bronze_layer.yml')
  })
  it('falls back to <dir>/<name>.yaml when no file matches', () => {
    expect(resolveResourceFilePath([], 'templates', 'ingest')).toBe('templates/ingest.yaml')
  })
})

function table(full_name: string, flowgroup: string, target_type = 'streaming_table'): TableSummary {
  return { full_name, flowgroup, pipeline: 'p', target_type, source_file: 's', write_mode: null, scd_type: null }
}

describe('table FQN helpers', () => {
  it('detects sinks and derives group/leaf labels', () => {
    expect(isSinkFqn('sink:kafka/out')).toBe(true)
    expect(isSinkFqn('main.bronze.customers')).toBe(false)
    expect(tableGroupLabel('main.bronze.customers')).toBe('main.bronze')
    expect(tableLeafName('main.bronze.customers')).toBe('customers')
    expect(tableGroupLabel('sink:kafka/out')).toBe('sinks')
    expect(tableLeafName('sink:kafka/out')).toBe('kafka/out')
  })
})

describe('groupTables', () => {
  const tables = [
    table('main.bronze.customers', 'bronze_customers'),
    table('main.silver.orders', 'silver_orders'),
    table('main.bronze.orders', 'bronze_orders'),
    table('sink:kafka/out', 'bronze_events', 'sink'),
  ]

  it('groups by schema prefix (sinks last), rows alphabetical', () => {
    const groups = groupTables(tables, '')
    expect(groups.map((g) => g.label)).toEqual(['main.bronze', 'main.silver', 'sinks'])
    expect(groups[0].tables.map((t) => tableLeafName(t.full_name))).toEqual(['customers', 'orders'])
  })

  it('filters by name/flowgroup/pipeline substring', () => {
    const groups = groupTables(tables, 'kafka')
    expect(groups).toHaveLength(1)
    expect(groups[0].label).toBe('sinks')
  })
})
