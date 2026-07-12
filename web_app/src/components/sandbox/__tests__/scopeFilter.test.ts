import { describe, expect, it } from 'vitest'
import {
  buildSourceFileToPipeline,
  filterFileTreeForScope,
  filterFlowgroupsForScope,
  filterGraphForScope,
  resolveScope,
} from '../scopeFilter'
import type {
  FileNode,
  FlowgroupSummary,
  GraphEdge,
  GraphNode,
  SandboxScope,
} from '../../../types/api'

function graphNode(partial: Partial<GraphNode> & { id: string }): GraphNode {
  return {
    id: partial.id,
    label: partial.label ?? partial.id,
    type: partial.type ?? 'pipeline',
    pipeline: partial.pipeline ?? partial.id,
    flowgroup: partial.flowgroup ?? '',
    stage: partial.stage ?? 0,
    metadata: partial.metadata ?? {},
  }
}

function fg(partial: Partial<FlowgroupSummary> & { name: string; pipeline: string }): FlowgroupSummary {
  return {
    name: partial.name,
    pipeline: partial.pipeline,
    action_count: partial.action_count ?? 1,
    action_types: partial.action_types ?? ['load'],
    presets: partial.presets ?? [],
    source_file: partial.source_file ?? '',
    template: partial.template ?? null,
  }
}

function scopeView(partial: Partial<SandboxScope>): SandboxScope {
  return { profile_exists: true, ...partial }
}

describe('resolveScope', () => {
  it('is null when sandbox is off (identity everywhere)', () => {
    expect(resolveScope(false, scopeView({ resolved_pipelines: ['a'] }))).toBeNull()
  })

  it('is null with no data, no profile, or an errored scope', () => {
    expect(resolveScope(true, undefined)).toBeNull()
    expect(resolveScope(true, scopeView({ profile_exists: false }))).toBeNull()
    expect(
      resolveScope(true, scopeView({ resolved_pipelines: [], error: 'LHP-VAL-064: no match' })),
    ).toBeNull()
  })

  it('is null when the resolved scope is empty (would blank the project)', () => {
    expect(resolveScope(true, scopeView({ resolved_pipelines: [] }))).toBeNull()
  })

  it('is the resolved set when on, present, error-free, and non-empty', () => {
    const scope = resolveScope(true, scopeView({ resolved_pipelines: ['a', 'b'] }))
    expect(scope).not.toBeNull()
    expect([...scope!].sort()).toEqual(['a', 'b'])
  })
})

describe('filterGraphForScope', () => {
  const nodes: GraphNode[] = [
    graphNode({ id: 'bronze', type: 'pipeline', pipeline: 'bronze' }),
    graphNode({ id: 'silver', type: 'pipeline', pipeline: 'silver' }),
    graphNode({ id: 'gold', type: 'pipeline', pipeline: 'gold' }),
    graphNode({ id: 's3://raw', type: 'external', pipeline: '' }),
  ]
  const edges: GraphEdge[] = [
    { source: 'bronze', target: 'silver', type: 'pipeline' }, // in→in
    { source: 'silver', target: 'gold', type: 'pipeline' }, // in→out
    { source: 's3://raw', target: 'bronze', type: 'external' }, // ext→in
    { source: 's3://raw', target: 'gold', type: 'external' }, // ext→out
  ]

  it('is identity when scope is null', () => {
    const out = filterGraphForScope(nodes, edges, null)
    expect(out.nodes).toEqual(nodes)
    expect(out.edges).toEqual(edges)
  })

  it('keeps in-scope pipelines and all externals, drops out-of-scope pipelines', () => {
    const out = filterGraphForScope(nodes, edges, new Set(['bronze', 'silver']))
    expect(out.nodes.map((n) => n.id).sort()).toEqual(['bronze', 's3://raw', 'silver'])
    expect(out.nodes.some((n) => n.id === 'gold')).toBe(false)
  })

  it('drops edges touching a dropped node and leaves no dangling references', () => {
    const out = filterGraphForScope(nodes, edges, new Set(['bronze', 'silver']))
    const keptIds = new Set(out.nodes.map((n) => n.id))
    for (const e of out.edges) {
      expect(keptIds.has(e.source)).toBe(true)
      expect(keptIds.has(e.target)).toBe(true)
    }
    // bronze→silver and s3://raw→bronze survive; the two edges into gold do not.
    expect(out.edges).toHaveLength(2)
  })
})

describe('filterFlowgroupsForScope', () => {
  const flowgroups = [
    fg({ name: 'a1', pipeline: 'bronze' }),
    fg({ name: 'b1', pipeline: 'silver' }),
    fg({ name: 'g1', pipeline: 'gold' }),
  ]

  it('is identity when scope is null', () => {
    expect(filterFlowgroupsForScope(flowgroups, null)).toEqual(flowgroups)
  })

  it('keeps only in-scope pipelines', () => {
    const out = filterFlowgroupsForScope(flowgroups, new Set(['bronze', 'gold']))
    expect(out.map((f) => f.name)).toEqual(['a1', 'g1'])
  })
})

describe('buildSourceFileToPipeline', () => {
  it('maps source_file → pipeline and skips blank sources', () => {
    const map = buildSourceFileToPipeline([
      fg({ name: 'a1', pipeline: 'bronze', source_file: 'pipelines/bronze/a1.yaml' }),
      fg({ name: 'noPath', pipeline: 'silver', source_file: '' }),
    ])
    expect(map.get('pipelines/bronze/a1.yaml')).toBe('bronze')
    expect(map.size).toBe(1)
  })
})

describe('filterFileTreeForScope', () => {
  const tree: FileNode = {
    name: '',
    path: '',
    type: 'directory',
    children: [
      { name: 'lhp.yaml', path: 'lhp.yaml', type: 'file' },
      {
        name: 'substitutions',
        path: 'substitutions',
        type: 'directory',
        children: [{ name: 'dev.yaml', path: 'substitutions/dev.yaml', type: 'file' }],
      },
      {
        name: 'pipelines',
        path: 'pipelines',
        type: 'directory',
        children: [
          {
            name: 'bronze',
            path: 'pipelines/bronze',
            type: 'directory',
            children: [
              { name: 'a1.yaml', path: 'pipelines/bronze/a1.yaml', type: 'file' },
              { name: 'README.md', path: 'pipelines/bronze/README.md', type: 'file' },
            ],
          },
          {
            name: 'gold',
            path: 'pipelines/gold',
            type: 'directory',
            children: [{ name: 'g1.yaml', path: 'pipelines/gold/g1.yaml', type: 'file' }],
          },
        ],
      },
    ],
  }
  const sourceMap = new Map([
    ['pipelines/bronze/a1.yaml', 'bronze'],
    ['pipelines/gold/g1.yaml', 'gold'],
  ])

  it('is identity when scope is null', () => {
    expect(filterFileTreeForScope(tree, sourceMap, null)).toBe(tree)
  })

  function paths(node: FileNode): string[] {
    const here = node.path ? [node.path] : []
    return [...here, ...(node.children ?? []).flatMap(paths)]
  }

  it('hides out-of-scope pipeline files; keeps shared dirs and unclassifiable files', () => {
    const out = filterFileTreeForScope(tree, sourceMap, new Set(['bronze']))
    const kept = paths(out)
    // Shared config always visible.
    expect(kept).toContain('lhp.yaml')
    expect(kept).toContain('substitutions/dev.yaml')
    // In-scope pipeline file visible; its unmapped sibling (README) kept.
    expect(kept).toContain('pipelines/bronze/a1.yaml')
    expect(kept).toContain('pipelines/bronze/README.md')
    // Out-of-scope pipeline file and its now-empty directory pruned.
    expect(kept).not.toContain('pipelines/gold/g1.yaml')
    expect(kept).not.toContain('pipelines/gold')
    // The pipelines/ dir survives because bronze remains.
    expect(kept).toContain('pipelines')
  })

  it('does not mutate the input tree', () => {
    const before = JSON.stringify(tree)
    filterFileTreeForScope(tree, sourceMap, new Set(['bronze']))
    expect(JSON.stringify(tree)).toBe(before)
  })
})
