import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionSubTypeSpec, FieldGroup, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { computeIssues, pathKey } from '../../formModel'
import { testRowCountSpec } from '../test-row-count'
import { testUniquenessSpec } from '../test-uniqueness'
import { testReferentialIntegritySpec } from '../test-referential-integrity'
import { testCompletenessSpec } from '../test-completeness'
import { testRangeSpec } from '../test-range'
import { testSchemaMatchSpec } from '../test-schema-match'
import { testAllLookupsFoundSpec } from '../test-all-lookups-found'
import { testCustomSqlSpec } from '../test-custom-sql'
import { testCustomExpectationsSpec } from '../test-custom-expectations'

// ── spec-level helpers (mirror the write/transform redesign tests) ─

const key = (path: YamlPath) => JSON.stringify(path)

function allFields(s: ActionSubTypeSpec): FieldSpec[] {
  return s.groups.flatMap((g) => g.fields)
}

function findField(s: ActionSubTypeSpec, path: YamlPath): FieldSpec | undefined {
  return allFields(s).find((f) => key(f.path) === key(path))
}

function groupContaining(s: ActionSubTypeSpec, path: YamlPath): FieldGroup | undefined {
  return s.groups.find((g) => g.fields.some((f) => key(f.path) === key(path)))
}

const VIOLATION: readonly string[] = ['fail', 'warn', 'drop']

const ALL_SPECS: [string, ActionSubTypeSpec][] = [
  ['row_count', testRowCountSpec],
  ['uniqueness', testUniquenessSpec],
  ['referential_integrity', testReferentialIntegritySpec],
  ['completeness', testCompletenessSpec],
  ['range', testRangeSpec],
  ['schema_match', testSchemaMatchSpec],
  ['all_lookups_found', testAllLookupsFoundSpec],
  ['custom_sql', testCustomSqlSpec],
  ['custom_expectations', testCustomExpectationsSpec],
]

// The 7 sub-types that carry an ACTION-level on_violation (custom_sql /
// custom_expectations set it per-expectation instead — see below).
const ACTION_LEVEL_ON_VIOLATION: [string, ActionSubTypeSpec][] = ALL_SPECS.filter(
  ([name]) => name !== 'custom_sql' && name !== 'custom_expectations',
)

// ── test_id + target present in an Advanced group on ALL 9 ───

describe('test specs — test_id + target in an Advanced group (all 9)', () => {
  for (const [name, spec] of ALL_SPECS) {
    it(`${name} carries test_id + target as text in an advanced group`, () => {
      for (const p of [['test_id'], ['target']] as YamlPath[]) {
        const f = findField(spec, p)
        expect(f, `${name} ${key(p)} missing`).toBeDefined()
        expect(f!.widget).toBe('text')
        expect(groupContaining(spec, p)!.advanced, `${name} ${key(p)} not advanced`).toBe(true)
      }
      // target's placeholder surfaces the generator default (tmp_test_<name>).
      expect(findField(spec, ['target'])!.placeholder).toBe('tmp_test_<name>')
    })
  }
})

// ── on_violation — [fail,warn,drop] select default fail, everywhere ─

describe('test specs — on_violation is a [fail,warn,drop] select (default fail)', () => {
  for (const [name, spec] of ACTION_LEVEL_ON_VIOLATION) {
    it(`${name} action-level on_violation`, () => {
      const f = findField(spec, ['on_violation'])!
      expect(f.widget).toBe('enum')
      expect(f.options).toEqual(VIOLATION)
      expect(f.enumDefault).toBe('fail')
      expect(f.display).not.toBe('segmented')
    })
  }

  it('custom_sql / custom_expectations have NO action-level on_violation', () => {
    expect(findField(testCustomSqlSpec, ['on_violation'])).toBeUndefined()
    expect(findField(testCustomExpectationsSpec, ['on_violation'])).toBeUndefined()
  })
})

// ── row_count — source is a fixed two-table dualSource ───────

describe('test/row_count — source is a dualSource (not a list)', () => {
  it('source is the dualSource widget', () => {
    expect(findField(testRowCountSpec, ['source'])!.widget).toBe('dualSource')
  })

  it('tolerance is a number field with min 0', () => {
    const t = findField(testRowCountSpec, ['tolerance'])!
    expect(t.widget).toBe('number')
    expect(t.min).toBe(0)
  })

  it('keeps the "exactly two" soft rule on source', () => {
    const three = computeIssues(testRowCountSpec, { source: ['a', 'b', 'c'] })
    expect(three.get(pathKey(['source']))).toMatch(/exactly two/i)
    const two = computeIssues(testRowCountSpec, { source: ['a', 'b'] })
    expect(two.has(pathKey(['source']))).toBe(false)
  })
})

// ── custom_sql / custom_expectations — objectList expectations ─

describe('test/custom_* — expectations is an objectList of {name, expression, on_violation}', () => {
  for (const [name, spec] of [
    ['custom_sql', testCustomSqlSpec],
    ['custom_expectations', testCustomExpectationsSpec],
  ] as [string, ActionSubTypeSpec][]) {
    it(`${name} expectations row shape`, () => {
      const f = findField(spec, ['expectations'])!
      expect(f.widget).toBe('objectList')
      const items = f.itemFields ?? []
      expect(items.map((i) => key(i.path))).toEqual([
        key(['name']),
        key(['expression']),
        key(['on_violation']),
      ])
      const byPath = (p: YamlPath) => items.find((i) => key(i.path) === key(p))!
      expect(byPath(['name']).required).toBe(true)
      expect(byPath(['expression']).required).toBe(true)
      const ov = byPath(['on_violation'])
      expect(ov.widget).toBe('enum')
      expect(ov.options).toEqual(VIOLATION)
      expect(ov.enumDefault).toBe('fail')
    })
  }

  it('custom_expectations.expectations is required; custom_sql.sql is required', () => {
    expect(findField(testCustomExpectationsSpec, ['expectations'])!.required).toBe(true)
    expect(findField(testCustomSqlSpec, ['sql'])!.required).toBe(true)
  })

  it('both surface a per-expectation violation note on the expectations group', () => {
    for (const spec of [testCustomSqlSpec, testCustomExpectationsSpec]) {
      expect(groupContaining(spec, ['expectations'])!.description).toMatch(/each expectation/i)
    }
  })
})

// ── range — min/max stay text (Any: dates/tokens round-trip) ──

describe('test/range — min/max are text', () => {
  it('min_value + max_value are text widgets; requiredOneOf kept', () => {
    expect(findField(testRangeSpec, ['min_value'])!.widget).toBe('text')
    expect(findField(testRangeSpec, ['max_value'])!.widget).toBe('text')
    const none = computeIssues(testRangeSpec, { source: 's', column: 'x' })
    expect(none.get(pathKey(['min_value']))).toMatch(/at least one/i)
  })
})

// ── uniqueness — filter moved into Advanced ──────────────────

describe('test/uniqueness — filter in Advanced', () => {
  it('filter lives in an advanced group', () => {
    expect(groupContaining(testUniquenessSpec, ['filter'])!.advanced).toBe(true)
  })
})

// ── equal-length column-pair rules preserved on both editors ─

describe('test specs — equal-length column-pair rules preserved', () => {
  it('referential_integrity: source_columns / reference_columns', () => {
    const unequal = computeIssues(testReferentialIntegritySpec, {
      source: 's',
      reference: 'c.s.t',
      source_columns: ['a', 'b'],
      reference_columns: ['a'],
    })
    expect(unequal.get(pathKey(['source_columns']))).toMatch(/same length/i)
  })

  it('all_lookups_found: lookup_columns / lookup_result_columns', () => {
    const unequal = computeIssues(testAllLookupsFoundSpec, {
      source: 's',
      lookup_table: 'c.s.t',
      lookup_columns: ['a'],
      lookup_result_columns: ['a', 'b'],
    })
    expect(unequal.get(pathKey(['lookup_columns']))).toMatch(/same length/i)
  })
})

// ── section ordering (Core/Target → params → violation → advanced) ─

describe('test specs — Advanced is the last group', () => {
  for (const [name, spec] of ALL_SPECS) {
    it(`${name} ends with the advanced group`, () => {
      const last = spec.groups[spec.groups.length - 1]
      expect(last.advanced, `${name} last group is not advanced`).toBe(true)
    })
  }
})

// ── render integration through the modal shell ───────────────

const { commitSpy } = vi.hoisted(() => ({
  commitSpy: vi.fn<(fn: (doc: unknown) => void) => boolean>(),
}))

vi.mock('@/components/entity/useFlowgroupDoc', () => ({
  useFlowgroupDoc: () => ({ commit: commitSpy, readOnly: false }),
}))
vi.mock('@/workspace/persistBuffer', () => ({
  persistBufferToDisk: vi.fn(() => Promise.resolve(true)),
}))
vi.mock('@/store/runStore', async (importActual) => {
  const actual = await importActual<typeof import('@/store/runStore')>()
  return {
    ...actual,
    useRunController: () => ({
      isRunning: false,
      startValidate: vi.fn(),
      startGenerate: vi.fn(),
      abort: vi.fn(),
    }),
  }
})
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironmentResolved: vi.fn(() => ({ data: { tokens: {} } })),
}))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))
vi.mock('@/hooks/useOperationalMetadata', () => ({
  useOperationalMetadata: () => ({ data: { columns: [] }, isLoading: false, error: null }),
}))
vi.mock('../../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../useCompanionFile')>()
  return { ...actual, useCompanionFile: () => ({ status: 'unavailable' as const, create: vi.fn() }) }
})

import type { ActionRead } from '@/lib/flowgroup-doc'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { ActionModalEditor } from '../../ActionModalEditor'

function wrapper({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
}

function open(path: string, yaml: string, action: ActionRead) {
  useWorkspaceStore.getState().openBuffer(path, { content: yaml, exists: true })
  return render(
    <ActionModalEditor filePath={path} docKind="flowgroup" action={action} actionId={action.name} />,
    { wrapper },
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  commitSpy.mockReturnValue(true)
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

const TEST_ACTION = (subType: string, name: string, raw: Record<string, unknown>): ActionRead => ({
  name,
  kind: 'test',
  subType,
  target: '',
  sources: [],
  dependsOn: [],
  raw: { name, type: 'test', test_type: subType, ...raw },
  index: 0,
})

const ROW_COUNT_YAML = `pipeline: perf
flowgroup: fg_rc
actions:
  - name: ta_row_count
    type: test
    test_type: row_count
    source:
      - "\${catalog}.\${raw_schema}.customers"
      - "\${catalog}.\${bronze_schema}.customers"
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
      - name: positive_amount
        expression: "total_price > 0"
        on_violation: warn
`

describe('test/row_count — rendered through ActionModalEditor', () => {
  it('renders the fixed two-table dualSource (Table A / Table B, no add/remove)', () => {
    open('pipelines/perf/fg_rc.yaml', ROW_COUNT_YAML, TEST_ACTION('row_count', 'ta_row_count', {}))
    expect(screen.getByLabelText('Table A')).toBeInTheDocument()
    expect(screen.getByLabelText('Table B')).toBeInTheDocument()
    // The dualSource has fixed arity 2 — no list add affordance anywhere.
    expect(screen.queryByRole('button', { name: /add/i })).toBeNull()
  })

  it('renders on_violation as a select (combobox), not a segmented control', () => {
    open('pipelines/perf/fg_rc.yaml', ROW_COUNT_YAML, TEST_ACTION('row_count', 'ta_row_count', {}))
    expect(screen.getByRole('combobox', { name: 'On violation' })).toBeInTheDocument()
    expect(screen.queryByRole('radio', { name: 'fail' })).toBeNull()
  })
})

describe('test/range — rendered through ActionModalEditor', () => {
  it('renders min/max as text inputs (Any value — not a number spinbutton)', () => {
    open('pipelines/perf/fg_range.yaml', RANGE_YAML, TEST_ACTION('range', 'ta_range', {}))
    // Text fields opt into `${env}`-token autocomplete in the modal, so the
    // underlying control is token-aware — but it must NOT be a number spinbutton
    // (min/max are `Any`, so dates/tokens have to round-trip).
    const min = screen.getByLabelText('Minimum')
    const max = screen.getByLabelText('Maximum')
    expect(min).toBeInTheDocument()
    expect(max).toBeInTheDocument()
    expect(min).not.toHaveAttribute('type', 'number')
    expect(screen.queryByRole('spinbutton', { name: 'Minimum' })).toBeNull()
    expect(screen.queryByRole('spinbutton', { name: 'Maximum' })).toBeNull()
  })
})

describe('test/custom_expectations — rendered through ActionModalEditor', () => {
  it('renders the expectations objectList with name/expression/on_violation rows', () => {
    open(
      'pipelines/perf/fg_ce.yaml',
      CUSTOM_EXP_YAML,
      TEST_ACTION('custom_expectations', 'ta_business_rules', {}),
    )
    expect(screen.getByLabelText('Name')).toBeInTheDocument()
    expect(screen.getByLabelText('Expression')).toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: 'On violation' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /add expectations entry/i })).toBeInTheDocument()
  })
})
