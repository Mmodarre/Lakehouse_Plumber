/**
 * Validator suite for the lhp.yaml (project config) surface.
 *
 * Ground truth: `src/lhp/core/loaders/project_config_loader.py`, the
 * per-section parsers `src/lhp/core/loaders/_*_config_parser.py`, and the
 * Pydantic models in `src/lhp/models/`. Errors are emitted only where the
 * Python side hard-fails; loader-permissive spots stay permissive.
 */
import { describe, expect, it } from 'vitest'

import {
  listProjectPassthroughKeys,
  listProjectSections,
  PROJECT_KNOWN_KEYS,
  validateProjectConfig,
} from '../index'
import type { ValidationIssue } from '../index'

function errorsOf(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((i) => i.severity === 'error')
}

function warningsOf(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((i) => i.severity === 'warning')
}

/** Minimal valid project doc; sections merged on top. */
function project(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { name: 'demo', ...extra }
}

/** Valid event_log + monitoring base used by the monitoring matrices. */
const VALID_EVENT_LOG = { catalog: 'cat', schema: '_meta' }
const VALID_MONITORING = {
  checkpoint_path: '/Volumes/cat/_meta/checkpoints',
  job_config_path: 'config/monitoring_job_config.yaml',
}

describe('top-level shape', () => {
  it('minimal doc with a name is clean', () => {
    expect(validateProjectConfig(project())).toEqual([])
  })

  it('missing name → WARNING only (loader defaults to "unnamed_project", project_config_loader.py:151)', () => {
    const issues = validateProjectConfig({ version: '1.0' })
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
    expect(warningsOf(issues)[0].path).toEqual(['name'])
  })

  // Top-level Pydantic type violations are WARNINGS, not errors: runtime-
  // verified that load_project_config SWALLOWS the ValidationError (the
  // `except ValueError` at project_config_loader.py:72-87 falls through
  // without re-raising) and returns None — the CLI runs on, silently
  // treating the project as having no lhp.yaml. Blocking Save would be
  // stricter than the loader.
  it('non-string name → warning (loader silently discards the whole config)', () => {
    for (const doc of [{ name: 42 }, { name: null }]) {
      const issues = validateProjectConfig(doc)
      expect(errorsOf(issues)).toEqual([])
      expect(warningsOf(issues)).toHaveLength(1)
    }
  })

  it('non-string version (unquoted YAML 1.0 parses as a float) → warning', () => {
    const issues = validateProjectConfig(project({ version: 1.0 }))
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
    expect(validateProjectConfig(project({ version: '1.0' }))).toEqual([])
  })

  it('non-string optional scalar fields → warning', () => {
    expect(warningsOf(validateProjectConfig(project({ description: 42 })))).toHaveLength(1)
    expect(errorsOf(validateProjectConfig(project({ description: 42 })))).toEqual([])
    expect(warningsOf(validateProjectConfig(project({ required_lhp_version: 0.4 })))).toHaveLength(1)
  })

  it('non-mapping document → error; empty document → warning', () => {
    expect(errorsOf(validateProjectConfig(['x']))).toHaveLength(1)
    expect(warningsOf(validateProjectConfig(null))).toHaveLength(1)
  })
})

describe('include patterns (loaders/_include_patterns_parser.py + utils/file_pattern_matcher.py:40-58)', () => {
  it('valid patterns are clean, for all three include keys', () => {
    expect(
      validateProjectConfig(
        project({
          include: ['*.yaml', 'bronze_*.yaml', 'dir/**/*.yaml'],
          blueprint_include: ['bp/*.yaml'],
          instance_include: ['inst/*.yaml'],
        }),
      ),
    ).toEqual([])
  })

  it('non-list include → CFG_003 error', () => {
    const issues = errorsOf(validateProjectConfig(project({ include: '*.yaml' })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_003')
  })

  it('non-string pattern → CFG_004 error', () => {
    const issues = errorsOf(validateProjectConfig(project({ include: [42] })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_004')
  })

  it('invalid glob (empty, ***/, [unclosed) → CFG_005 error', () => {
    for (const pattern of ['', '***/x.yaml', '[unclosed']) {
      const issues = errorsOf(validateProjectConfig(project({ include: [pattern] })))
      expect(issues).toHaveLength(1)
      expect(issues[0].code).toBe('CFG_005')
    }
  })
})

describe('operational_metadata (loaders/_operational_metadata_config_parser.py)', () => {
  it('accepts the bare-string column shorthand (:24-26) and the bare-list preset shorthand (:62-64)', () => {
    expect(
      validateProjectConfig(
        project({
          operational_metadata: {
            columns: { _ts: 'F.current_timestamp()' },
            presets: { minimal: ['_ts'] },
          },
        }),
      ),
    ).toEqual([])
  })

  it('accepts full column/preset objects', () => {
    expect(
      validateProjectConfig(
        project({
          operational_metadata: {
            columns: {
              _ts: {
                expression: 'F.current_timestamp()',
                description: 'd',
                applies_to: ['view'],
                additional_imports: ['from x import y'],
                enabled: true,
              },
            },
            presets: { p: { columns: ['_ts'], description: 'd' } },
            defaults: { preset: 'p' },
          },
        }),
      ),
    ).toEqual([])
  })

  it('non-dict/non-string column config → CFG_003 error (:27-30)', () => {
    const issues = errorsOf(
      validateProjectConfig(project({ operational_metadata: { columns: { c: 42 } } })),
    )
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_003')
  })

  it('non-dict/non-list preset config → CFG_004 error (:65-68)', () => {
    const issues = errorsOf(
      validateProjectConfig(project({ operational_metadata: { presets: { p: 'oops' } } })),
    )
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_004')
  })

  it('preset referencing an undefined column → CFG_005 error (:104-125)', () => {
    const issues = errorsOf(
      validateProjectConfig(
        project({
          operational_metadata: { columns: { a: 'F.lit(1)' }, presets: { p: ['a', 'ghost'] } },
        }),
      ),
    )
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_005')
    expect(issues[0].path).toEqual(['operational_metadata', 'presets', 'p'])
  })

  it('non-mapping operational_metadata → error', () => {
    expect(errorsOf(validateProjectConfig(project({ operational_metadata: 'x' })))).toHaveLength(1)
  })
})

describe('event_log (loaders/_event_log_config_parser.py)', () => {
  it('catalog + schema present → clean', () => {
    expect(validateProjectConfig(project({ event_log: VALID_EVENT_LOG }))).toEqual([])
  })

  it('enabled (default true) with missing catalog+schema → ONE CFG_007 error listing both (:48-71)', () => {
    const issues = errorsOf(validateProjectConfig(project({ event_log: {} })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_007')
    expect(issues[0].message).toContain('catalog')
    expect(issues[0].message).toContain('schema')
  })

  it('enabled with only catalog → CFG_007 error naming schema', () => {
    const issues = errorsOf(validateProjectConfig(project({ event_log: { catalog: 'c' } })))
    expect(issues).toHaveLength(1)
    expect(issues[0].message).toContain('schema')
    expect(issues[0].message).not.toContain('catalog,')
  })

  it('enabled: false → no required-fields error (:50-51)', () => {
    expect(validateProjectConfig(project({ event_log: { enabled: false } }))).toEqual([])
  })

  it('bare event_log (null) → CFG_006 error — unlike monitoring, null is NOT treated as {} (:14-23)', () => {
    const issues = errorsOf(validateProjectConfig(project({ event_log: null })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_006')
  })

  it('non-string catalog → CFG_006 type error, and the required-fields check is skipped', () => {
    const issues = errorsOf(
      validateProjectConfig(project({ event_log: { catalog: 42, schema: 's' } })),
    )
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_006')
  })
})

describe('monitoring (loaders/_monitoring_config_parser.py)', () => {
  it('enabled with event_log + checkpoint_path + job_config_path → clean', () => {
    expect(
      validateProjectConfig(
        project({ event_log: VALID_EVENT_LOG, monitoring: VALID_MONITORING }),
      ),
    ).toEqual([])
  })

  it('enabled without an event_log section → CFG_008 error (:142-155)', () => {
    const issues = errorsOf(validateProjectConfig(project({ monitoring: VALID_MONITORING })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_008')
    expect(issues[0].message).toContain('event_log')
  })

  it('enabled with a DISABLED event_log → CFG_008 error', () => {
    const issues = errorsOf(
      validateProjectConfig(
        project({ event_log: { enabled: false }, monitoring: VALID_MONITORING }),
      ),
    )
    expect(issues).toHaveLength(1)
    expect(issues[0].message).toContain('event_log')
  })

  it('enabled without checkpoint_path/job_config_path → one error each (:157-187)', () => {
    const issues = errorsOf(
      validateProjectConfig(project({ event_log: VALID_EVENT_LOG, monitoring: {} })),
    )
    expect(issues).toHaveLength(2)
    expect(issues.map((i) => i.code)).toEqual(['CFG_008', 'CFG_008'])
  })

  it('bare monitoring (null) is treated as {} — enabled with all defaults (:26-27)', () => {
    const issues = errorsOf(
      validateProjectConfig(project({ event_log: VALID_EVENT_LOG, monitoring: null })),
    )
    expect(issues).toHaveLength(2) // checkpoint_path + job_config_path
  })

  it('enabled: false → required-fields checks skipped entirely (:139-140)', () => {
    expect(validateProjectConfig(project({ monitoring: { enabled: false } }))).toEqual([])
  })

  it.each([
    [0, 1],
    [1, 0],
    [20, 0],
    [21, 1],
  ])(
    'max_concurrent_streams boundary %i → %i error(s) (range 1..20, :74-101)',
    (value, errorCount) => {
      const issues = errorsOf(
        validateProjectConfig(
          project({
            event_log: VALID_EVENT_LOG,
            monitoring: { ...VALID_MONITORING, max_concurrent_streams: value },
          }),
        ),
      )
      expect(issues).toHaveLength(errorCount)
    },
  )

  it('boolean or non-int max_concurrent_streams → error (bool rejected explicitly, :75)', () => {
    for (const value of [true, '5', 10.5]) {
      const issues = errorsOf(
        validateProjectConfig(
          project({
            event_log: VALID_EVENT_LOG,
            monitoring: { ...VALID_MONITORING, max_concurrent_streams: value },
          }),
        ),
      )
      expect(issues).toHaveLength(1)
    }
  })

  it('the range check runs at parse time EVEN when monitoring is disabled (:74-101 precedes the enabled gate)', () => {
    const issues = errorsOf(
      validateProjectConfig(
        project({ monitoring: { enabled: false, max_concurrent_streams: 0 } }),
      ),
    )
    expect(issues).toHaveLength(1)
  })
})

describe('monitoring materialized_views (loaders/_monitoring_config_parser.py:210-249)', () => {
  function mvProject(mvs: unknown): Record<string, unknown> {
    return project({
      event_log: VALID_EVENT_LOG,
      monitoring: { ...VALID_MONITORING, materialized_views: mvs },
    })
  }

  it('sql alone / sql_path alone are both fine', () => {
    expect(validateProjectConfig(mvProject([{ name: 'a', sql: 'SELECT 1' }]))).toEqual([])
    expect(validateProjectConfig(mvProject([{ name: 'a', sql_path: 'sql/a.sql' }]))).toEqual([])
  })

  it('NEITHER sql nor sql_path is accepted — the loader only rejects BOTH (at-most-one, :238-249)', () => {
    expect(validateProjectConfig(mvProject([{ name: 'a' }]))).toEqual([])
  })

  it('both sql and sql_path → error', () => {
    const issues = errorsOf(
      validateProjectConfig(mvProject([{ name: 'a', sql: 'SELECT 1', sql_path: 'a.sql' }])),
    )
    expect(issues).toHaveLength(1)
    expect(issues[0].path).toEqual(['monitoring', 'materialized_views', 0])
  })

  it('missing/empty name → error (:213-222)', () => {
    expect(errorsOf(validateProjectConfig(mvProject([{ sql: 'SELECT 1' }])))).toHaveLength(1)
    expect(errorsOf(validateProjectConfig(mvProject([{ name: '' }])))).toHaveLength(1)
  })

  it('duplicate names → error (:224-236)', () => {
    const issues = errorsOf(
      validateProjectConfig(mvProject([{ name: 'a', sql: 's' }, { name: 'a' }])),
    )
    expect(issues).toHaveLength(1)
  })

  it('non-list materialized_views and non-mapping entries → parse-time errors (:44-65)', () => {
    expect(errorsOf(validateProjectConfig(mvProject('a view')))).toHaveLength(1)
    expect(errorsOf(validateProjectConfig(mvProject(['not a map'])))).toHaveLength(1)
  })

  it('name/sql XOR checks are SKIPPED when monitoring is disabled, but parse-time shape checks still run (:139-140 vs :44-65)', () => {
    const disabled = project({
      monitoring: {
        enabled: false,
        materialized_views: [{ name: 'a', sql: 's', sql_path: 'p' }],
      },
    })
    expect(validateProjectConfig(disabled)).toEqual([])
    const disabledBadShape = project({
      monitoring: { enabled: false, materialized_views: ['nope'] },
    })
    expect(errorsOf(validateProjectConfig(disabledBadShape))).toHaveLength(1)
  })
})

describe('uc_tagging (loaders/_uc_tagging_config_parser.py)', () => {
  it('bare block (null) and empty mapping are both valid (:22-24)', () => {
    expect(validateProjectConfig(project({ uc_tagging: null }))).toEqual([])
    expect(validateProjectConfig(project({ uc_tagging: {} }))).toEqual([])
  })

  it('non-mapping → CFG_009 error', () => {
    expect(errorsOf(validateProjectConfig(project({ uc_tagging: 'on' })))).toHaveLength(1)
  })

  it('enabled/remove_undeclared_tags must be REAL booleans (:37-49 uses isinstance bool)', () => {
    expect(
      errorsOf(validateProjectConfig(project({ uc_tagging: { enabled: 'yes' } }))),
    ).toHaveLength(1)
    expect(
      errorsOf(validateProjectConfig(project({ uc_tagging: { remove_undeclared_tags: 1 } }))),
    ).toHaveLength(1)
    expect(validateProjectConfig(project({ uc_tagging: { enabled: false } }))).toEqual([])
  })

  it.each([
    [0, 1],
    [1, 0],
    [20, 0],
    [21, 1],
  ])('tag_update_concurrency boundary %i → %i error(s) (range 1..20, :51-71)', (value, count) => {
    const issues = errorsOf(
      validateProjectConfig(project({ uc_tagging: { tag_update_concurrency: value } })),
    )
    expect(issues).toHaveLength(count)
  })

  it('boolean tag_update_concurrency → error (bool rejected explicitly, :53-57)', () => {
    expect(
      errorsOf(validateProjectConfig(project({ uc_tagging: { tag_update_concurrency: true } }))),
    ).toHaveLength(1)
  })
})

describe('test_reporting (loaders/_test_reporting_config_parser.py)', () => {
  it('module_path + function_name → clean', () => {
    expect(
      validateProjectConfig(
        project({ test_reporting: { module_path: 'src/p.py', function_name: 'publish' } }),
      ),
    ).toEqual([])
  })

  it('missing required fields → ONE CFG_009 error listing them (:25-41)', () => {
    const issues = errorsOf(validateProjectConfig(project({ test_reporting: {} })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_009')
    expect(issues[0].message).toContain('module_path')
    expect(issues[0].message).toContain('function_name')
  })

  it('bare test_reporting (null) → error — must be a mapping (:14-23)', () => {
    expect(errorsOf(validateProjectConfig(project({ test_reporting: null })))).toHaveLength(1)
  })
})

describe('wheel (loaders/_wheel_config_parser.py)', () => {
  it('empty mapping and artifact_volume string → clean', () => {
    expect(validateProjectConfig(project({ wheel: {} }))).toEqual([])
    expect(
      validateProjectConfig(project({ wheel: { artifact_volume: '/Volumes/c/s/v' } })),
    ).toEqual([])
  })

  it('bare wheel (null) → CFG_060 error (:18-27)', () => {
    const issues = errorsOf(validateProjectConfig(project({ wheel: null })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_060')
  })

  it('non-string artifact_volume → CFG_060 error (:29-39)', () => {
    expect(
      errorsOf(validateProjectConfig(project({ wheel: { artifact_volume: 42 } }))),
    ).toHaveLength(1)
  })
})

describe('sandbox (loaders/_sandbox_config_parser.py + models/_sandbox.py:34-76)', () => {
  it('empty mapping is valid (defaults: strategy table, pattern {namespace}_{table})', () => {
    expect(validateProjectConfig(project({ sandbox: {} }))).toEqual([])
  })

  it.each(['{namespace}_{table}', '{namespace}__{table}', '{table}{namespace}', '{namespace}_x_{table}'])(
    'valid table_pattern %s',
    (pattern) => {
      expect(validateProjectConfig(project({ sandbox: { table_pattern: pattern } }))).toEqual([])
    },
  )

  it.each([
    ['dev_{table}', 'missing {namespace}'],
    ['{namespace}-{table}', 'literal text outside [A-Za-z0-9_]'],
    ['{namespace}_{table}_{other}', 'unknown placeholder'],
    ['{namespace!r}_{table}', 'conversion not allowed'],
    ['{namespace:>10}_{table}', 'format spec not allowed'],
    ['{namespace', 'unterminated replacement field'],
    ['{{x}}_{namespace}_{table}', 'escaped braces produce literal { }'],
  ])('invalid table_pattern %s → CFG_063 error (%s)', (pattern) => {
    const issues = errorsOf(validateProjectConfig(project({ sandbox: { table_pattern: pattern } })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_063')
    expect(issues[0].path).toEqual(['sandbox', 'table_pattern'])
  })

  it('strategy other than "table" → CFG_062 error (Literal, models/_sandbox.py:29)', () => {
    const issues = errorsOf(validateProjectConfig(project({ sandbox: { strategy: 'schema' } })))
    expect(issues).toHaveLength(1)
    expect(issues[0].code).toBe('CFG_062')
  })

  it('allowed_envs: empty list → CFG_062 error; null/omitted → unrestricted (:70-85)', () => {
    expect(errorsOf(validateProjectConfig(project({ sandbox: { allowed_envs: [] } })))).toHaveLength(1)
    expect(validateProjectConfig(project({ sandbox: { allowed_envs: null } }))).toEqual([])
    expect(validateProjectConfig(project({ sandbox: { allowed_envs: ['dev', 'tst'] } }))).toEqual([])
  })

  it('non-list / non-string-items allowed_envs → CFG_062 error', () => {
    expect(errorsOf(validateProjectConfig(project({ sandbox: { allowed_envs: 'dev' } })))).toHaveLength(1)
    expect(errorsOf(validateProjectConfig(project({ sandbox: { allowed_envs: [1] } })))).toHaveLength(1)
  })

  it('bare sandbox (null) → CFG_062 error (:16-25)', () => {
    expect(errorsOf(validateProjectConfig(project({ sandbox: null })))).toHaveLength(1)
  })
})

describe('sections and passthrough', () => {
  it('PROJECT_KNOWN_KEYS covers every key the loader reads (project_config_loader.py:105-168)', () => {
    for (const key of [
      'name',
      'version',
      'description',
      'author',
      'created_date',
      'include',
      'blueprint_include',
      'instance_include',
      'operational_metadata',
      'event_log',
      'monitoring',
      'required_lhp_version',
      'test_reporting',
      'uc_tagging',
      'wheel',
      'sandbox',
      'apply_formatting',
    ]) {
      expect(PROJECT_KNOWN_KEYS.has(key)).toBe(true)
    }
  })

  it('listProjectSections reports which optional sections are present', () => {
    expect(
      listProjectSections(project({ event_log: VALID_EVENT_LOG, sandbox: {}, wheel: {} })),
    ).toEqual(['event_log', 'wheel', 'sandbox'])
    expect(listProjectSections(project())).toEqual([])
    expect(listProjectSections(null)).toEqual([])
  })

  it('listProjectPassthroughKeys lists unknown top-level keys only', () => {
    const doc = project({
      version: '1.0',
      custom_tooling: { nested: true },
      x_extra: 1,
    })
    expect(listProjectPassthroughKeys(doc)).toEqual(['custom_tooling', 'x_extra'])
    expect(listProjectPassthroughKeys(null)).toEqual([])
  })
})
