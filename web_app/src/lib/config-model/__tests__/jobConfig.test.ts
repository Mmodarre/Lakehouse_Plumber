/**
 * Validator/classification suite for the job_config surface.
 *
 * Ground truth: `src/lhp/core/loaders/job_config_loader.py` (structure,
 * duplicates, legacy single-doc handling) and
 * `src/lhp/core/jobs/job_generator.py` (DEFAULT_JOB_CONFIG :60-66 and
 * EXPLICITLY_RENDERED_JOB_CONFIG_KEYS :38-56). The loader performs NO
 * per-field validation — everything unknown passes through — so this
 * validator is deliberately permissive: errors only where the loader raises.
 */
import { describe, expect, it } from 'vitest'

import {
  classifyJobDoc,
  JOB_BUILTIN_DEFAULTS,
  JOB_KNOWN_KEYS,
  listJobPassthroughKeys,
  validateJobConfigFile,
} from '../index'
import type { ValidationIssue } from '../index'

function errorsOf(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((i) => i.severity === 'error')
}

function warningsOf(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((i) => i.severity === 'warning')
}

describe('JOB_BUILTIN_DEFAULTS', () => {
  it('mirrors DEFAULT_JOB_CONFIG (job_generator.py:60-66)', () => {
    expect(JOB_BUILTIN_DEFAULTS).toEqual({
      max_concurrent_runs: 1,
      queue: { enabled: true },
      performance_target: 'STANDARD',
      generate_master_job: true,
      master_job_name: null,
    })
  })
})

describe('JOB_KNOWN_KEYS', () => {
  it('matches EXPLICITLY_RENDERED_JOB_CONFIG_KEYS (job_generator.py:38-56)', () => {
    expect([...JOB_KNOWN_KEYS].sort()).toEqual(
      [
        'max_concurrent_runs',
        'queue',
        'performance_target',
        'timeout_seconds',
        'tags',
        'email_notifications',
        'webhook_notifications',
        'permissions',
        'schedule',
        'notebook_cluster',
        'generate_master_job',
        'master_job_name',
      ].sort(),
    )
  })
})

describe('classifyJobDoc', () => {
  it('project_defaults doc → defaults (any document count)', () => {
    expect(classifyJobDoc({ project_defaults: {} }, 1)).toBe('defaults')
    expect(classifyJobDoc({ project_defaults: {} }, 3)).toBe('defaults')
  })

  it('job_name doc in a multi-doc file → job', () => {
    expect(classifyJobDoc({ job_name: 'j' }, 2)).toBe('job')
    expect(classifyJobDoc({ job_name: ['a', 'b'] }, 3)).toBe('job')
  })

  it('single flat doc without project_defaults → legacy-flat (loader :97-106 treats the WHOLE doc as project defaults)', () => {
    expect(classifyJobDoc({ max_concurrent_runs: 2 }, 1)).toBe('legacy-flat')
  })

  it('single doc WITH job_name is still legacy-flat — the single-doc path never reads job_name (loader :97-106)', () => {
    expect(classifyJobDoc({ job_name: 'j', tags: {} }, 1)).toBe('legacy-flat')
  })

  it('multi-doc flat doc without either key → unrecognized (loader :180-184 skips it)', () => {
    expect(classifyJobDoc({ max_concurrent_runs: 2 }, 3)).toBe('unrecognized')
  })

  it('non-mapping docs → unrecognized', () => {
    expect(classifyJobDoc(null, 1)).toBe('unrecognized')
    expect(classifyJobDoc('x', 2)).toBe('unrecognized')
    expect(classifyJobDoc([1], 1)).toBe('unrecognized')
  })
})

describe('validateJobConfigFile — duplicates and structure', () => {
  it('duplicate job_name across docs → VAL_004 error (loader :149-169)', () => {
    const issues = validateJobConfigFile([
      { project_defaults: {} },
      { job_name: 'j', tags: {} },
      { job_name: 'j' },
    ])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_004')
    expect(errors[0].docIndex).toBe(2)
    expect(errors[0].path).toEqual(['job_name'])
  })

  it('duplicate inside a job_name list → VAL_004 with the list index', () => {
    const issues = validateJobConfigFile([
      { project_defaults: {} },
      { job_name: ['a', 'b', 'a'] },
    ])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_004')
    expect(errors[0].path).toEqual(['job_name', 2])
  })

  it('empty job_name list → VAL_003 error (loader :132-145)', () => {
    const issues = validateJobConfigFile([{ project_defaults: {} }, { job_name: [] }])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_003')
  })

  it('invalid job_name type → warning, doc skipped (loader :126-130)', () => {
    const issues = validateJobConfigFile([{ project_defaults: {} }, { job_name: 42 }])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
  })

  it('multi-doc flat doc → warning (loader :180-184)', () => {
    const issues = validateJobConfigFile([{ project_defaults: {} }, { tags: {} }])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
  })

  it('single legacy flat doc → no issues (loader accepts it wholesale :102-106)', () => {
    expect(validateJobConfigFile([{ max_concurrent_runs: 2, trigger: {} }])).toEqual([])
  })

  it('null docs are dropped BEFORE the single/multi decision (loader :91): [null, job doc] is legacy-flat', () => {
    // With the null filtered out this is a one-document file, so the loader
    // treats the job_name doc as flat project defaults — no dup tracking, no
    // unrecognized warning.
    expect(validateJobConfigFile([null, { job_name: 'j' }])).toEqual([])
  })

  it('empty file → no issues (loader :93-95)', () => {
    expect(validateJobConfigFile([])).toEqual([])
    expect(validateJobConfigFile([null])).toEqual([])
  })

  it('sibling keys next to project_defaults are ignored by the loader → warning', () => {
    const issues = validateJobConfigFile([
      { project_defaults: {}, stray: 1 },
      { job_name: 'j' },
    ])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
    expect(warningsOf(issues)[0].docIndex).toBe(0)
  })

  it('single non-mapping doc → error (the loader crashes on it)', () => {
    const issues = validateJobConfigFile([[1, 2, 3]])
    expect(errorsOf(issues)).toHaveLength(1)
  })
})

describe('validateJobConfigFile — schedule stays warning-only', () => {
  // The Python loader performs no field validation at all (job_config_loader.py
  // has no schedule handling); the template renders schedule.* as scalars
  // (templates/bundle/job_resource.yml.j2:91-95). Blocking save here would be
  // stricter than the loader, so shape problems are surfaced as warnings.
  it('valid schedule → clean', () => {
    expect(
      validateJobConfigFile([
        {
          project_defaults: {
            schedule: {
              quartz_cron_expression: '0 0 2 * * ?',
              timezone_id: 'UTC',
              pause_status: 'UNPAUSED',
            },
          },
        },
        { job_name: 'j' },
      ]),
    ).toEqual([])
  })

  it('non-mapping schedule → warning, not error', () => {
    const issues = validateJobConfigFile([
      { project_defaults: {} },
      { job_name: 'j', schedule: 'daily' },
    ])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
    expect(warningsOf(issues)[0].path).toEqual(['schedule'])
  })

  it('non-string schedule fields → warnings, not errors', () => {
    const issues = validateJobConfigFile([
      { project_defaults: { schedule: { quartz_cron_expression: 123 } } },
      { job_name: 'j' },
    ])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
    expect(warningsOf(issues)[0].path).toEqual([
      'project_defaults',
      'schedule',
      'quartz_cron_expression',
    ])
  })

  it('everything else passes through without complaint (loader is fully permissive)', () => {
    expect(
      validateJobConfigFile([
        {
          project_defaults: {
            max_concurrent_runs: 'not-an-int',
            performance_target: 42,
            queue: 'yes',
          },
        },
        { job_name: 'j', timeout_seconds: 'soon' },
      ]),
    ).toEqual([])
  })
})

describe('listJobPassthroughKeys', () => {
  it('lists run_as/trigger/health/git_source inside project_defaults; known keys are not listed', () => {
    const doc = {
      project_defaults: {
        trigger: { file_arrival: { url: 's3://b/' } },
        run_as: { service_principal_name: 'sp' },
        health: { rules: [] },
        git_source: { git_url: 'x' },
        max_concurrent_runs: 1,
        schedule: { quartz_cron_expression: '0 0 2 * * ?' },
      },
    }
    expect(listJobPassthroughKeys(doc, 2)).toEqual(['trigger', 'run_as', 'health', 'git_source'])
  })

  it('lists nested unknown maps on a job doc, excluding job_name itself', () => {
    const doc = { job_name: 'j', continuous: { pause_status: 'UNPAUSED' }, tags: {} }
    expect(listJobPassthroughKeys(doc, 2)).toEqual(['continuous'])
  })

  it('legacy flat doc: unknown top-level keys are passthrough', () => {
    const doc = { environments: [{ environment_key: 'e' }], queue: { enabled: true } }
    expect(listJobPassthroughKeys(doc, 1)).toEqual(['environments'])
  })

  it('non-mapping input → empty list', () => {
    expect(listJobPassthroughKeys(null, 1)).toEqual([])
    expect(listJobPassthroughKeys('x', 2)).toEqual([])
  })
})
