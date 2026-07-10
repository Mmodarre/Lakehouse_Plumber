/**
 * Validator/classification suite for the pipeline_config surface.
 *
 * Every rule asserted here mirrors a specific behavior of the Python loader
 * `src/lhp/core/loaders/pipeline_config_loader.py` — cited per test. A rule
 * stricter than the loader is a bug (it would block configs the CLI accepts);
 * a looser rule misses errors the CLI raises.
 */
import { describe, expect, it } from 'vitest'

import {
  classifyPipelineDoc,
  listPipelinePassthroughKeys,
  MONITORING_PIPELINE_ALIAS,
  PIPELINE_BUILTIN_DEFAULTS,
  PIPELINE_KNOWN_KEYS,
  validatePipelineConfigFile,
} from '../index'
import type { ValidationIssue } from '../index'

function defaultsDoc(settings: Record<string, unknown> = {}): Record<string, unknown> {
  return { project_defaults: settings }
}

function pipelineDoc(
  name: unknown,
  settings: Record<string, unknown> = {},
): Record<string, unknown> {
  return { pipeline: name, ...settings }
}

function errorsOf(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((i) => i.severity === 'error')
}

function warningsOf(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((i) => i.severity === 'warning')
}

describe('PIPELINE_BUILTIN_DEFAULTS', () => {
  it('mirrors DEFAULT_PIPELINE_CONFIG (pipeline_config_loader.py:18-23)', () => {
    expect(PIPELINE_BUILTIN_DEFAULTS).toEqual({
      serverless: true,
      edition: 'ADVANCED',
      channel: 'CURRENT',
      continuous: false,
    })
  })
})

describe('classifyPipelineDoc', () => {
  it('classifies a project_defaults document', () => {
    expect(classifyPipelineDoc(defaultsDoc())).toBe('defaults')
  })

  it('classifies a pipeline document (string and list forms)', () => {
    expect(classifyPipelineDoc(pipelineDoc('bronze'))).toBe('pipeline')
    expect(classifyPipelineDoc(pipelineDoc(['a', 'b']))).toBe('pipeline')
  })

  it('project_defaults wins when both keys are present (loader if/elif :137-144)', () => {
    expect(classifyPipelineDoc({ project_defaults: {}, pipeline: 'x' })).toBe('defaults')
  })

  it('classifies everything else as unrecognized', () => {
    expect(classifyPipelineDoc({})).toBe('unrecognized')
    expect(classifyPipelineDoc({ foo: 1 })).toBe('unrecognized')
    expect(classifyPipelineDoc(null)).toBe('unrecognized')
    expect(classifyPipelineDoc('scalar')).toBe('unrecognized')
    expect(classifyPipelineDoc([1, 2])).toBe('unrecognized')
  })
})

describe('validatePipelineConfigFile — duplicate pipeline names (VAL_006)', () => {
  // Loader rule (pipeline_config_loader.py:192-215): after expanding
  // `pipeline: <str>` to a one-element list, EVERY name is registered in a
  // single `seen_pipelines` set spanning ALL documents — group lists
  // included — and re-registering any name raises VAL_006. This catches a
  // repeat within one list too, because the set is updated per name as the
  // list is iterated.
  it('same name in two pipeline docs', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('a'), pipelineDoc('a')])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_006')
    expect(errors[0].docIndex).toBe(1)
    expect(errors[0].path).toEqual(['pipeline'])
  })

  it('name in one doc AND inside another doc group list', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('a'), pipelineDoc(['x', 'a'])])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_006')
    expect(errors[0].docIndex).toBe(1)
    expect(errors[0].path).toEqual(['pipeline', 1])
  })

  it('name twice within ONE group list', () => {
    const issues = validatePipelineConfigFile([pipelineDoc(['a', 'b', 'a'])])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_006')
    expect(errors[0].docIndex).toBe(0)
    expect(errors[0].path).toEqual(['pipeline', 2])
  })

  it('two groups sharing a member', () => {
    const issues = validatePipelineConfigFile([pipelineDoc(['a', 'b']), pipelineDoc(['b', 'c'])])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_006')
    expect(errors[0].docIndex).toBe(1)
    expect(errors[0].path).toEqual(['pipeline', 0])
  })

  it('no duplicates → clean', () => {
    const issues = validatePipelineConfigFile([
      defaultsDoc({ serverless: true }),
      pipelineDoc('a'),
      pipelineDoc(['b', 'c']),
    ])
    expect(issues).toEqual([])
  })

  it('a pipeline key on a both-keys doc is IGNORED by the loader, so it never registers', () => {
    // Loader :137-144 is if/elif: a doc carrying project_defaults never has
    // its `pipeline` key read, so a later doc reusing that name is NOT a dup.
    const issues = validatePipelineConfigFile([
      { project_defaults: {}, pipeline: 'x' },
      pipelineDoc('x'),
    ])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
    expect(warningsOf(issues)[0].docIndex).toBe(0)
  })
})

describe('validatePipelineConfigFile — structural document rules', () => {
  it('empty pipeline list → VAL_005 error (loader :158-171)', () => {
    const issues = validatePipelineConfigFile([pipelineDoc([])])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_005')
    expect(errors[0].path).toEqual(['pipeline'])
  })

  it('monitoring alias in a multi-name list → VAL_011 error (loader :173-186)', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc([MONITORING_PIPELINE_ALIAS, 'other']),
    ])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_011')
  })

  it('standalone monitoring alias is fine', () => {
    expect(validatePipelineConfigFile([pipelineDoc(MONITORING_PIPELINE_ALIAS)])).toEqual([])
  })

  it('invalid pipeline value type → warning and the doc is skipped BEFORE settings validation (loader :151-156)', () => {
    const issues = validatePipelineConfigFile([pipelineDoc(123, { edition: 'BOGUS' })])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
  })

  it('null documents are skipped silently (loader :130-131)', () => {
    expect(validatePipelineConfigFile([null, pipelineDoc('a'), undefined])).toEqual([])
  })

  it('non-mapping document → warning (loader :133-135)', () => {
    const issues = validatePipelineConfigFile(['just a string'])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
  })

  it('doc with neither key → warning (loader :222-225)', () => {
    const issues = validatePipelineConfigFile([{ foo: 1 }])
    expect(errorsOf(issues)).toEqual([])
    expect(warningsOf(issues)).toHaveLength(1)
  })

  it('non-mapping project_defaults value → error (loader crashes on it downstream)', () => {
    const issues = validatePipelineConfigFile([{ project_defaults: 'oops' }])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].path).toEqual(['project_defaults'])
  })
})

describe('validatePipelineConfigFile — enums (loader _validate_config :301-347)', () => {
  it.each(['CORE', 'PRO', 'ADVANCED'])('edition %s is valid', (edition) => {
    expect(validatePipelineConfigFile([pipelineDoc('p', { edition })])).toEqual([])
  })

  it.each(['CURRENT', 'PREVIEW'])('channel %s is valid', (channel) => {
    expect(validatePipelineConfigFile([pipelineDoc('p', { channel })])).toEqual([])
  })

  it.each(['wheel', 'source'])('packaging %s is valid', (packaging) => {
    expect(validatePipelineConfigFile([pipelineDoc('p', { packaging })])).toEqual([])
  })

  it('invalid edition → VAL_009 error', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { edition: 'BASIC' })])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_009')
    expect(errors[0].path).toEqual(['edition'])
  })

  it('invalid channel → VAL_009 error', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { channel: 'STABLE' })])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].code).toBe('VAL_009')
  })

  it('invalid packaging → VAL_062 error', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { packaging: 'zip' })])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].code).toBe('VAL_062')
  })

  it('enum violations inside project_defaults get the project_defaults path prefix', () => {
    const issues = validatePipelineConfigFile([defaultsDoc({ edition: 'BAD' })])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].path).toEqual(['project_defaults', 'edition'])
  })

  it('case-sensitive: lowercase edition is rejected like the loader', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { edition: 'advanced' })])
    expect(errorsOf(issues)).toHaveLength(1)
  })
})

describe('validatePipelineConfigFile — configuration map (loader :366-400)', () => {
  it('string→string configuration is valid', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { configuration: { 'spark.x': 'false', 'spark.y': '128' } }),
    ])
    expect(issues).toEqual([])
  })

  it('non-mapping configuration → VAL_009 error', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { configuration: ['a'] })])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].code).toBe('VAL_009')
  })

  it('non-string configuration value → VAL_009 ERROR (loader :381-400 hard-fails; the task brief said warning but the loader wins)', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { configuration: { 'spark.x': 128 } }),
    ])
    const errors = errorsOf(issues)
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBe('VAL_009')
    expect(errors[0].path).toEqual(['configuration', 'spark.x'])
  })

  it('boolean and null configuration values are also errors', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { configuration: { a: false, b: null } }),
    ])
    expect(errorsOf(issues)).toHaveLength(2)
  })
})

describe('validatePipelineConfigFile — environment (loader :349-364)', () => {
  it('mapping environment is valid', () => {
    expect(
      validatePipelineConfigFile([pipelineDoc('p', { environment: { dependencies: ['x==1'] } })]),
    ).toEqual([])
  })

  it('non-mapping environment → VAL_009 error', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { environment: ['x==1'] })])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].code).toBe('VAL_009')
  })
})

describe('validatePipelineConfigFile — permissions (loader :404-481)', () => {
  it('level plus exactly one principal is valid', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', {
        permissions: [
          { level: 'CAN_MANAGE', user_name: 'u@x.com' },
          { level: 'CAN_VIEW', group_name: 'g' },
          { level: 'CAN_RUN', service_principal_name: 'sp' },
        ],
      }),
    ])
    expect(issues).toEqual([])
  })

  it('zero principals → error (exactly one required, loader :463-481)', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { permissions: [{ level: 'CAN_MANAGE' }] }),
    ])
    expect(errorsOf(issues)).toHaveLength(1)
    expect(errorsOf(issues)[0].path).toEqual(['permissions', 0])
  })

  it('two principals → error', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', {
        permissions: [{ level: 'CAN_MANAGE', user_name: 'u', group_name: 'g' }],
      }),
    ])
    expect(errorsOf(issues)).toHaveLength(1)
  })

  it('missing level → error (loader :445-462)', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { permissions: [{ user_name: 'u' }] }),
    ])
    expect(errorsOf(issues)).toHaveLength(1)
  })

  it('non-string level → error', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { permissions: [{ level: 1, user_name: 'u' }] }),
    ])
    expect(errorsOf(issues)).toHaveLength(1)
  })

  it('non-mapping entry → error (loader :428-444)', () => {
    const issues = validatePipelineConfigFile([pipelineDoc('p', { permissions: ['CAN_VIEW'] })])
    expect(errorsOf(issues)).toHaveLength(1)
  })

  it('non-list permissions → error (loader :405-424)', () => {
    const issues = validatePipelineConfigFile([
      pipelineDoc('p', { permissions: { level: 'CAN_MANAGE', user_name: 'u' } }),
    ])
    expect(errorsOf(issues)).toHaveLength(1)
  })
})

describe('listPipelinePassthroughKeys / PIPELINE_KNOWN_KEYS', () => {
  it('knows every explicitly rendered key plus packaging (bundle/manager.py:29-46, loader :25-27)', () => {
    for (const key of [
      'catalog',
      'schema',
      'serverless',
      'clusters',
      'configuration',
      'continuous',
      'photon',
      'edition',
      'channel',
      'notifications',
      'tags',
      'event_log',
      'environment',
      'permissions',
      'packaging',
    ]) {
      expect(PIPELINE_KNOWN_KEYS.has(key)).toBe(true)
    }
    expect(PIPELINE_KNOWN_KEYS.has('run_as')).toBe(false)
  })

  it('lists unknown keys on a pipeline doc, including nested unknown maps; known keys are not listed', () => {
    const doc = pipelineDoc('p', {
      serverless: false,
      run_as: { service_principal_name: 'sp' },
      custom_block: { nested: { a: 1 } },
      tags: { team: 'x' },
    })
    expect(listPipelinePassthroughKeys(doc)).toEqual(['run_as', 'custom_block'])
  })

  it('lists unknown keys inside project_defaults for a defaults doc', () => {
    const doc = defaultsDoc({ serverless: true, run_as: { user_name: 'u' }, trigger: {} })
    expect(listPipelinePassthroughKeys(doc)).toEqual(['run_as', 'trigger'])
  })

  it('lists all keys of an unrecognized doc and nothing for non-mappings', () => {
    expect(listPipelinePassthroughKeys({ foo: 1, bar: 2 })).toEqual(['foo', 'bar'])
    expect(listPipelinePassthroughKeys(null)).toEqual([])
    expect(listPipelinePassthroughKeys('str')).toEqual([])
  })
})
