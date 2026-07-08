import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  derivePipelineFromYaml,
  isYamlPath,
  startScopedValidate,
} from '../yamlSaveSupport'

describe('isYamlPath', () => {
  it('accepts .yaml and .yml, case-insensitively', () => {
    expect(isYamlPath('pipelines/raw/orders.yaml')).toBe(true)
    expect(isYamlPath('pipelines/raw/orders.yml')).toBe(true)
    expect(isYamlPath('ORDERS.YAML')).toBe(true)
    expect(isYamlPath('orders.YML')).toBe(true)
  })

  it('rejects other extensions and extensionless paths', () => {
    expect(isYamlPath('generated/orders.py')).toBe(false)
    expect(isYamlPath('queries/orders.sql')).toBe(false)
    expect(isYamlPath('Makefile')).toBe(false)
    expect(isYamlPath('archive.yaml.bak')).toBe(false)
  })
})

describe('derivePipelineFromYaml', () => {
  it('extracts a plain top-level pipeline value', () => {
    expect(derivePipelineFromYaml('pipeline: raw_ingest\nflowgroup: orders\n')).toBe(
      'raw_ingest',
    )
  })

  it('returns null when no pipeline key exists', () => {
    expect(derivePipelineFromYaml('flowgroup: orders\nactions: []\n')).toBeNull()
    expect(derivePipelineFromYaml('')).toBeNull()
  })

  it('ignores indented (nested) pipeline keys', () => {
    const yaml = 'presets:\n  pipeline: nested_value\n'
    expect(derivePipelineFromYaml(yaml)).toBeNull()
  })

  it('ignores commented-out pipeline lines', () => {
    expect(derivePipelineFromYaml('# pipeline: ghost\npipeline: real\n')).toBe('real')
  })

  it('strips surrounding double and single quotes', () => {
    expect(derivePipelineFromYaml('pipeline: "raw ingest"\n')).toBe('raw ingest')
    expect(derivePipelineFromYaml("pipeline: 'raw'\n")).toBe('raw')
  })

  it('drops a trailing inline comment on an unquoted scalar', () => {
    expect(derivePipelineFromYaml('pipeline: raw # the bronze layer\n')).toBe('raw')
  })

  it('keeps a hash inside a quoted scalar', () => {
    expect(derivePipelineFromYaml('pipeline: "raw # not-a-comment"\n')).toBe(
      'raw # not-a-comment',
    )
  })

  it('returns null for block scalars and empty values', () => {
    expect(derivePipelineFromYaml('pipeline: |\n  multi\n')).toBeNull()
    expect(derivePipelineFromYaml('pipeline: >\n  folded\n')).toBeNull()
    expect(derivePipelineFromYaml('pipeline:\nflowgroup: x\n')).toBeNull()
    expect(derivePipelineFromYaml('pipeline: ""\n')).toBeNull()
  })

  it('uses the first top-level pipeline key when several exist', () => {
    expect(derivePipelineFromYaml('pipeline: first\npipeline: second\n')).toBe('first')
  })

  it('tolerates trailing whitespace after the value', () => {
    expect(derivePipelineFromYaml('pipeline: raw   \n')).toBe('raw')
  })
})

describe('startScopedValidate', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  function controller(isRunning: boolean) {
    return {
      isRunning,
      startValidate: vi.fn(),
      abort: vi.fn(),
    }
  }

  it('starts immediately when no run is in flight', () => {
    const ctl = controller(false)
    startScopedValidate(ctl, 'raw')
    expect(ctl.abort).not.toHaveBeenCalled()
    expect(ctl.startValidate).toHaveBeenCalledExactlyOnceWith(undefined, 'raw')
  })

  it('passes an undefined pipeline through for an unscoped validate', () => {
    const ctl = controller(false)
    startScopedValidate(ctl, undefined)
    expect(ctl.startValidate).toHaveBeenCalledExactlyOnceWith(undefined, undefined)
  })

  it('aborts an in-flight run and defers the start to the next macrotask', () => {
    const ctl = controller(true)
    startScopedValidate(ctl, 'raw')

    expect(ctl.abort).toHaveBeenCalledTimes(1)
    // Not yet — the transport hook clears its running flag asynchronously.
    expect(ctl.startValidate).not.toHaveBeenCalled()

    vi.advanceTimersByTime(0)
    expect(ctl.startValidate).toHaveBeenCalledExactlyOnceWith(undefined, 'raw')
  })
})
