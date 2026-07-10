/**
 * Real-templates smoke suite: the packaged config templates that `lhp init`
 * ships must render clean in the Config UI — zero errors, and a pinned
 * (empty) warning set. Templates are read from disk and fed through the
 * Task-3 yaml-doc layer exactly as the forms will consume them.
 */
/// <reference types="node" />
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

import { documentCount, parseConfigFile, toJS } from '../../yaml-doc'
import {
  validateJobConfigFile,
  validatePipelineConfigFile,
  validateProjectConfig,
} from '../index'

const REPO_ROOT = resolve(import.meta.dirname, '../../../../..')
const INIT_DIR = resolve(REPO_ROOT, 'src/lhp/templates/init')
const INIT_SAMPLE_DIR = resolve(REPO_ROOT, 'src/lhp/templates/init_sample')

function read(path: string): string {
  return readFileSync(path, 'utf8')
}

/** Fill the Jinja placeholders `lhp init` renders into lhp.yaml.j2. */
function renderInitPlaceholders(source: string): string {
  return source
    .replace(/\{\{\s*project_name\s*\}\}/g, 'demo_project')
    .replace(/\{\{\s*author\s*\}\}/g, 'Demo Author')
    .replace(/\{\{\s*current_date\s*\}\}/g, '2026-01-01')
}

function allDocs(source: string): unknown[] {
  const handle = parseConfigFile(source)
  expect(handle.errors).toEqual([])
  const docs: unknown[] = []
  for (let i = 0; i < documentCount(handle); i++) docs.push(toJS(handle, i))
  return docs
}

describe('packaged templates validate clean', () => {
  it('pipeline_config_env.yaml.tmpl → no errors, no warnings', () => {
    const docs = allDocs(read(resolve(INIT_DIR, 'config/pipeline_config_env.yaml.tmpl')))
    expect(validatePipelineConfigFile(docs)).toEqual([])
  })

  it('job_config_env.yaml.tmpl → no errors, no warnings', () => {
    const docs = allDocs(read(resolve(INIT_DIR, 'config/job_config_env.yaml.tmpl')))
    expect(validateJobConfigFile(docs)).toEqual([])
  })

  it('monitoring_job_config_env.yaml.tmpl → no errors, no warnings (legacy single-doc job config)', () => {
    const docs = allDocs(read(resolve(INIT_DIR, 'config/monitoring_job_config_env.yaml.tmpl')))
    expect(validateJobConfigFile(docs)).toEqual([])
  })

  it('init lhp.yaml.j2 → no errors, no warnings', () => {
    const docs = allDocs(renderInitPlaceholders(read(resolve(INIT_DIR, 'lhp.yaml.j2'))))
    expect(docs).toHaveLength(1)
    expect(validateProjectConfig(docs[0])).toEqual([])
  })

  it('init_sample lhp.yaml.j2 (event_log + monitoring enabled) → no errors, no warnings', () => {
    const docs = allDocs(renderInitPlaceholders(read(resolve(INIT_SAMPLE_DIR, 'lhp.yaml.j2'))))
    expect(docs).toHaveLength(1)
    expect(validateProjectConfig(docs[0])).toEqual([])
  })
})
