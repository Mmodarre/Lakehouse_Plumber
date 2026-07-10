import { describe, expect, it } from 'vitest'
import type { FileNode } from '../../../types/api'
import {
  defaultTemplatePath,
  isPreferredForKind,
  listAllFilePaths,
  listConfigYamlFiles,
  validateNewConfigPath,
} from '../configFileSupport'

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
        { name: 'zeta.yaml', path: 'config/zeta.yaml', type: 'file' },
        { name: 'job_config_dev.yaml', path: 'config/job_config_dev.yaml', type: 'file' },
        { name: 'pipeline_config_dev.yaml', path: 'config/pipeline_config_dev.yaml', type: 'file' },
        { name: 'notes.md', path: 'config/notes.md', type: 'file' },
        {
          name: 'sub',
          path: 'config/sub',
          type: 'directory',
          children: [
            { name: 'pipeline_config_prod.yml', path: 'config/sub/pipeline_config_prod.yml', type: 'file' },
          ],
        },
        {
          name: 'monitoring_job_config_dev.yaml',
          path: 'config/monitoring_job_config_dev.yaml',
          type: 'file',
        },
      ],
    },
    { name: 'lhp.yaml', path: 'lhp.yaml', type: 'file' },
    {
      name: 'pipelines',
      path: 'pipelines',
      type: 'directory',
      children: [{ name: 'a.yaml', path: 'pipelines/a.yaml', type: 'file' }],
    },
  ],
}

describe('listAllFilePaths', () => {
  it('flattens files depth-first and tolerates an undefined tree', () => {
    expect(listAllFilePaths(undefined)).toEqual([])
    expect(listAllFilePaths(tree)).toContain('config/sub/pipeline_config_prod.yml')
    expect(listAllFilePaths(tree)).toContain('lhp.yaml')
    expect(listAllFilePaths(tree)).not.toContain('config') // directories excluded
  })
})

describe('listConfigYamlFiles', () => {
  it('keeps only config/ YAMLs (both .yaml and .yml), excluding other trees and extensions', () => {
    const files = listConfigYamlFiles(tree, 'pipeline')
    expect(files).not.toContain('lhp.yaml')
    expect(files).not.toContain('pipelines/a.yaml')
    expect(files).not.toContain('config/notes.md')
    expect(files).toContain('config/zeta.yaml')
    expect(files).toContain('config/sub/pipeline_config_prod.yml')
  })

  it('sorts the pipeline tab preference first, alphabetical within groups', () => {
    expect(listConfigYamlFiles(tree, 'pipeline')).toEqual([
      'config/pipeline_config_dev.yaml',
      'config/sub/pipeline_config_prod.yml',
      'config/job_config_dev.yaml',
      'config/monitoring_job_config_dev.yaml',
      'config/zeta.yaml',
    ])
  })

  it('sorts job + monitoring configs first on the job tab', () => {
    expect(listConfigYamlFiles(tree, 'job')).toEqual([
      'config/job_config_dev.yaml',
      'config/monitoring_job_config_dev.yaml',
      'config/pipeline_config_dev.yaml',
      'config/sub/pipeline_config_prod.yml',
      'config/zeta.yaml',
    ])
  })
})

describe('isPreferredForKind', () => {
  it('matches on the basename, not the directory', () => {
    expect(isPreferredForKind('config/sub/pipeline_config_prod.yml', 'pipeline')).toBe(true)
    expect(isPreferredForKind('config/pipeline_dir/other.yaml', 'pipeline')).toBe(false)
    expect(isPreferredForKind('config/monitoring_job_config_dev.yaml', 'job')).toBe(true)
    expect(isPreferredForKind('config/job_config_dev.yaml', 'job')).toBe(true)
    expect(isPreferredForKind('config/job_config_dev.yaml', 'pipeline')).toBe(false)
  })
})

describe('defaultTemplatePath', () => {
  it('builds the env-suffixed default under config/', () => {
    expect(defaultTemplatePath('pipeline_config', 'dev')).toBe('config/pipeline_config_dev.yaml')
    expect(defaultTemplatePath('monitoring_job_config', 'tst')).toBe(
      'config/monitoring_job_config_tst.yaml',
    )
  })
})

describe('validateNewConfigPath', () => {
  const existing = new Set(['config/pipeline_config_dev.yaml'])

  it('accepts a fresh config/ .yaml path (including subdirectories and .yml)', () => {
    expect(validateNewConfigPath('config/pipeline_config_prod.yaml', existing)).toBeNull()
    expect(validateNewConfigPath('config/sub/job_config_dev.yml', existing)).toBeNull()
  })

  it('rejects empty, misplaced, or wrongly-suffixed paths', () => {
    expect(validateNewConfigPath('', existing)).toBe('Enter a file name')
    expect(validateNewConfigPath('   ', existing)).toBe('Enter a file name')
    expect(validateNewConfigPath('pipeline_config.yaml', existing)).toBe(
      'The file must live under config/',
    )
    expect(validateNewConfigPath('config/pipeline_config.json', existing)).toBe(
      'The file name must end with .yaml',
    )
    expect(validateNewConfigPath('config/.yaml', existing)).toBe('Enter a file name')
  })

  it('rejects traversal, empty segments, and disallowed characters', () => {
    expect(validateNewConfigPath('config/../secrets.yaml', existing)).not.toBeNull()
    expect(validateNewConfigPath('config//x.yaml', existing)).toBe('Invalid path')
    expect(validateNewConfigPath('config/a b.yaml', existing)).toBe(
      'Only letters, digits, ".", "_" and "-" are allowed',
    )
  })

  it('refuses a path that already exists in the tree', () => {
    expect(validateNewConfigPath('config/pipeline_config_dev.yaml', existing)).toBe(
      'This file already exists',
    )
  })
})
