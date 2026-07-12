/**
 * newFlowgroupDoc — creation-time YAML assembly + path + name validation.
 *
 * Each builder must produce YAML that parses via flowgroup-doc (or the yaml
 * parser for blueprint instances) and carries the right top-level keys.
 */
import { describe, expect, it } from 'vitest'
import { parse } from 'yaml'

import { listFlowgroups, parseFlowgroupFile, readFlowgroupMeta, selectFlowgroup } from '@/lib/flowgroup-doc'
import {
  blueprintInstancePath,
  buildBlankFlowgroupYaml,
  buildBlueprintInstanceYaml,
  buildTemplateFlowgroupYaml,
  flowgroupFilePath,
  validateName,
} from '../newFlowgroupDoc'

describe('buildBlankFlowgroupYaml', () => {
  it('parses to a flowgroup with pipeline/flowgroup and empty actions', () => {
    const yaml = buildBlankFlowgroupYaml('sales_raw', 'orders')
    const handle = parseFlowgroupFile(yaml)
    expect(handle.errors).toHaveLength(0)
    const [fg] = listFlowgroups(handle)
    expect(fg.name).toBe('orders')
    expect(fg.pipeline).toBe('sales_raw')
    expect(parse(yaml).actions).toEqual([])
  })
})

describe('buildTemplateFlowgroupYaml', () => {
  it('emits use_template + template_parameters', () => {
    const yaml = buildTemplateFlowgroupYaml('sales_raw', 'orders', 'csv_ingestion_template', {
      table_name: 'orders',
      landing_folder: 'orders',
    })
    const doc = selectFlowgroup(parseFlowgroupFile(yaml), 'orders')!
    const meta = readFlowgroupMeta(doc)
    expect(meta.use_template).toBe('csv_ingestion_template')
    expect(meta.template_parameters).toEqual({ table_name: 'orders', landing_folder: 'orders' })
  })

  it('omits template_parameters when no params are given', () => {
    const yaml = buildTemplateFlowgroupYaml('p', 'fg', 'tmpl', {})
    expect(parse(yaml)).toEqual({ pipeline: 'p', flowgroup: 'fg', use_template: 'tmpl' })
  })
})

describe('buildBlueprintInstanceYaml', () => {
  it('emits the new use_blueprint + nested parameters syntax', () => {
    const yaml = buildBlueprintInstanceYaml('domain_u_end_to_end', { site_name: 'site1' })
    expect(parse(yaml)).toEqual({
      use_blueprint: 'domain_u_end_to_end',
      parameters: { site_name: 'site1' },
    })
    // flowgroup-doc recognises it as a (non-authorable) instance file.
    expect(parseFlowgroupFile(yaml).blueprintLike).toBe(true)
  })

  it('omits parameters when none are given', () => {
    expect(parse(buildBlueprintInstanceYaml('bp', {}))).toEqual({ use_blueprint: 'bp' })
  })
})

describe('flowgroupFilePath / blueprintInstancePath', () => {
  it('derives the conventional pipeline path, with and without a subdir', () => {
    expect(flowgroupFilePath('sales_raw', '', 'orders')).toBe('pipelines/sales_raw/orders.yaml')
    expect(flowgroupFilePath('sales_raw', '01_bronze', 'orders')).toBe(
      'pipelines/sales_raw/01_bronze/orders.yaml',
    )
  })

  it('derives the blueprint instance path under pipelines/', () => {
    expect(blueprintInstancePath('site1_end_to_end')).toBe('pipelines/site1_end_to_end.yaml')
  })
})

describe('validateName', () => {
  it('accepts filesystem-safe names and empty (pending) input', () => {
    expect(validateName('customer_orders-1')).toBeNull()
    expect(validateName('')).toBeNull()
  })

  it('rejects names with spaces or path separators', () => {
    expect(validateName('bad name')).toMatch(/letters, numbers/)
    expect(validateName('a/b')).toMatch(/letters, numbers/)
  })
})
