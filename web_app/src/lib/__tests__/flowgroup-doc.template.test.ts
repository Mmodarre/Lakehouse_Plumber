import { describe, expect, it } from 'vitest'
import {
  addTemplateParam,
  deleteTemplateParam,
  deleteTemplateParamField,
  deriveGraph,
  parseFlowgroupFile,
  readTemplateParams,
  selectFlowgroup,
  selectTemplate,
  serializeFlowgroupFile,
  setTemplateParamField,
} from '../flowgroup-doc'
import type { FlowgroupFileHandle } from '../flowgroup-doc'

// A template file: top-level `name` + `parameters` + `actions`, with
// `{{ param }}` Jinja references sprinkled through the action body.
const TEMPLATE = `# CSV ingestion template
name: csv_ingestion_template
version: "1.0"
description: "Ingest CSV"

parameters:
  - name: table_name
    required: true
    description: "Name of the table"
  - name: table_properties
    required: false
    default: {}

actions:
  - name: load_{{ table_name }}_csv
    type: load
    readMode: stream
    source:
      type: cloudfiles
      path: "/land/{{ table_name }}/*.csv"
      format: csv
    target: v_{{ table_name }}_cloudfiles
  - name: write_{{ table_name }}
    type: write
    source: v_{{ table_name }}_cloudfiles
    write_target:
      type: streaming_table
      table: "{{ table_name }}"
`

const FLOWGROUP = `pipeline: p
flowgroup: fg
actions:
  - name: a1
    type: transform
    transform_type: sql
    source: v_raw
    target: v_out
`

// Blueprint definition: parameters + flowgroups, NO actions.
const BLUEPRINT = `name: my_blueprint
parameters:
  - name: region
flowgroups:
  - flowgroup: fg_{{ region }}
    pipeline: p
`

function template(yaml: string) {
  const file = parseFlowgroupFile(yaml)
  const t = selectTemplate(file)
  if (t === undefined) throw new Error('not a template')
  return { file, t }
}

/** Re-parse after mutations, exactly as the write path does before each save. */
function reselect(file: FlowgroupFileHandle) {
  const t = selectTemplate(parseFlowgroupFile(serializeFlowgroupFile(file)))
  if (t === undefined) throw new Error('lost the template')
  return t
}

describe('flowgroup-doc — template detection', () => {
  it('flags a template file and does NOT enumerate it as a flowgroup', () => {
    const file = parseFlowgroupFile(TEMPLATE)
    expect(file.templateLike).toBe(true)
    expect(file.blueprintLike).toBe(false)
    expect(file.flowgroups).toHaveLength(0)
    // The top-level `name` (not `flowgroup`) must not raise a missing-name warning.
    expect(file.warnings).toHaveLength(0)
    // selectFlowgroup finds nothing; selectTemplate does.
    expect(selectFlowgroup(file, 'csv_ingestion_template')).toBeUndefined()
    expect(selectTemplate(file)).toBeDefined()
  })

  it('leaves ordinary flowgroups and blueprints unchanged', () => {
    const fg = parseFlowgroupFile(FLOWGROUP)
    expect(fg.templateLike).toBe(false)
    expect(fg.flowgroups).toHaveLength(1)
    expect(selectTemplate(fg)).toBeUndefined()

    const bp = parseFlowgroupFile(BLUEPRINT)
    expect(bp.templateLike).toBe(false)
    expect(bp.blueprintLike).toBe(true)
    expect(selectTemplate(bp)).toBeUndefined()
  })

  it('exposes template metadata and declared parameters', () => {
    const { t } = template(TEMPLATE)
    expect(t.info.name).toBe('csv_ingestion_template')
    expect(t.info.version).toBe('1.0')
    expect(t.info.description).toBe('Ingest CSV')
    expect(t.info.paramCount).toBe(2)

    const params = readTemplateParams(t)
    expect(params.map((p) => p.name)).toEqual(['table_name', 'table_properties'])
    expect(params[0].required).toBe(true)
    expect(params[0].description).toBe('Name of the table')
    expect(params[1].required).toBe(false)
    expect(params[1].default).toEqual({})
  })
})

describe('flowgroup-doc — template graph derivation', () => {
  it('wires the template body by view name like a flowgroup', () => {
    const { t } = template(TEMPLATE)
    const graph = deriveGraph(t.body)
    expect(graph.nodes.map((n) => n.name)).toEqual([
      'load_{{ table_name }}_csv',
      'write_{{ table_name }}',
    ])
    // The write consumes the load's `{{ }}`-templated view: one internal edge.
    expect(graph.externals).toHaveLength(0)
    expect(graph.edges).toHaveLength(1)
    expect(graph.edges[0].kind).toBe('data')
    expect(graph.edges[0].viewName).toBe('v_{{ table_name }}_cloudfiles')
    expect(graph.edges[0].from).toBe('load_{{ table_name }}_csv')
    expect(graph.edges[0].to).toBe('write_{{ table_name }}')
  })
})

describe('flowgroup-doc — parameter mutators (comment-preserving)', () => {
  it('adds a parameter, keeping the file comment', () => {
    const { file, t } = template(TEMPLATE)
    addTemplateParam(t.body, { name: 'landing_folder', required: true })
    const out = serializeFlowgroupFile(file)
    expect(out).toContain('# CSV ingestion template')
    const params = reselect(file)
    expect(readTemplateParams(params).map((p) => p.name)).toEqual([
      'table_name',
      'table_properties',
      'landing_folder',
    ])
  })

  it('creates the parameters list when a template declares none', () => {
    const bare = `name: bare
actions:
  - name: load_x
    type: load
    source:
      type: delta
      table: t
    target: v_x
`
    const { file, t } = template(bare)
    expect(readTemplateParams(t)).toHaveLength(0)
    addTemplateParam(t.body, { name: 'first' })
    expect(readTemplateParams(reselect(file)).map((p) => p.name)).toEqual(['first'])
  })

  it('edits a parameter field surgically (unrelated lines survive)', () => {
    const { file, t } = template(TEMPLATE)
    setTemplateParamField(t.body, 0, ['type'], 'string')
    const out = serializeFlowgroupFile(file)
    expect(out).toContain('# CSV ingestion template')
    expect(out).toContain('description: "Name of the table"')
    expect(readTemplateParams(reselect(file))[0].type).toBe('string')
  })

  it('deletes a parameter field (required toggled off)', () => {
    const { file, t } = template(TEMPLATE)
    deleteTemplateParamField(t.body, 0, ['required'])
    expect(readTemplateParams(reselect(file))[0].required).toBeUndefined()
  })

  it('deletes a whole parameter', () => {
    const { file, t } = template(TEMPLATE)
    deleteTemplateParam(t.body, 1)
    expect(readTemplateParams(reselect(file)).map((p) => p.name)).toEqual(['table_name'])
  })

  it('round-trips object/array defaults', () => {
    const { file, t } = template(TEMPLATE)
    setTemplateParamField(t.body, 1, ['default'], [])
    expect(readTemplateParams(reselect(file))[1].default).toEqual([])
  })

  it('authored parameters match the API-declared parameter shape', () => {
    // GET /api/templates/{name} declares each param as
    // {name, type, required, description, default}. Author one with every
    // field and confirm the written YAML carries exactly those keys.
    const { file, t } = template(TEMPLATE)
    addTemplateParam(t.body, { name: 'schema_file' })
    setTemplateParamField(t.body, 2, ['type'], 'string')
    setTemplateParamField(t.body, 2, ['required'], true)
    setTemplateParamField(t.body, 2, ['default'], '')
    setTemplateParamField(t.body, 2, ['description'], 'Schema file name')

    const p = readTemplateParams(reselect(file))[2]
    expect(p.name).toBe('schema_file')
    expect(p.type).toBe('string')
    expect(p.required).toBe(true)
    expect(p.default).toBe('')
    expect(p.description).toBe('Schema file name')
    expect(Object.keys(p.raw).sort()).toEqual([
      'default',
      'description',
      'name',
      'required',
      'type',
    ])
  })
})
