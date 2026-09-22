import { describe, expect, it } from 'vitest'
import { addTemplateParam, parseFlowgroupFile, readTemplateParams, selectTemplate, serializeFlowgroupFile, setTemplateParamField } from '../flowgroup-doc'
import { hasTemplateDefault, inspectTemplateReferences, parseTemplateValue, referencesForParameter, setTemplateMetadata, templateParameterIssues, templateReferenceForPath, templateStructuredEditIssue, validateTemplatePath } from '../template-document'
const source = '# template header\r\nname: ingestion\r\nversion: "1.0" # keep\r\nunknown: {stay: yes}\r\nparameters:\r\n  - name: table\r\n    default: "" # explicit blank\r\nactions:\r\n  - name: "load_{{ table }}"\r\n    type: load\r\n    target: "{{ table | lower }}"\r\n    sql: |\r\n      SELECT {{ table }}\r\n      {% if enabled %} WHERE 1=1 {% endif %}\r\n'
function load(text = source) { const file = parseFlowgroupFile(text); const template = selectTemplate(file)!; return { file, template } }
describe('template document adapter', () => {
  it('changes metadata while retaining comments, unknown keys, SQL and CRLF', () => {
    const { file, template } = load()
    setTemplateMetadata(template.body, 'version', '2.0')
    expect(serializeFlowgroupFile(file)).toBe(source.replace('"1.0"', '"2.0"'))
  })
  it.each([null, false, 0, '', [], {}])('keeps explicit default %j distinct from omission', (value) => {
    const { file, template } = load()
    setTemplateParamField(template.body, 0, ['default'], value)
    const params = readTemplateParams(selectTemplate(parseFlowgroupFile(serializeFlowgroupFile(file)))!)
    expect(hasTemplateDefault(params[0])).toBe(true)
    expect(params[0].default).toEqual(value)
    addTemplateParam(template.body, { name: 'omitted' })
    expect(hasTemplateDefault(readTemplateParams(template)[1])).toBe(false)
  })
  it('inspects direct and possible complex uses without altering expressions', () => {
    const { file, template } = load()
    const refs = inspectTemplateReferences(template.body)
    expect(refs.filter((r) => r.parameter === 'table')).toHaveLength(2)
    expect(referencesForParameter(refs, 'table')).toHaveLength(3)
    expect(serializeFlowgroupFile(file)).toBe(source)
  })
  it('labels block-only expressions as unrendered runtime syntax', () => {
    const { template } = load('name: x\nactions:\n  - sql: "{% if x %}yes{% endif %}"\n')
    expect(inspectTemplateReferences(template.body)[0].kind).toBe('block-only')
  })
  it('reports duplicate declarations while preserving raw unknown metadata', () => {
    const { template } = load()
    addTemplateParam(template.body, { name: 'table', unknown_hint: 'keep' })
    const params = readTemplateParams(template)
    expect(templateParameterIssues(params)).toHaveLength(2)
    expect(params[1].raw.unknown_hint).toBe('keep')
  })
  it('preserves aliases and chooses Code instead of unsafe structured mutation', () => {
    const text = 'name: t\nparameters: &inputs []\nactions: []\nunknown: *inputs\n'
    const { file } = load(text)
    expect(serializeFlowgroupFile(file)).toBe(text)
    expect(templateStructuredEditIssue(text)).toContain('anchors or aliases')
    expect(templateStructuredEditIssue('name: t\nactions: []\n---\nname: t2\nactions: []\n')).toContain('multi-document')
  })
  it('parses native nested values and rejects malformed or non-JSON numbers', () => {
    expect(parseTemplateValue('[false, 0, {nested: []}]', 'yaml')).toEqual({ ok: true, value: [false, 0, { nested: [] }] })
    expect(parseTemplateValue('', 'string')).toEqual({ ok: true, value: '' })
    expect(parseTemplateValue('[unclosed', 'yaml').ok).toBe(false)
    expect(parseTemplateValue('.inf', 'yaml').ok).toBe(false)
  })
  it('keeps nested invocation references and prevents new path collisions', () => {
    expect(templateReferenceForPath('templates/ingestion/orders.yaml')).toBe('ingestion/orders')
    expect(templateReferenceForPath('templates/orders.yml')).toBeNull()
    expect(validateTemplatePath('templates/ingestion/orders.yaml')).toBeNull()
    expect(validateTemplatePath('templates/../orders.yaml')).toBeTruthy()
    expect(validateTemplatePath('templates/orders.yaml', ['templates/orders.yaml'])).toContain('already exists')
  })
})
