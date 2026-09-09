import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { resolveFieldHelp, helpSourceLink, type HelpCatalog } from '../field-help'
import { yamlHelpContext } from '../yaml-help-context'
const catalog = (kind: string) => JSON.parse(readFileSync(resolve(process.cwd(), `../src/lhp/schemas/help/${kind}.json`), 'utf8')) as HelpCatalog

describe('contextual field guidance', () => {
  it('distinguishes identical source.schema paths by actual action subtype', () => {
    const c = catalog('flowgroup')
    expect(resolveFieldHelp(c, ['source','schema'], 'load:cloudfiles')?.summary).toContain('file')
    expect(resolveFieldHelp(c, ['source','schema'], 'load:delta')?.summary).toContain('Unity Catalog')
    expect(resolveFieldHelp(c, ['source','schema'], 'transform:sql')).toBeUndefined()
  })
  it('resolves numeric parameter/expectation rows with wildcard bindings', () => {
    expect(resolveFieldHelp(catalog('template'), ['parameters', 2, 'required'])?.id).toBe('template.parameters.*.required')
    expect(resolveFieldHelp(catalog('flowgroup'), ['expectations', 3, 'expression'], 'test:custom_expectations')?.summary).toContain('true for a valid row')
  })
  it('locates a template action without confusing its parameter declaration', () => {
    const text = 'name: test\nparameters:\n  - name: schema\n    required: true\nactions:\n  - name: load\n    type: load\n    source:\n      type: delta\n      schema: "{{ schema }}"\n'
    const action = yamlHelpContext('templates/nested/test.yaml', text, text.lastIndexOf('schema:') + 2)
    expect(action).toMatchObject({kind: 'flowgroup', subtype: 'load:delta', path: ['source', 'schema']})
    expect(yamlHelpContext('templates/test.yaml', text, text.indexOf('required:') + 2)).toMatchObject({kind: 'template', path: ['parameters',0,'required']})
  })
  it('resolves settings in the selected document and strips project_defaults', () => {
    const text = 'project_defaults:\n  packaging: wheel\n---\npipeline: orders\npackaging: source\n'
    expect(yamlHelpContext('/config/pipeline_config.yaml', text, text.indexOf('packaging') + 2)).toMatchObject({kind: 'pipeline_config', path: ['packaging']})
    expect(yamlHelpContext('/config/pipeline_config.yaml', text, text.lastIndexOf('packaging') + 2)).toMatchObject({kind: 'pipeline_config', path: ['packaging']})
  })
  it('does not invent an action subtype for incomplete source', () => {
    const text = 'actions:\n  - name: load\n    source:\n      schema: bronze\n'
    expect(yamlHelpContext('pipelines/test.yaml', text, text.indexOf('schema') + 1)).toBeUndefined()
    expect(yamlHelpContext('py_functions/test.py', text, 5)).toBeUndefined()
  })
  it('uses documentation URLs and escapes source path segments', () => {
    expect(helpSourceLink({file: 'docs/reference/config/templates.rst'}).href).toBe('https://lakehouse-plumber.readthedocs.io/en/latest/reference/config/templates.html')
    expect(helpSourceLink({file: 'src/lhp/resources/skills/lhp/references/actions-load-delta.md'}).href).toContain('/blob/release/V0.9.2/src/lhp/resources/skills/')
  })
})
