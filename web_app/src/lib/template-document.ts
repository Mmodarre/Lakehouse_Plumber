import { parseAllDocuments, parseDocument, stringify, visit, isAlias } from 'yaml'
import {
  readFlowgroupValue, setFlowgroupField, deleteFlowgroupField, parseFlowgroupFile, selectTemplate, serializeFlowgroupFile,
  type FlowgroupDocHandle, type TemplateParamRead,
} from './flowgroup-doc'

export type TemplateValueResult = { ok: true; value: unknown } | { ok: false; error: string }
export function parseTemplateValue(text: string, format: 'string' | 'yaml'): TemplateValueResult {
  if (format === 'string') return { ok: true, value: text }
  if (!text.trim()) return { ok: false, error: 'Enter a YAML value. Use null for an explicit empty value, or "" for an empty string.' }
  try {
    const doc = parseDocument(text)
    if (doc.errors.length) return { ok: false, error: doc.errors[0].message.split('\n')[0] }
    const value: unknown = doc.toJS({ maxAliasCount: 20 })
    if (!jsonValue(value)) return { ok: false, error: 'Use a string, finite number, boolean, null, list, or mapping with string keys.' }
    return { ok: true, value }
  } catch (error) { return { ok: false, error: error instanceof Error ? error.message : 'Could not read this YAML value.' } }
}
function jsonValue(value: unknown, seen = new Set<object>()): boolean {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return true
  if (typeof value === 'number') return Number.isFinite(value)
  if (typeof value !== 'object' || seen.has(value)) return false
  seen.add(value)
  const valid = (Array.isArray(value) ? value : Object.values(value)).every((item) => jsonValue(item, seen))
  seen.delete(value)
  return valid
}
export function formatTemplateValue(value: unknown): string {
  return stringify(value, { lineWidth: 0 }).trimEnd()
}
export function initialParameterValue(type?: string): unknown {
  return type === 'number' ? 0 : type === 'boolean' ? false : type === 'array' ? [] : type === 'object' ? {} : ''
}
export function hasTemplateDefault(param: Pick<TemplateParamRead, 'raw'>): boolean {
  return Object.prototype.hasOwnProperty.call(param.raw, 'default')
}
export function setTemplateMetadata(body: FlowgroupDocHandle, field: 'name' | 'version' | 'description' | 'presets', value: unknown): void {
  setFlowgroupField(body, [field], value)
}
export function deleteTemplateMetadata(body: FlowgroupDocHandle, field: 'version' | 'description' | 'presets'): void {
  deleteFlowgroupField(body, [field])
}
export interface TemplateReference {
  path: (string | number)[]
  expression: string
  parameter?: string
  kind: 'direct' | 'complex' | 'block-only' | 'mapping-key'
}
/** Conservative inspection, never an expression parser or automatic rename engine. */
export function inspectTemplateReferences(body: FlowgroupDocHandle): TemplateReference[] {
  const references: TemplateReference[] = []
  const seen = new Set<object>()
  const walk = (value: unknown, path: (string | number)[]) => {
    if (value && typeof value === 'object') { if (seen.has(value)) return; seen.add(value) }
    if (typeof value === 'string') {
      for (const match of value.matchAll(/{{([\s\S]*?)}}/g)) {
        const expression = match[0]
        const identifier = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*$/.exec(match[1])?.[1]
        references.push({ path, expression, ...(identifier ? { parameter: identifier } : {}), kind: identifier ? 'direct' : 'complex' })
      }
      if (value.includes('{%') && !value.includes('{{')) references.push({ path, expression: value, kind: 'block-only' })
    } else if (Array.isArray(value)) value.forEach((item, index) => walk(item, [...path, index]))
    else if (value && typeof value === 'object') Object.entries(value).forEach(([key, item]) => {
      if (key.includes('{{') || key.includes('{%')) references.push({ path: [...path, key], expression: key, kind: 'mapping-key' })
      walk(item, [...path, key])
    })
  }
  walk(readFlowgroupValue(body, ['actions']), ['actions'])
  return references
}
export function referencesForParameter(references: readonly TemplateReference[], name: string): TemplateReference[] {
  return references.filter((ref) => ref.parameter === name || ref.kind === 'complex' && ref.expression.match(/[A-Za-z_][A-Za-z0-9_]*/g)?.includes(name))
}
export interface ParameterIssue { index: number; field: 'name' | 'type' | 'default'; message: string }
export function templateParameterIssues(params: readonly TemplateParamRead[]): ParameterIssue[] {
  const issues: ParameterIssue[] = []
  for (const param of params) {
    if (!param.name.trim()) issues.push({ index: param.index, field: 'name', message: 'Enter a parameter name.' })
    else if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(param.name)) issues.push({ index: param.index, field: 'name', message: 'Use letters, numbers and underscores, starting with a letter or underscore, to reference this input directly in Jinja.' })
    if (params.some((other) => other.index !== param.index && other.name === param.name)) issues.push({ index: param.index, field: 'name', message: `Parameter name '${param.name}' is declared more than once.` })
    if (param.type && !['string', 'number', 'boolean', 'object', 'array'].includes(param.type)) issues.push({ index: param.index, field: 'type', message: `Unrecognised advisory type '${param.type}'. Existing values are preserved.` })
  }
  return issues
}
/** Structural edits involving aliases can change other values through shared nodes. */
export function templateStructuredEditIssue(source: string): string | null {
  const docs = parseAllDocuments(source)
  if (docs.some((doc) => doc.errors.length)) return 'Fix YAML syntax errors in Code before using the builder.'
  if (docs.length !== 1) return 'The builder edits one template document at a time. Use Code for this multi-document file.'
  let sharedNodes = false
  visit(docs[0], (_key, node) => { if (isAlias(node) || node && typeof node === 'object' && 'anchor' in node && node.anchor) sharedNodes = true })
  return sharedNodes ? 'This template uses YAML anchors or aliases. Edit it in Code so shared values remain intact.' : null
}
export function templateReferenceForPath(path: string): string | null {
  return /^templates\/.+\.yaml$/.test(path) ? path.slice('templates/'.length, -'.yaml'.length) : null
}
export function validateTemplatePath(path: string, occupied: Iterable<string> = []): string | null {
  if (!/^templates\/(?:[A-Za-z0-9_-]+\/)*[A-Za-z0-9_-]+\.yaml$/.test(path)) return 'Use templates/name.yaml, with optional folders containing letters, numbers, underscores or hyphens.'
  if (new Set(occupied).has(path)) return 'This path already exists or has an open draft. Choose another name.'
  return null
}

export function buildTemplateDraft(name: string, duplicateYaml?: string): string {
  if (duplicateYaml === undefined) return stringify({ name, version: '1.0', description: '', parameters: [], actions: [] })
  const issue = templateStructuredEditIssue(duplicateYaml)
  if (issue) throw new Error(issue)
  const file = parseFlowgroupFile(duplicateYaml)
  const template = selectTemplate(file)
  if (!template) throw new Error('Choose a template definition to duplicate.')
  setTemplateMetadata(template.body, 'name', name)
  return serializeFlowgroupFile(file)
}
