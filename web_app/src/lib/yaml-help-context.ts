import { isMap, isScalar, isSeq, parseAllDocuments } from 'yaml'
import type { SchemaKind } from '../api/schemas'
import type { SchemaPath } from './schema-help'

type Located = { path: (string | number)[]; start: number; end: number }
function locate(node: unknown, offset: number, path: (string | number)[] = []): Located | undefined {
  if (isMap(node)) {
    for (const item of node.items) {
      if (!isScalar(item.key) || typeof item.key.value !== 'string') continue
      const child = [...path, item.key.value]
      const range = item.key.range
      if (range && offset >= range[0] && offset <= range[1]) return { path: child, start: range[0], end: range[1] }
      const found = locate(item.value, offset, child)
      if (found) return found
    }
  } else if (isSeq(node)) {
    for (let i = 0; i < node.items.length; i++) {
      const found = locate(node.items[i], offset, [...path, i])
      if (found) return found
    }
  } else if (isScalar(node) && node.range && offset >= node.range[0] && offset <= node.range[1]) {
    return { path, start: node.range[0], end: node.range[1] }
  }
  return undefined
}
export interface YamlHelpContext { kind: SchemaKind; path: SchemaPath; subtype?: string; start: number; end: number }

/** Parse the current text, including multiple YAML documents, without mutating
 * it or evaluating template expressions. Unsafe/unknown shapes get no override. */
export function yamlHelpContext(filePath: string, text: string, offset: number): YamlHelpContext | undefined {
  const file = filePath.replace(/^\/+/, '')
  const kind: SchemaKind | undefined = /(^|\/)lhp\.ya?ml$/.test(file) ? 'project'
    : /(^|\/)config\/pipeline_config[^/]*\.ya?ml$/.test(file) ? 'pipeline_config'
    : /(^|\/)config\/(?:monitoring_)?job_config[^/]*\.ya?ml$/.test(file) ? 'job_config'
    : /(^|\/)templates\/.+\.ya?ml$/.test(file) ? 'template'
    : /(^|\/)pipelines\/.+\.ya?ml$/.test(file) ? 'flowgroup' : undefined
  if (!kind) return undefined
  try {
    for (const doc of parseAllDocuments(text)) {
      const found = locate(doc.contents, offset)
      if (!found) continue
      let path = found.path
      if (kind === 'pipeline_config' || kind === 'job_config') {
        if (path[0] === 'project_defaults') path = path.slice(1)
      }
      const actionIndex = path.lastIndexOf('actions')
      if ((kind === 'flowgroup' || kind === 'template') && actionIndex >= 0 && typeof path[actionIndex + 1] === 'number') {
        const prefix = path.slice(0, actionIndex + 2)
        const type = doc.getIn([...prefix, 'type'])
        const subtype = type === 'load' ? doc.getIn([...prefix, 'source', 'type'])
          : type === 'write' ? doc.getIn([...prefix, 'write_target', 'type'])
          : type === 'transform' ? doc.getIn([...prefix, 'transform_type'])
          : type === 'test' ? doc.getIn([...prefix, 'test_type']) : undefined
        if (typeof type !== 'string' || typeof subtype !== 'string') return undefined
        return { ...found, kind: 'flowgroup', path: path.slice(actionIndex + 2), subtype: `${type}:${subtype}` }
      }
      return { ...found, kind, path }
    }
  } catch { /* Invalid source stays editable with the standard YAML service. */ }
  return undefined
}
