/**
 * Pure schema-help resolver (Task 3, Workstream B). Given ONE served JSON
 * schema document and a starting JSON-pointer root, resolve a field path to
 * that field's `description` string. Monaco hover and the "(i)" tooltips both
 * read the same document through this resolver, so field help cannot drift.
 *
 * No I/O, no React, and no upward coupling to components/** — `SchemaPath` is
 * defined locally (structurally compatible with the designer's YamlPath).
 */

export type SchemaDoc = Record<string, unknown>
export type SchemaPath = readonly (string | number)[]

/** A schema node is a plain (non-array) object; arrays only appear as values. */
type SchemaNode = Record<string, unknown>

function isObject(value: unknown): value is SchemaNode {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** JSON-pointer segment unescape: `~1` → `/`, `~0` → `~` (order matters). */
function unescapePointerSegment(segment: string): string {
  return segment.replace(/~1/g, '/').replace(/~0/g, '~')
}

/**
 * Build a resolver over one schema document. `root` is a JSON pointer to the
 * node the paths are relative to (default `'#'` = document root). The returned
 * fn maps a field path to that field's `description`, or `undefined` if the
 * path or description is absent.
 */
export function buildSchemaHelpResolver(
  schema: SchemaDoc,
  root?: string,
): (path: SchemaPath) => string | undefined {
  /** Resolve a JSON pointer against `schema`; `undefined` if it dead-ends. */
  function derefPointer(pointer: string): unknown {
    const withoutHash = pointer.startsWith('#') ? pointer.slice(1) : pointer
    if (withoutHash === '') return schema
    const rawSegments = withoutHash.split('/')
    // A valid pointer starts with '/', so the first split token is empty.
    const segments = rawSegments
      .slice(rawSegments[0] === '' ? 1 : 0)
      .map(unescapePointerSegment)

    let node: unknown = schema
    for (const segment of segments) {
      if (Array.isArray(node)) {
        node = node[Number(segment)]
      } else if (isObject(node)) {
        node = node[segment]
      } else {
        return undefined
      }
      if (node === undefined) return undefined
    }
    return node
  }

  /**
   * Follow a `$ref` chain to a concrete node. Cycle-safe: a LOCAL set of the
   * `$ref` strings seen in THIS chain stops a repeat (returning the current
   * node). Fresh set per call, so cross-branch repeats are not over-pruned.
   */
  function resolveRefs(node: unknown): unknown {
    const seen = new Set<string>()
    let current: unknown = node
    while (isObject(current) && typeof current.$ref === 'string') {
      const ref = current.$ref
      if (seen.has(ref)) return current
      seen.add(ref)
      const target = derefPointer(ref)
      if (target === undefined) return current
      current = target
    }
    return current
  }

  function directChild(node: unknown, segment: string | number): unknown {
    if (!isObject(node)) return undefined

    if (typeof segment === 'number') {
      const prefixItems = node.prefixItems
      if (Array.isArray(prefixItems) && prefixItems[segment] !== undefined) {
        return prefixItems[segment]
      }
      const items = node.items
      if (Array.isArray(items)) return items[segment]
      if (isObject(items)) return items
      return undefined
    }

    const properties = node.properties
    if (isObject(properties) && properties[segment] !== undefined) {
      return properties[segment]
    }

    const patternProperties = node.patternProperties
    if (isObject(patternProperties)) {
      for (const key of Object.keys(patternProperties)) {
        let pattern: RegExp
        try {
          pattern = new RegExp(key)
        } catch {
          continue
        }
        if (pattern.test(segment)) return patternProperties[key]
      }
    }

    const additionalProperties = node.additionalProperties
    if (isObject(additionalProperties)) return additionalProperties

    return undefined
  }

  function stepInto(node: unknown, segment: string | number): unknown {
    const resolved = resolveRefs(node)

    const direct = directChild(resolved, segment)
    if (direct !== undefined) return direct

    if (isObject(resolved)) {
      for (const combinator of ['oneOf', 'anyOf', 'allOf'] as const) {
        const branches = resolved[combinator]
        if (Array.isArray(branches)) {
          for (const branch of branches) {
            const found = stepInto(branch, segment)
            if (found !== undefined) return found
          }
        }
      }
    }

    return undefined
  }

  return (path: SchemaPath): string | undefined => {
    let node: unknown = derefPointer(root ?? '#')
    if (node === undefined) return undefined

    for (const segment of path) {
      node = stepInto(node, segment)
      if (node === undefined) return undefined
    }

    // Terminal: a local description wins over the $ref target's description.
    if (isObject(node) && typeof node.description === 'string') {
      return node.description
    }
    const resolved = resolveRefs(node)
    if (isObject(resolved) && typeof resolved.description === 'string') {
      return resolved.description
    }
    return undefined
  }
}
