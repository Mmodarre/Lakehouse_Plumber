/**
 * Tests for the pure schema-help resolver (Task 3, Workstream B). Hand-crafted
 * fixtures mirror the real served-schema shapes ($ref chains, oneOf branches,
 * tuple/array items, patternProperties/additionalProperties) WITHOUT importing
 * the real src/lhp/schemas/*.json (cross-package). Each enumerated case in the
 * brief has a real assertion.
 */
import { describe, expect, it } from 'vitest'

import { buildSchemaHelpResolver } from '../schema-help'

describe('buildSchemaHelpResolver — 1. plain property', () => {
  const schema = {
    properties: {
      name: { type: 'string', description: 'The flowgroup name.' },
    },
  }

  it('resolves a top-level property description', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['name'])).toBe('The flowgroup name.')
  })
})

describe('buildSchemaHelpResolver — 2. nested $ref (local-wins terminal, deref descent)', () => {
  const schema = {
    properties: {
      write_target: {
        $ref: '#/definitions/WriteTarget',
        description: 'Write target configuration',
      },
    },
    definitions: {
      WriteTarget: {
        properties: {
          table: { type: 'string', description: 'Fully-qualified table name.' },
        },
      },
    },
  }

  it('local description wins for the terminal ref-bearing node', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['write_target'])).toBe('Write target configuration')
  })

  it('descends THROUGH the $ref to reach WriteTarget.table', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['write_target', 'table'])).toBe('Fully-qualified table name.')
  })
})

describe('buildSchemaHelpResolver — 3. oneOf branch DFS', () => {
  const schema = {
    properties: {
      source: {
        oneOf: [
          { type: 'string' },
          {
            type: 'object',
            properties: {
              format: { type: 'string', description: 'The source file format.' },
            },
          },
        ],
      },
    },
  }

  it('reaches a field inside the object branch of a oneOf', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['source', 'format'])).toBe('The source file format.')
  })
})

describe('buildSchemaHelpResolver — 4. array indices', () => {
  const schema = {
    properties: {
      cols: {
        type: 'array',
        items: { type: 'string', description: 'A column name.' },
      },
      pair: {
        type: 'array',
        prefixItems: [
          { type: 'string', description: 'Left element.' },
          { type: 'number', description: 'Right element.' },
        ],
      },
      tuple_items: {
        type: 'array',
        items: [
          { type: 'string', description: 'First tuple item.' },
          { type: 'number', description: 'Second tuple item.' },
        ],
      },
    },
  }

  it('single-schema items: numeric segment descends into items', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['cols', 0])).toBe('A column name.')
    // Any index maps to the single sub-schema.
    expect(resolve(['cols', 5])).toBe('A column name.')
  })

  it('prefixItems tuple: numeric segment indexes the positional schema', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['pair', 1])).toBe('Right element.')
  })

  it('legacy array-form items tuple: numeric segment indexes it', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['tuple_items', 1])).toBe('Second tuple item.')
  })
})

describe('buildSchemaHelpResolver — 5. patternProperties / additionalProperties', () => {
  it('falls through properties to a matching patternProperties entry', () => {
    const schema = {
      properties: {
        known: { type: 'string', description: 'A known property.' },
      },
      patternProperties: {
        '^x-': { type: 'string', description: 'An extension property.' },
      },
    }
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['known'])).toBe('A known property.')
    expect(resolve(['x-custom'])).toBe('An extension property.')
  })

  it('falls through to additionalProperties when it is an object schema', () => {
    const schema = {
      properties: {
        known: { type: 'string', description: 'A known property.' },
      },
      additionalProperties: {
        type: 'string',
        description: 'A free-form additional property.',
      },
    }
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['anything'])).toBe('A free-form additional property.')
  })
})

describe('buildSchemaHelpResolver — 6. missing paths → undefined', () => {
  const schema = {
    properties: {
      name: { type: 'string', description: 'The flowgroup name.' },
      plain: { type: 'string' },
    },
  }

  it('unknown segment returns undefined', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['nonexistent'])).toBeUndefined()
  })

  it('valid path whose terminal node has no description returns undefined', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['plain'])).toBeUndefined()
  })

  it('descending past a leaf that has no children returns undefined', () => {
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['plain', 'deeper'])).toBeUndefined()
  })
})

describe('buildSchemaHelpResolver — 7. $ref cycle safety', () => {
  const schema = {
    properties: {
      cyclic: { $ref: '#/definitions/A' },
    },
    definitions: {
      A: { $ref: '#/definitions/B' },
      B: { $ref: '#/definitions/A' },
    },
  }

  it('terminates on a mutually-recursive $ref cycle (mere completion proves it)', () => {
    const resolve = buildSchemaHelpResolver(schema)
    // No infinite loop; a cyclic chain with no reachable description → undefined.
    expect(resolve(['cyclic'])).toBeUndefined()
  })

  it('does not over-prune: a fresh cycle set per call still resolves siblings', () => {
    const withSibling = {
      properties: {
        cyclic: { $ref: '#/definitions/A' },
        good: { $ref: '#/definitions/Good' },
      },
      definitions: {
        A: { $ref: '#/definitions/B' },
        B: { $ref: '#/definitions/A' },
        Good: { description: 'A resolvable target.' },
      },
    }
    const resolve = buildSchemaHelpResolver(withSibling)
    expect(resolve(['cyclic'])).toBeUndefined()
    expect(resolve(['good'])).toBe('A resolvable target.')
  })
})

describe('buildSchemaHelpResolver — 8. custom root pointer', () => {
  const schema = {
    definitions: {
      PipelineSettings: {
        properties: {
          serverless: { type: 'boolean', description: 'Use serverless compute.' },
        },
      },
    },
  }

  it('resolves paths relative to a non-root pointer', () => {
    const resolve = buildSchemaHelpResolver(schema, '#/definitions/PipelineSettings')
    expect(resolve(['serverless'])).toBe('Use serverless compute.')
  })

  it('returns undefined when the custom root pointer does not resolve', () => {
    const resolve = buildSchemaHelpResolver(schema, '#/definitions/DoesNotExist')
    expect(resolve(['serverless'])).toBeUndefined()
  })
})

describe('buildSchemaHelpResolver — 9. cross-branch shared $ref (over-prune guard)', () => {
  // These fixtures prove the $ref-cycle `Set` is fresh PER resolveRefs CALL, not
  // hoisted to any wider (per-resolve-call or builder-closure) scope. The key
  // move: the shared def is followed ONCE during the path descent, THEN needed
  // again inside a `oneOf` branch. A per-call/closure-hoisted Set would already
  // contain the shared $ref by the time the deciding branch runs and would
  // wrongly prune it, collapsing the result to undefined. A genuine "would-fail-
  // if-hoisted" guard: mere completion is NOT enough — we assert the description.

  it('both oneOf branches $ref the same def already consumed by the descent', () => {
    // Literal reading of the brief: the oneOf has two sibling branches that BOTH
    // $ref the SAME definition (Shared), and Shared was already dereferenced once
    // on the way in (entry -> Shared -> branchpoint). Under correct per-call
    // semantics branch 1 re-derefs Shared freshly and yields `format`. Under a
    // hoisted Set, Shared is already "seen" so BOTH branches are pruned -> undefined.
    const schema = {
      properties: {
        entry: { $ref: '#/definitions/Shared' },
      },
      definitions: {
        Shared: {
          properties: {
            format: { description: 'The shared format field.' },
            branchpoint: {
              oneOf: [
                { $ref: '#/definitions/Shared' },
                { $ref: '#/definitions/Shared' },
              ],
            },
          },
        },
      },
    }
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['entry', 'branchpoint', 'format'])).toBe('The shared format field.')
  })

  it('first branch misses (different def); target is reached via the SECOND branch', () => {
    // "First branch must miss the segment for a different reason": branch 1 $refs
    // a DIFFERENT def (Miss) that has no `format`; branch 2 $refs the SAME Shared
    // def already consumed by the descent. Correct code re-derefs Shared on
    // branch 2 and returns `format`; a hoisted Set prunes branch 2 -> undefined.
    const schema = {
      properties: {
        entry: { $ref: '#/definitions/Shared' },
      },
      definitions: {
        Shared: {
          properties: {
            format: { description: 'The shared format field.' },
            branchpoint: {
              oneOf: [
                { $ref: '#/definitions/Miss' },
                { $ref: '#/definitions/Shared' },
              ],
            },
          },
        },
        Miss: {
          properties: {
            somethingelse: { description: 'Not the field we want.' },
          },
        },
      },
    }
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['entry', 'branchpoint', 'format'])).toBe('The shared format field.')
  })
})

describe('buildSchemaHelpResolver — 10. properties precedence over patternProperties', () => {
  it('a segment matching BOTH a properties key and a patternProperties regex uses properties', () => {
    const schema = {
      properties: {
        id: { description: 'The declared id property.' },
      },
      patternProperties: {
        '^id$': { description: 'The pattern-matched id (must NOT win).' },
        '.*': { description: 'Catch-all (must NOT win).' },
      },
    }
    const resolve = buildSchemaHelpResolver(schema)
    expect(resolve(['id'])).toBe('The declared id property.')
    // Sanity: a segment NOT in properties still falls through to patternProperties.
    expect(resolve(['other'])).toBe('Catch-all (must NOT win).')
  })
})
