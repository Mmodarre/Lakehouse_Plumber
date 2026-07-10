/**
 * sandbox.table_pattern validation (internal to the config-model layer).
 *
 * Mirrors `SandboxConfig._check_table_pattern` (src/lhp/models/_sandbox.py:
 * 34-76), which parses the pattern with Python's `string.Formatter().parse`
 * so the check matches exactly what `str.format` does at rewrite time.
 */

interface FormatSegment {
  literal: string
  field: string | null
  conversion: string | null
  spec: string | null
}

/**
 * First violation of the table_pattern rules, or null when valid:
 * placeholders must be a subset of {namespace, table} with BOTH present,
 * carry no conversion or format spec, and literal text is limited to
 * [A-Za-z0-9_] (note `{{`/`}}` escapes produce literal braces, which fail
 * that rule). Stops at the first violation, like the Pydantic validator.
 */
export function tablePatternViolation(pattern: string): string | null {
  let segments: FormatSegment[]
  try {
    segments = parsePythonFormat(pattern)
  } catch (parseError) {
    return `not a valid format string (${(parseError as Error).message})`
  }
  const seen = new Set<string>()
  for (const segment of segments) {
    if (!/^[A-Za-z0-9_]*$/.test(segment.literal)) {
      return `literal text may only contain letters, digits, and underscores; got '${segment.literal}'`
    }
    if (segment.field === null) continue
    if (segment.field !== 'namespace' && segment.field !== 'table') {
      return `placeholder {${segment.field}} is not recognized; only {namespace} and {table} are allowed`
    }
    if (segment.conversion !== null || (segment.spec !== null && segment.spec !== '')) {
      return `placeholder {${segment.field}} must be plain — conversions (e.g. !r) and format specs (e.g. :>10) are not allowed`
    }
    seen.add(segment.field)
  }
  const missing = ['namespace', 'table'].filter((placeholder) => !seen.has(placeholder))
  if (missing.length > 0) {
    return `table_pattern must contain ${missing.map((name) => `{${name}}`).join(', ')}`
  }
  return null
}

/**
 * Minimal re-implementation of Python's `string.Formatter().parse`: yields
 * (literal_text, field_name, format_spec, conversion) segments, with
 * `{{`/`}}` unescaped INTO the literal text (matching CPython). Throws on a
 * malformed pattern, exactly where Python raises ValueError.
 */
function parsePythonFormat(pattern: string): FormatSegment[] {
  const segments: FormatSegment[] = []
  let literal = ''
  let i = 0
  while (i < pattern.length) {
    const ch = pattern[i]
    if (ch === '{') {
      if (pattern[i + 1] === '{') {
        literal += '{'
        i += 2
        continue
      }
      let j = i + 1
      let field = ''
      while (j < pattern.length && pattern[j] !== '!' && pattern[j] !== ':' && pattern[j] !== '}') {
        field += pattern[j]
        j++
      }
      let conversion: string | null = null
      if (pattern[j] === '!') {
        conversion = pattern[j + 1] ?? ''
        j += 2
        if (conversion === '' || (pattern[j] !== ':' && pattern[j] !== '}')) {
          throw new Error('invalid conversion specifier')
        }
      }
      let spec: string | null = null
      if (pattern[j] === ':') {
        j++
        spec = ''
        let depth = 1 // format specs may nest one level of braces
        while (j < pattern.length) {
          const specChar = pattern[j]
          if (specChar === '{') depth++
          else if (specChar === '}') {
            depth--
            if (depth === 0) break
          }
          spec += specChar
          j++
        }
      }
      if (pattern[j] !== '}') throw new Error("expected '}' before end of string")
      segments.push({ literal, field, conversion, spec })
      literal = ''
      i = j + 1
      continue
    }
    if (ch === '}') {
      if (pattern[i + 1] === '}') {
        literal += '}'
        i += 2
        continue
      }
      throw new Error("single '}' encountered in format string")
    }
    literal += ch
    i++
  }
  if (literal !== '') {
    segments.push({ literal, field: null, conversion: null, spec: null })
  }
  return segments
}
