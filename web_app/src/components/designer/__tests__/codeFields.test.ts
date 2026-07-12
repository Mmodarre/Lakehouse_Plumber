import { describe, expect, it } from 'vitest'
import { codeFieldForPath } from '../codeFields'

describe('codeFieldForPath', () => {
  it('marks inline SQL bodies', () => {
    expect(codeFieldForPath(['sql'])).toEqual({ backing: 'inline', inlineLanguage: 'sql' })
    expect(codeFieldForPath(['write_target', 'sql'])).toEqual({
      backing: 'inline',
      inlineLanguage: 'sql',
    })
  })

  it('marks file-backed refs by their last segment', () => {
    expect(codeFieldForPath(['sql_path'])?.backing).toBe('file')
    expect(codeFieldForPath(['source', 'sql_path'])?.backing).toBe('file')
    expect(codeFieldForPath(['source', 'module_path'])).toEqual({
      backing: 'file',
      inlineLanguage: 'python',
    })
    expect(codeFieldForPath(['expectations_file'])?.backing).toBe('file')
  })

  it('returns null for non-code fields', () => {
    expect(codeFieldForPath(['target'])).toBeNull()
    expect(codeFieldForPath(['source', 'format'])).toBeNull()
    expect(codeFieldForPath([])).toBeNull()
    expect(codeFieldForPath(['tags', 0])).toBeNull()
  })
})
