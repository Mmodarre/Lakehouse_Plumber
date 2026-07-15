import { describe, expect, it } from 'vitest'
import { codeFieldForPath, fileRefForField } from '../codeFields'
import type { FieldSpec } from '../specs/types'

describe('fileRefForField', () => {
  it('returns the explicit fileRef when the spec declares one', () => {
    const spec: FieldSpec = {
      path: ['whatever'],
      label: 'Module',
      widget: 'text',
      fileRef: { accept: ['.py'] },
    }
    expect(fileRefForField(spec)).toEqual({ accept: ['.py'] })
  })

  it('preserves an explicit fileRef baseDir', () => {
    const spec: FieldSpec = {
      path: ['whatever'],
      label: 'Module',
      widget: 'text',
      fileRef: { accept: ['.py'], baseDir: 'src' },
    }
    expect(fileRefForField(spec)).toEqual({ accept: ['.py'], baseDir: 'src' })
  })

  it('falls back to the last-segment heuristic for a file-backed field', () => {
    const spec: FieldSpec = {
      path: ['source', 'sql_path'],
      label: 'SQL file',
      widget: 'text',
    }
    const ref = fileRefForField(spec)
    expect(ref).not.toBeNull()
    expect(ref?.accept).toEqual(['.sql'])
  })

  it('derives a python accept list from a module_path field', () => {
    const spec: FieldSpec = {
      path: ['source', 'module_path'],
      label: 'Module',
      widget: 'text',
    }
    expect(fileRefForField(spec)?.accept).toEqual(['.py'])
  })

  it('returns null for an inline code field (inline is not a file ref)', () => {
    const spec: FieldSpec = { path: ['sql'], label: 'SQL', widget: 'textarea' }
    expect(fileRefForField(spec)).toBeNull()
  })

  it('returns null for a plain text field with no heuristic and no fileRef', () => {
    const spec: FieldSpec = { path: ['target'], label: 'Target', widget: 'text' }
    expect(fileRefForField(spec)).toBeNull()
  })
})

describe('codeFieldForPath — schema-transform bodies', () => {
  it('resolves schema_inline to an inline yaml body', () => {
    expect(codeFieldForPath(['schema_inline'])).toEqual({
      backing: 'inline',
      inlineLanguage: 'yaml',
    })
  })

  it('resolves schema_file to a file-backed yaml body', () => {
    expect(codeFieldForPath(['schema_file'])).toEqual({
      backing: 'file',
      inlineLanguage: 'yaml',
    })
  })

  it('derives a yaml accept list from a schema_file field', () => {
    const spec: FieldSpec = {
      path: ['schema_file'],
      label: 'Schema file',
      widget: 'text',
    }
    expect(fileRefForField(spec)?.accept).toEqual(['.yaml', '.yml'])
  })
})
