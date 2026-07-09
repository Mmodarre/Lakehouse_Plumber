import { describe, expect, it } from 'vitest'
import { parseToolArguments, shortenPath, toolDisplay } from '../toolDisplay'

describe('parseToolArguments', () => {
  it('parses the arguments JSON string of a tool_call item', () => {
    expect(parseToolArguments({ arguments: '{"file_path":"/a/b.py"}' })).toEqual({
      file_path: '/a/b.py',
    })
  })

  it.each([
    [{ arguments: 'not json{' }],
    [{ arguments: '"a plain string"' }],
    [{}],
  ])('degrades to an empty object on %j', (item) => {
    expect(parseToolArguments(item)).toEqual({})
  })
})

describe('shortenPath', () => {
  it('relativizes paths under the project root', () => {
    expect(shortenPath('/proj/pipelines/x.yaml', '/proj')).toBe('pipelines/x.yaml')
    expect(shortenPath('/proj/pipelines/x.yaml', '/proj/')).toBe('pipelines/x.yaml')
  })

  it('leaves outside paths and missing roots untouched', () => {
    expect(shortenPath('/etc/hosts', '/proj')).toBe('/etc/hosts')
    expect(shortenPath('/proj/x.yaml', null)).toBe('/proj/x.yaml')
  })
})

describe('toolDisplay', () => {
  it('summarizes file tools by their (shortened) path', () => {
    expect(toolDisplay('Read', { file_path: '/proj/a.py' }, '/proj')).toEqual({
      label: 'Read',
      detail: 'a.py',
    })
    expect(toolDisplay('Write', { file_path: '/proj/b.py' }, '/proj')).toEqual({
      label: 'Write',
      detail: 'b.py',
    })
    expect(toolDisplay('Edit', { file_path: '/proj/c.py' }, '/proj')).toEqual({
      label: 'Edit',
      detail: 'c.py',
    })
  })

  it('summarizes Bash by its command', () => {
    expect(toolDisplay('Bash', { command: 'lhp generate --env dev' })).toEqual({
      label: 'Command',
      detail: 'lhp generate --env dev',
    })
  })

  it('summarizes search tools by pattern and web tools by target', () => {
    expect(toolDisplay('Grep', { pattern: 'cloudfiles' }).detail).toBe('cloudfiles')
    expect(toolDisplay('Glob', { pattern: '**/*.yaml' }).detail).toBe('**/*.yaml')
    expect(toolDisplay('WebFetch', { url: 'https://x.test' }).detail).toBe(
      'https://x.test',
    )
  })

  it('falls back to the raw tool name and first string argument', () => {
    expect(toolDisplay('mcp__db__query', { sql: 'select 1', limit: 5 })).toEqual({
      label: 'mcp__db__query',
      detail: 'select 1',
    })
    expect(toolDisplay('Mystery', {})).toEqual({ label: 'Mystery', detail: null })
  })
})
