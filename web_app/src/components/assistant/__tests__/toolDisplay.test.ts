import { describe, expect, it } from 'vitest'
import { parseToolArguments, shortenPath } from '../toolDisplay'

describe('parseToolArguments', () => {
  it('parses the arguments JSON string of an item.done tool_call', () => {
    expect(parseToolArguments({ arguments: '{"file_path":"/a/b.py"}' })).toEqual({
      file_path: '/a/b.py',
    })
  })

  it('passes through the already-decoded object of an item.started', () => {
    expect(parseToolArguments({ arguments: { command: 'ls' } })).toEqual({
      command: 'ls',
    })
  })

  it.each([
    [{ arguments: 'not json{' }],
    [{ arguments: '"a plain string"' }],
    [{ arguments: ['not', 'a', 'dict'] }],
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
