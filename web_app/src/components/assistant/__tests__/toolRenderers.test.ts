import { describe, expect, it } from 'vitest'
import { getToolRenderer } from '../toolRenderers'

const display = (name: string, args: Record<string, unknown>, root?: string) =>
  getToolRenderer(name).display(args, root)

describe('getToolRenderer — display()', () => {
  it('Bash: description as title, command as subtitle', () => {
    expect(
      display('Bash', { command: 'lhp generate --env dev', description: 'Generate' }),
    ).toEqual({ title: 'Generate', subtitle: 'lhp generate --env dev' })
    expect(display('Bash', { command: 'ls' })).toEqual({
      title: 'Command',
      subtitle: 'ls',
    })
  })

  it('Read / NotebookRead: shortened file path', () => {
    expect(display('Read', { file_path: '/proj/a.py' }, '/proj')).toEqual({
      title: 'Read',
      subtitle: 'a.py',
    })
    expect(
      display('NotebookRead', { notebook_path: '/proj/n.ipynb' }, '/proj'),
    ).toEqual({ title: 'Read', subtitle: 'n.ipynb' })
  })

  it('Edit family: shortened path per tool', () => {
    expect(display('Edit', { file_path: '/proj/c.py' }, '/proj')).toEqual({
      title: 'Edit',
      subtitle: 'c.py',
    })
    expect(display('MultiEdit', { file_path: '/proj/m.py' }, '/proj')).toEqual({
      title: 'Edit',
      subtitle: 'm.py',
    })
    expect(display('Write', { file_path: '/proj/w.py' }, '/proj')).toEqual({
      title: 'Write',
      subtitle: 'w.py',
    })
    expect(
      display('NotebookEdit', { notebook_path: '/proj/n.ipynb' }, '/proj'),
    ).toEqual({ title: 'Edit', subtitle: 'n.ipynb' })
  })

  it('Grep / Glob: pattern plus optional path', () => {
    expect(display('Grep', { pattern: 'cloudfiles' })).toEqual({
      title: 'Search',
      subtitle: 'cloudfiles',
    })
    expect(
      display('Grep', { pattern: 'cloudfiles', path: '/proj/pipelines' }, '/proj'),
    ).toEqual({ title: 'Search', subtitle: 'cloudfiles in pipelines' })
    expect(display('Glob', { pattern: '**/*.yaml' })).toEqual({
      title: 'Find files',
      subtitle: '**/*.yaml',
    })
  })

  it('web tools: target as subtitle', () => {
    expect(display('WebFetch', { url: 'https://x.test' }).subtitle).toBe(
      'https://x.test',
    )
    expect(display('WebSearch', { query: 'lhp docs' }).subtitle).toBe('lhp docs')
  })

  it('Task: description as title, subagent type as subtitle', () => {
    expect(
      display('Task', { description: 'Audit errors', subagent_type: 'reviewer' }),
    ).toEqual({ title: 'Audit errors', subtitle: 'reviewer' })
  })

  it('TodoWrite: item count', () => {
    expect(display('TodoWrite', { todos: [{}, {}] }).subtitle).toBe('2 items')
    expect(display('TodoWrite', { todos: [{}] }).subtitle).toBe('1 item')
  })

  it('unknown tools fall back to raw name + first string argument', () => {
    const renderer = getToolRenderer('mcp__db__query')
    expect(renderer.unknown).toBe(true)
    expect(renderer.Body).toBeUndefined()
    expect(renderer.display({ sql: 'select 1', limit: 5 })).toEqual({
      title: 'mcp__db__query',
      subtitle: 'select 1',
    })
    expect(getToolRenderer('Mystery').display({})).toEqual({
      title: 'Mystery',
      subtitle: null,
    })
  })

  it('known tools carry short labels for group summaries', () => {
    expect(getToolRenderer('Bash').label).toBe('Command')
    expect(getToolRenderer('Grep').label).toBe('Search')
    expect(getToolRenderer('Read').label).toBe('Read')
    expect(getToolRenderer('mcp__x').label).toBe('mcp__x')
  })
})
