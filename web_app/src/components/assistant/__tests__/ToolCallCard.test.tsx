import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ToolCallCard } from '../ToolCallCard'

vi.mock('../../../hooks/useProject', () => ({
  useHealth: () => ({ data: { root: '/proj' } }),
}))

describe('ToolCallCard states', () => {
  it('running: spinner, header only (no details disclosure)', () => {
    render(
      <ToolCallCard
        item={{
          id: 't1',
          type: 'tool_call',
          name: 'Bash',
          status: 'running',
          arguments: { command: 'lhp generate', description: 'Generate' },
        }}
      />,
    )
    expect(screen.getByLabelText('running')).toBeInTheDocument()
    expect(screen.getByText('Generate')).toBeInTheDocument()
    expect(screen.getByText('lhp generate')).toBeInTheDocument()
    expect(screen.queryByText('Details')).not.toBeInTheDocument()
  })

  it('incomplete: neutral dash, no spinner, no details', () => {
    render(
      <ToolCallCard
        item={{
          id: 't2',
          type: 'tool_call',
          name: 'Read',
          status: 'incomplete',
          arguments: { file_path: '/proj/a.py' },
        }}
      />,
    )
    expect(screen.getByLabelText('incomplete')).toBeInTheDocument()
    expect(screen.queryByLabelText('running')).not.toBeInTheDocument()
    expect(screen.queryByText('Details')).not.toBeInTheDocument()
  })

  it('completed: check icon and registry body behind the disclosure', () => {
    render(
      <ToolCallCard
        item={{
          id: 't3',
          type: 'tool_call',
          name: 'Edit',
          status: 'completed',
          arguments: JSON.stringify({
            file_path: '/proj/a.py',
            old_string: 'old code',
            new_string: 'new code',
          }),
        }}
      />,
    )
    expect(screen.getByLabelText('completed')).toBeInTheDocument()
    expect(screen.getByText('Details')).toBeInTheDocument()
    expect(screen.getByText('old code')).toBeInTheDocument()
    expect(screen.getByText('new code')).toBeInTheDocument()
  })

  it('failed: error icon and the output preview', () => {
    render(
      <ToolCallCard
        item={{
          id: 't4',
          type: 'tool_call',
          name: 'Bash',
          status: 'failed',
          arguments: JSON.stringify({ command: 'exit 1' }),
          output_preview: 'boom',
        }}
      />,
    )
    expect(screen.getByLabelText('failed')).toBeInTheDocument()
    expect(screen.getByText('boom')).toBeInTheDocument()
  })

  it('unknown tool: raw-JSON fallback body', () => {
    render(
      <ToolCallCard
        item={{
          id: 't5',
          type: 'tool_call',
          name: 'mcp__db__query',
          status: 'completed',
          arguments: JSON.stringify({ sql: 'select 1' }),
        }}
      />,
    )
    expect(screen.getByText('mcp__db__query')).toBeInTheDocument()
    expect(screen.getByText(/"sql": "select 1"/)).toBeInTheDocument()
  })
})
