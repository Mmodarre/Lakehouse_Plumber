import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { ApprovalCard } from '../ApprovalCard'

const { mutateMock } = vi.hoisted(() => ({ mutateMock: vi.fn() }))

vi.mock('../../../hooks/useAssistant', () => ({
  useResolveApproval: () => ({
    mutate: mutateMock,
    isPending: false,
    isError: false,
    error: null,
  }),
}))
vi.mock('../../../hooks/useProject', () => ({
  useHealth: () => ({ data: { root: '/proj' } }),
}))

function renderCard(params: Record<string, unknown>) {
  return render(
    <ApprovalCard elicitationId="e1" params={params} resolved={null} />,
  )
}

describe('ApprovalCard', () => {
  beforeEach(() => {
    mutateMock.mockClear()
  })

  it('Edit approval: registry header + old/new preview, no raw JSON', () => {
    renderCard({
      tool_name: 'Edit',
      message: 'Claude wants to use Edit',
      content_preview: JSON.stringify({
        file_path: '/proj/a.py',
        old_string: 'old code',
        new_string: 'new code',
      }),
    })
    expect(screen.getByText('Allow edit?')).toBeInTheDocument()
    expect(screen.getByText('a.py')).toBeInTheDocument()
    expect(screen.getByText('old code')).toBeInTheDocument()
    expect(screen.getByText('new code')).toBeInTheDocument()
    expect(screen.queryByText('Full request')).not.toBeInTheDocument()
  })

  it('Bash approval: command block, no raw JSON', () => {
    renderCard({
      tool_name: 'Bash',
      content_preview: JSON.stringify({
        command: 'lhp generate --env dev',
        description: 'Generate pipelines',
      }),
    })
    expect(screen.getByText('Allow command?')).toBeInTheDocument()
    expect(screen.getAllByText('lhp generate --env dev').length).toBeGreaterThan(0)
    expect(screen.queryByText('Full request')).not.toBeInTheDocument()
  })

  it('unknown tool keeps the raw-JSON full-request disclosure', () => {
    renderCard({
      tool_name: 'mcp__db__query',
      content_preview: JSON.stringify({ sql: 'select 1' }),
    })
    expect(screen.getByText('Full request')).toBeInTheDocument()
    expect(screen.getByText(/"sql": "select 1"/)).toBeInTheDocument()
  })

  it('known tool with an unparseable preview falls back to raw params', () => {
    renderCard({
      tool_name: 'Edit',
      content_preview: '{"file_path": "/proj/a.py", "old_string": "trunc',
    })
    expect(screen.getByText('Full request')).toBeInTheDocument()
  })

  it('no tool_name falls back to the elicitation message', () => {
    renderCard({ message: 'Run this?' })
    expect(screen.getByText('Run this?')).toBeInTheDocument()
    expect(screen.getByText('Full request')).toBeInTheDocument()
  })

  it('renders the always-allow button from the offer and posts the flag', () => {
    renderCard({
      tool_name: 'Bash',
      content_preview: JSON.stringify({ command: 'npm test -- --run' }),
      always_allow_offer: {
        tool: 'Bash',
        prefix: 'npm test',
        label: 'Always allow "npm test"',
      },
    })
    const button = screen.getByRole('button', {
      name: 'Always allow "npm test"',
    })
    fireEvent.click(button)
    expect(mutateMock).toHaveBeenCalledWith({
      elicitation_id: 'e1',
      action: 'accept',
      always_allow: true,
    })
  })

  it('plain Accept posts always_allow: false', () => {
    renderCard({
      tool_name: 'Bash',
      content_preview: JSON.stringify({ command: 'npm test' }),
      always_allow_offer: { tool: 'Bash', prefix: 'npm test', label: 'Always allow "npm test"' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Accept' }))
    expect(mutateMock).toHaveBeenCalledWith({
      elicitation_id: 'e1',
      action: 'accept',
      always_allow: false,
    })
  })

  it('no offer -> no always-allow button, standard actions only', () => {
    renderCard({
      tool_name: 'Bash',
      content_preview: JSON.stringify({ command: 'make' }),
    })
    expect(screen.queryByText(/Always allow/)).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Accept' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Decline' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })
})
