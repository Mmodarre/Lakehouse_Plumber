import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { IssueList } from '../IssueList'
import type { IssueListItem } from '../IssueList'

const issues: IssueListItem[] = [
  {
    severity: 'error',
    code: 'LHP-VAL-001',
    message: 'Missing target table',
    file: 'pipelines/bronze/customers.yaml',
    line: 12,
  },
  {
    severity: 'warning',
    code: 'DEP-002',
    message: 'Unresolvable reference',
    file: null,
    line: null,
  },
]

describe('IssueList', () => {
  it('renders nothing for an empty issue list', () => {
    const { container } = render(<IssueList issues={[]} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('renders code, message, and file:line location per row', () => {
    render(<IssueList issues={issues} />)
    expect(screen.getByText('LHP-VAL-001')).toBeInTheDocument()
    expect(screen.getByText('Missing target table')).toBeInTheDocument()
    expect(screen.getByText('customers.yaml:12')).toBeInTheDocument()
    expect(screen.getByText('DEP-002')).toBeInTheDocument()
    expect(screen.getByText('Unresolvable reference')).toBeInTheDocument()
  })

  it('renders static rows (no buttons) without onSelect', () => {
    render(<IssueList issues={issues} />)
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
  })

  it('renders button rows and fires onSelect with the issue and index', async () => {
    const user = userEvent.setup()
    const onSelect = vi.fn()
    render(<IssueList issues={issues} onSelect={onSelect} />)

    const buttons = screen.getAllByRole('button')
    expect(buttons).toHaveLength(2)
    await user.click(buttons[1])
    expect(onSelect).toHaveBeenCalledWith(issues[1], 1)
  })
})
