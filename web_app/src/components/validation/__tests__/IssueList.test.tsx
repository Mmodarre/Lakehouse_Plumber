import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
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
    expect(screen.getAllByText('Missing target table').length).toBeGreaterThan(0)
    expect(screen.getByText('customers.yaml:12')).toBeInTheDocument()
    expect(screen.getByText('DEP-002')).toBeInTheDocument()
    expect(screen.getAllByText('Unresolvable reference').length).toBeGreaterThan(0)
  })

  it('filters severity and full file paths without changing onSelect source indexes', () => {
    const onSelect = vi.fn()
    const diagnosticRows = [...issues, { ...issues[0], code: 'OTHER', file: 'pipelines/silver/orders.yaml', message: 'Order problem' }]
    render(<IssueList issues={diagnosticRows} filterable onSelect={onSelect} />)
    fireEvent.change(screen.getByRole('combobox', { name: 'Issue severity' }), { target: { value: 'error' } })
    fireEvent.change(screen.getByRole('searchbox', { name: 'Filter issues by file' }), { target: { value: 'SILVER/' } })
    expect(screen.queryByText('DEP-002')).not.toBeInTheDocument()
    expect(screen.queryByText('LHP-VAL-001')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /Order problem/ }))
    expect(onSelect).toHaveBeenCalledWith(diagnosticRows[2], 2)
    fireEvent.change(screen.getByRole('combobox', { name: 'Issue severity' }), { target: { value: 'warning' } })
    expect(screen.getByText('No issues match these filters.')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }))
    expect(screen.getByText('DEP-002')).toBeInTheDocument()
  })

  it('exposes full diagnostic details and suggested fixes independently of source opening', async () => {
    const user = userEvent.setup()
    render(<IssueList issues={[{ ...issues[0], details: 'Target is missing from the project', suggestions: ['Add a write action'] }]} />)
    await user.click(screen.getByText(/Details · pipelines/))
    expect(screen.getByText('Target is missing from the project')).toBeVisible()
    expect(screen.getByText('Add a write action')).toBeVisible()
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
