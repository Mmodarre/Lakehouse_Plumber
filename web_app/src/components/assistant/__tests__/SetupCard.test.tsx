import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SetupCard } from '../SetupCard'
import {
  fetchPermissionsConfig,
  putPermissionsConfig,
} from '../../../api/assistant'

vi.mock('../../../api/assistant', () => ({
  fetchExecutorConfig: vi.fn().mockResolvedValue(null),
  putExecutorConfig: vi.fn(),
  fetchPricingConfig: vi.fn().mockResolvedValue({ models: {} }),
  putPricingConfig: vi.fn(),
  fetchPermissionsConfig: vi.fn(),
  putPermissionsConfig: vi.fn(),
  fetchDatabricksProfiles: vi.fn().mockResolvedValue({ profiles: [] }),
}))

const fetchPermissionsMock = vi.mocked(fetchPermissionsConfig)
const putPermissionsMock = vi.mocked(putPermissionsConfig)

const RULES = [
  { tool: 'WebFetch', prefix: null },
  { tool: 'Bash', prefix: 'npm test' },
]

function renderCard() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <SetupCard />
    </QueryClientProvider>,
  )
}

describe('SetupCard always-allow rules', () => {
  beforeEach(() => {
    fetchPermissionsMock.mockReset()
    putPermissionsMock.mockReset()
    fetchPermissionsMock.mockResolvedValue({ always_allow: RULES })
  })

  it('lists the stored rules once the section is opened', async () => {
    renderCard()
    // Collapsed: nothing fetched, nothing shown.
    expect(fetchPermissionsMock).not.toHaveBeenCalled()

    await userEvent.click(screen.getByText('Always-allow rules'))

    expect(await screen.findByText('WebFetch')).toBeInTheDocument()
    expect(screen.getByText('Bash: "npm test"')).toBeInTheDocument()
  })

  it('deleting a rule PUTs the filtered list', async () => {
    putPermissionsMock.mockResolvedValue({ always_allow: [RULES[0]] })
    renderCard()
    await userEvent.click(screen.getByText('Always-allow rules'))
    await screen.findByText('Bash: "npm test"')

    await userEvent.click(
      screen.getByRole('button', { name: 'Remove rule Bash: "npm test"' }),
    )

    await waitFor(() =>
      expect(putPermissionsMock).toHaveBeenCalledWith({
        always_allow: [{ tool: 'WebFetch', prefix: null }],
      }),
    )
  })

  it('shows an empty state when no rules are stored', async () => {
    fetchPermissionsMock.mockResolvedValue({ always_allow: [] })
    renderCard()
    await userEvent.click(screen.getByText('Always-allow rules'))

    expect(await screen.findByText('No rules yet.')).toBeInTheDocument()
  })
})
