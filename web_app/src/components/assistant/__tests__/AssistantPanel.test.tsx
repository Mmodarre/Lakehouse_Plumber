import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import AssistantPanel from '../AssistantPanel'
import { useAssistantStore } from '../../../store/assistantStore'
import {
  fetchAssistantStatus,
  fetchExecutorConfig,
  installAssistantSkill,
  putExecutorConfig,
  resolveAssistantApproval,
  startAssistantDaemon,
} from '../../../api/assistant'
import type { AssistantStatus } from '../../../types/assistant'

vi.mock('../../../api/assistant', () => ({
  fetchAssistantStatus: vi.fn(),
  fetchExecutorConfig: vi.fn().mockResolvedValue(null),
  putExecutorConfig: vi.fn(),
  fetchPricingConfig: vi.fn().mockResolvedValue({ models: {} }),
  putPricingConfig: vi.fn(),
  fetchPermissionsConfig: vi.fn().mockResolvedValue({ always_allow: [] }),
  putPermissionsConfig: vi.fn(),
  fetchDatabricksProfiles: vi.fn().mockResolvedValue({ profiles: [] }),
  installAssistantSkill: vi.fn(),
  startAssistantDaemon: vi.fn(),
  resolveAssistantApproval: vi.fn(),
  interruptAssistant: vi.fn(),
  fetchAssistantSession: vi.fn(),
  fetchAssistantSessions: vi.fn().mockResolvedValue({ sessions: [], total: 0 }),
  newAssistantSession: vi.fn(),
  archiveAssistantSession: vi.fn(),
  startAssistantChat: vi.fn(),
}))

const statusMock = vi.mocked(fetchAssistantStatus)
const configMock = vi.mocked(fetchExecutorConfig)
const putConfigMock = vi.mocked(putExecutorConfig)
const installMock = vi.mocked(installAssistantSkill)
const daemonStartMock = vi.mocked(startAssistantDaemon)
const approvalMock = vi.mocked(resolveAssistantApproval)

/** All-green status; override per gate under test. */
function statusOf(over: Partial<AssistantStatus> = {}): AssistantStatus {
  return {
    binary_found: true,
    server_ok: true,
    host_online: true,
    host_id: 'host-1',
    server_url: 'http://127.0.0.1:6767',
    skill_installed: true,
    skill_version: '0.9.1',
    executor_configured: true,
    active_session: null,
    provider: 'omnigent',
    ...over,
  }
}

function renderPanel() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<AssistantPanel />, { wrapper })
}

function resetStore() {
  useAssistantStore.setState({
    conversations: {},
    tabOrder: [],
    activeTabKey: null,
    tabTitles: {},
    nextDraftId: 1,
    panelOpen: true,
    permissionMode: 'default',
  })
}

/** Fold a frame into the ACTIVE tab (the panel boot opens one). */
function applyToActive(frame: Parameters<
  ReturnType<typeof useAssistantStore.getState>['applyFrame']
>[1]) {
  const s = useAssistantStore.getState()
  if (s.activeTabKey === null) throw new Error('no active tab to apply to')
  s.applyFrame(s.activeTabKey, frame)
}

function activeConversation() {
  const s = useAssistantStore.getState()
  return s.activeTabKey !== null ? s.conversations[s.activeTabKey] : undefined
}

beforeEach(() => {
  vi.clearAllMocks()
  resetStore()
})

describe('AssistantPanel switchboard', () => {
  it('binary missing → install instructions with the uv command and docs link', async () => {
    statusMock.mockResolvedValue(statusOf({ binary_found: false, server_ok: false, host_online: false, host_id: null }))
    renderPanel()

    expect(await screen.findByText('omnigent is not installed')).toBeInTheDocument()
    expect(screen.getByText('uv tool install omnigent')).toBeInTheDocument()
    expect(
      screen.getByRole('link', { name: /omnigent documentation/i }),
    ).toBeInTheDocument()
    // Gated: no composer.
    expect(screen.queryByLabelText('Chat message')).not.toBeInTheDocument()
  })

  it('server down → "Start it for me" fires the daemon-start mutation', async () => {
    const user = userEvent.setup()
    statusMock.mockResolvedValue(statusOf({ server_ok: false, host_online: false, host_id: null }))
    daemonStartMock.mockResolvedValue({ started: true, detail: null })
    renderPanel()

    expect(
      await screen.findByText('The omnigent server is not running'),
    ).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /start it for me/i }))
    await waitFor(() => expect(daemonStartMock).toHaveBeenCalledTimes(1))
  })

  it('host offline → its own ladder rung', async () => {
    statusMock.mockResolvedValue(statusOf({ host_online: false, host_id: null }))
    renderPanel()

    expect(await screen.findByText('No omnigent host is online')).toBeInTheDocument()
  })

  it('skill not installed → install card fires the skill mutation', async () => {
    const user = userEvent.setup()
    statusMock.mockResolvedValue(
      statusOf({ skill_installed: false, skill_version: null }),
    )
    installMock.mockResolvedValue({
      install_dir: '/tmp/p/.claude/skills/lhp',
      skill_version: '0.9.1',
      action: 'installed',
    })
    renderPanel()

    expect(await screen.findByText('Install the LHP skill')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /install skill/i }))
    await waitFor(() => expect(installMock).toHaveBeenCalledTimes(1))
    expect(screen.queryByLabelText('Chat message')).not.toBeInTheDocument()
  })

  it('executor unconfigured → SetupCard blocks the composer', async () => {
    statusMock.mockResolvedValue(statusOf({ executor_configured: false }))
    renderPanel()

    expect(
      await screen.findByText('Choose how the assistant runs'),
    ).toBeInTheDocument()
    expect(screen.queryByLabelText('Chat message')).not.toBeInTheDocument()
  })

  it('fully ready → chat view with the composer', async () => {
    statusMock.mockResolvedValue(statusOf())
    renderPanel()

    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /send message/i })).toBeInTheDocument()
  })

  it('claude provider ready → chat view, no omnigent daemon gate', async () => {
    statusMock.mockResolvedValue(
      statusOf({
        provider: 'claude_sdk',
        host_id: 'local',
        server_url: '',
        server_ok: true,
        host_online: true,
      }),
    )
    renderPanel()

    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
    expect(
      screen.queryByText('omnigent is not installed'),
    ).not.toBeInTheDocument()
  })

  it('claude provider with SDK binary missing → ClaudeGate reinstall card', async () => {
    statusMock.mockResolvedValue(
      statusOf({
        provider: 'claude_sdk',
        binary_found: false,
        server_ok: false,
        host_online: false,
        host_id: null,
        server_url: '',
      }),
    )
    renderPanel()

    expect(
      await screen.findByText('Claude runtime unavailable'),
    ).toBeInTheDocument()
    expect(screen.queryByLabelText('Chat message')).not.toBeInTheDocument()
  })

  it('gear button opens the setup card prefilled; Cancel returns to chat', async () => {
    const user = userEvent.setup()
    statusMock.mockResolvedValue(statusOf())
    configMock.mockResolvedValue({
      provider: 'omnigent' as const,
      mode: 'databricks' as const,
      profile: 'field-enf',
      host: null,
      model: null,
      api_key_env: null,
      oauth_token_env: null,
    })
    renderPanel()

    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /assistant settings/i }))

    expect(
      await screen.findByText('Choose how the assistant runs'),
    ).toBeInTheDocument()
    // Prefilled from the stored config: the mode select shows the label.
    expect(screen.getByLabelText('Auth mode')).toHaveTextContent(
      'Databricks workspace',
    )

    await user.click(screen.getByRole('button', { name: /^cancel$/i }))
    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
    expect(putConfigMock).not.toHaveBeenCalled()
  })

  it('saving a changed executor from the gear card PUTs and returns to chat', async () => {
    const user = userEvent.setup()
    statusMock.mockResolvedValue(statusOf())
    configMock.mockResolvedValue({
      provider: 'omnigent' as const,
      mode: 'databricks' as const,
      profile: 'field-enf',
      host: null,
      model: null,
      api_key_env: null,
      oauth_token_env: null,
    })
    putConfigMock.mockResolvedValue({
      provider: 'omnigent' as const,
      mode: 'databricks' as const,
      profile: 'field-enf',
      host: null,
      model: null,
      api_key_env: null,
      oauth_token_env: null,
    })
    renderPanel()

    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /assistant settings/i }))
    await screen.findByText('Choose how the assistant runs')

    await user.click(screen.getByRole('button', { name: /^save$/i }))

    await waitFor(() => expect(putConfigMock).toHaveBeenCalledTimes(1))
    expect(putConfigMock).toHaveBeenCalledWith({
      provider: 'omnigent' as const,
      mode: 'databricks' as const,
      profile: 'field-enf',
      host: null,
      model: null,
      api_key_env: null,
      oauth_token_env: null,
    })
    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
  })

  it('status endpoint unreachable → in-panel retry card (never toast-only)', async () => {
    statusMock.mockRejectedValue(new Error('ECONNREFUSED'))
    renderPanel()

    // The hook retries once (retry: 1) before surfacing the error, so
    // allow for the ~1s retry delay.
    expect(
      await screen.findByText('Assistant status unavailable', undefined, {
        timeout: 5_000,
      }),
    ).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
  })

  it('binary missing but server up → chat renders, NOT the install gate', async () => {
    // isDaemonReady deliberately ignores binary_found: a daemon run from a
    // venv (binary off PATH) is fully usable — only spawning needs the binary.
    statusMock.mockResolvedValue(statusOf({ binary_found: false }))
    renderPanel()

    expect(await screen.findByLabelText('Chat message')).toBeInTheDocument()
    expect(
      screen.queryByText('omnigent is not installed'),
    ).not.toBeInTheDocument()
  })

  it('binary missing with host offline → host rung with manual commands, no start button', async () => {
    statusMock.mockResolvedValue(
      statusOf({ binary_found: false, host_online: false, host_id: null }),
    )
    renderPanel()

    expect(await screen.findByText('No omnigent host is online')).toBeInTheDocument()
    // "Start it for me" needs the binary; the manual fallback shows instead
    // (immediately, not after the 15s poll fallback).
    expect(
      screen.queryByRole('button', { name: /start it for me/i }),
    ).not.toBeInTheDocument()
    expect(screen.getByText(/omnigent server start/)).toBeInTheDocument()
  })

  it.each([
    ['decline', /declined/i],
    ['cancel', /cancelled/i],
  ] as const)(
    'approval card %s fires the mutation and marks it resolved',
    async (action, resolvedCopy) => {
      const user = userEvent.setup()
      statusMock.mockResolvedValue(statusOf())
      approvalMock.mockResolvedValue({ message: 'Approval resolved', details: null })
      renderPanel()
      await screen.findByLabelText('Chat message')

      applyToActive({
        type: 'approval.request',
        elicitation_id: 'e2',
        params: { message: 'Edit this file?' },
      })

      expect(await screen.findByText('Edit this file?')).toBeInTheDocument()
      await user.click(
        screen.getByRole('button', { name: new RegExp(`^${action}$`, 'i') }),
      )

      await waitFor(() =>
        expect(approvalMock).toHaveBeenCalledExactlyOnceWith({
          elicitation_id: 'e2',
          action,
          always_allow: false,
        }),
      )
      expect(await screen.findByText(resolvedCopy)).toBeInTheDocument()
      expect(activeConversation()?.pendingApproval).toBeNull()
    },
  )

  it.each([
    ['omnigent_setup', /omnigent setup/],
    ['databricks_auth', /databricks auth login --profile/],
  ] as const)(
    'session.failed %s hint renders its remediation copy',
    async (hint, copy) => {
      statusMock.mockResolvedValue(statusOf())
      renderPanel()
      await screen.findByLabelText('Chat message')

      applyToActive({
        type: 'session.failed',
        detail: 'runner_error: boom',
        hint,
      })

      expect(await screen.findByText('Assistant session failed')).toBeInTheDocument()
      expect(screen.getByText(copy)).toBeInTheDocument()
    },
  )

  it('session.failed unknown hint falls back to the raw detail', async () => {
    statusMock.mockResolvedValue(statusOf())
    renderPanel()
    await screen.findByLabelText('Chat message')

    applyToActive({
      type: 'session.failed',
      detail: 'disk exploded',
      hint: 'unknown',
    })

    expect(await screen.findByText('Assistant session failed')).toBeInTheDocument()
    expect(screen.getByText('disk exploded')).toBeInTheDocument()
  })

  it('composer permission-mode selector reflects the store', async () => {
    statusMock.mockResolvedValue(statusOf())
    renderPanel()
    await screen.findByLabelText('Chat message')

    const trigger = screen.getByRole('combobox', { name: /permission mode/i })
    expect(trigger).toHaveTextContent('Ask every time')

    useAssistantStore.getState().setPermissionMode('bypassPermissions')
    await waitFor(() => expect(trigger).toHaveTextContent('Allow all'))
  })

  it('approval card actions fire the approval mutation and mark it resolved', async () => {
    const user = userEvent.setup()
    statusMock.mockResolvedValue(statusOf())
    approvalMock.mockResolvedValue({ message: 'Approval resolved', details: null })
    renderPanel()
    await screen.findByLabelText('Chat message')

    applyToActive({
      type: 'approval.request',
      elicitation_id: 'e1',
      params: { message: 'Run lhp generate?', phase: 'execute' },
    })

    expect(await screen.findByText('Run lhp generate?')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /accept/i }))

    await waitFor(() =>
      expect(approvalMock).toHaveBeenCalledExactlyOnceWith({
        elicitation_id: 'e1',
        action: 'accept',
        always_allow: false,
      }),
    )
    // onSuccess marks the part resolved and clears the pending approval.
    expect(await screen.findByText(/accepted/i)).toBeInTheDocument()
    expect(activeConversation()?.pendingApproval).toBeNull()
  })
})
