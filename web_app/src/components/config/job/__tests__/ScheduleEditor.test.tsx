import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { fetchMock, installRadixStubs, renderJobEditor, serveJob } from './jobFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── ScheduleEditor — exact splice in, pristine absence out ───
//
// Setting cron/timezone/pause_status on a doc with no `schedule` key must
// insert EXACTLY the schedule block (a byte-surgical splice at the end of
// the settings map); clearing all three must delete the `schedule` key
// entirely — no `schedule: {}` residue.

const FIXTURE = `project_defaults:
  max_concurrent_runs: 1
---
job_name: nightly
timeout_seconds: 600
`

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('ScheduleEditor', () => {
  it('setting all three fields splices exactly the schedule block', async () => {
    const { bufferContent } = serveJob(FIXTURE)
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    // Defaults doc is selected by default.
    await user.type(screen.getByLabelText('Cron expression'), '0 0 8 * * ?')
    await user.tab()
    await user.type(screen.getByLabelText('Time zone'), 'America/New_York')
    await user.tab()
    await user.click(screen.getByLabelText('Pause status'))
    await user.click(await screen.findByRole('option', { name: 'UNPAUSED' }))

    // The whole edit is ONE inserted block at the end of project_defaults;
    // every other byte (including the sibling document) is untouched.
    await waitFor(() =>
      expect(bufferContent()).toBe(
        'project_defaults:\n' +
          '  max_concurrent_runs: 1\n' +
          '  schedule:\n' +
          '    quartz_cron_expression: 0 0 8 * * ?\n' +
          '    timezone_id: America/New_York\n' +
          '    pause_status: UNPAUSED\n' +
          '---\n' +
          'job_name: nightly\n' +
          'timeout_seconds: 600\n',
      ),
    )
  })

  it('clearing all three fields deletes the schedule key (pristine absence)', async () => {
    const withSchedule =
      'project_defaults:\n' +
      '  max_concurrent_runs: 1\n' +
      '  schedule:\n' +
      '    quartz_cron_expression: 0 0 8 * * ?\n' +
      '    timezone_id: America/New_York\n' +
      '    pause_status: UNPAUSED\n' +
      '---\n' +
      'job_name: nightly\n' +
      'timeout_seconds: 600\n'
    const { bufferContent } = serveJob(withSchedule)
    renderJobEditor()
    await screen.findByRole('navigation', { name: 'Configuration documents' })
    const user = userEvent.setup()

    await user.clear(screen.getByLabelText('Cron expression'))
    await user.tab()
    await user.clear(screen.getByLabelText('Time zone'))
    await user.tab()
    await user.click(screen.getByLabelText('Pause status'))
    await user.click(await screen.findByRole('option', { name: 'Not set' }))

    // Nothing else changed: the file equals the original fixture.
    await waitFor(() => expect(bufferContent()).toBe(FIXTURE))
    expect(bufferContent()).not.toContain('schedule')
  })
})
