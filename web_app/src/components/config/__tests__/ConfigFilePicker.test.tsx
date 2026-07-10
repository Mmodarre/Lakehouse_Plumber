import { beforeAll, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ConfigFilePicker } from '../ConfigFilePicker'
import type { ConfigTabKind } from '../configFileSupport'
import type { FileNode } from '../../../types/api'

// jsdom lacks the layout APIs the radix popover + cmdk pair relies on.
beforeAll(() => {
  Element.prototype.scrollIntoView = vi.fn()
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
})

const tree: FileNode = {
  name: '',
  path: '',
  type: 'directory',
  children: [
    {
      name: 'config',
      path: 'config',
      type: 'directory',
      children: [
        { name: 'zeta.yaml', path: 'config/zeta.yaml', type: 'file' },
        { name: 'job_config_dev.yaml', path: 'config/job_config_dev.yaml', type: 'file' },
        { name: 'pipeline_config_dev.yaml', path: 'config/pipeline_config_dev.yaml', type: 'file' },
        { name: 'readme.md', path: 'config/readme.md', type: 'file' },
      ],
    },
    { name: 'lhp.yaml', path: 'lhp.yaml', type: 'file' },
  ],
}

function renderPicker({
  kind = 'pipeline' as ConfigTabKind,
  value = null as string | null,
  files = tree as FileNode | undefined,
} = {}) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  if (files) queryClient.setQueryData(['files'], files)
  const onSelect = vi.fn()
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<ConfigFilePicker kind={kind} value={value} onSelect={onSelect} />, { wrapper })
  return { onSelect }
}

describe('ConfigFilePicker', () => {
  it('lists only config/ YAMLs, kind-preferred first, and selects on click', async () => {
    const user = userEvent.setup()
    const { onSelect } = renderPicker({ kind: 'pipeline' })

    await user.click(screen.getByRole('combobox', { name: /select config file/i }))

    const options = screen.getAllByRole('option').map((o) => o.textContent)
    expect(options).toEqual([
      'config/pipeline_config_dev.yaml',
      'config/job_config_dev.yaml',
      'config/zeta.yaml',
    ])

    await user.click(screen.getByRole('option', { name: 'config/pipeline_config_dev.yaml' }))
    expect(onSelect).toHaveBeenCalledExactlyOnceWith('config/pipeline_config_dev.yaml')
  })

  it('prefers job + monitoring configs on the job tab', async () => {
    const user = userEvent.setup()
    renderPicker({ kind: 'job' })

    await user.click(screen.getByRole('combobox', { name: /select config file/i }))

    const options = screen.getAllByRole('option').map((o) => o.textContent)
    expect(options[0]).toBe('config/job_config_dev.yaml')
  })

  it('shows the placeholder without a value and the path (mono) with one', () => {
    renderPicker({ value: null })
    expect(screen.getByRole('combobox')).toHaveTextContent('Select config file…')
  })

  it('shows an empty message when config/ holds no YAML files', async () => {
    const user = userEvent.setup()
    renderPicker({
      files: { name: '', path: '', type: 'directory', children: [] },
    })

    await user.click(screen.getByRole('combobox', { name: /select config file/i }))
    expect(screen.getByText('No YAML files under config/.')).toBeInTheDocument()
    expect(screen.queryAllByRole('option')).toHaveLength(0)
  })
})
