import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { FileNode } from '@/types/api'

// Mock the file-list hook so the picker renders a fixed tree with no network.
const useFileListMock = vi.fn()
vi.mock('@/hooks/useFiles', () => ({
  useFileList: () => useFileListMock(),
}))

import { FilePicker } from '../FilePicker'

// Root tree: one directory holding a .sql and a .py sibling, plus a
// top-level non-matching file. `path: ""` mirrors the real backend root.
const TREE: FileNode = {
  name: '',
  path: '',
  type: 'directory',
  children: [
    {
      name: 'pipelines',
      path: 'pipelines',
      type: 'directory',
      children: [
        {
          name: 'raw',
          path: 'pipelines/raw',
          type: 'directory',
          children: [
            { name: 'ingest.sql', path: 'pipelines/raw/ingest.sql', type: 'file' },
            { name: 'transform.py', path: 'pipelines/raw/transform.py', type: 'file' },
          ],
        },
      ],
    },
    { name: 'README.md', path: 'README.md', type: 'file' },
  ],
}

function mockData(node: FileNode | undefined, extra: Record<string, unknown> = {}) {
  useFileListMock.mockReturnValue({
    data: node,
    isLoading: false,
    error: null,
    ...extra,
  })
}

describe('FilePicker — extension filtering', () => {
  it('shows only accepted files and hides non-matching ones', () => {
    mockData(TREE)
    render(<FilePicker accept={['.sql']} onPick={vi.fn()} onClose={vi.fn()} />)

    expect(screen.getByText('ingest.sql')).toBeInTheDocument()
    expect(screen.queryByText('transform.py')).toBeNull()
    expect(screen.queryByText('README.md')).toBeNull()
  })

  it('calls onPick with the project-relative path when a file is clicked', () => {
    mockData(TREE)
    const onPick = vi.fn()
    render(<FilePicker accept={['.sql']} onPick={onPick} onClose={vi.fn()} />)

    fireEvent.click(screen.getByRole('button', { name: 'ingest.sql' }))

    expect(onPick).toHaveBeenCalledTimes(1)
    expect(onPick).toHaveBeenCalledWith('pipelines/raw/ingest.sql')
  })
})
