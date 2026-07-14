import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FilesLens } from '../FilesLens'

// FilesLens is a thin re-export host: it renders the reused FileBrowser as-is.
vi.mock('../../../sidebar/FileBrowser', () => ({
  FileBrowser: () => <div data-testid="file-browser" />,
}))

describe('FilesLens', () => {
  it('renders the reused FileBrowser', () => {
    render(<FilesLens />)
    expect(screen.getByTestId('file-browser')).toBeInTheDocument()
  })
})
