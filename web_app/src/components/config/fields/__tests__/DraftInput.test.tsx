import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { DraftInput } from '../DraftInput'

it('warns on unload while local text is uncommitted and removes the guard after reverting', () => {
  render(<DraftInput initial="saved" onCommit={vi.fn()} aria-label="Draft" />)
  const input = screen.getByRole('textbox', { name: 'Draft' })
  fireEvent.change(input, { target: { value: 'unsaved' } })
  const unload = new Event('beforeunload', { cancelable: true })
  window.dispatchEvent(unload)
  expect(unload.defaultPrevented).toBe(true)
  fireEvent.keyDown(input, { key: 'Escape' })
  const nextUnload = new Event('beforeunload', { cancelable: true })
  window.dispatchEvent(nextUnload)
  expect(nextUnload.defaultPrevented).toBe(false)
})

describe('DraftInput IME', () => {
  it('does not commit Enter during composition but commits completed text on blur', () => {
    const commit = vi.fn()
    render(<DraftInput initial="" onCommit={commit} aria-label="Draft" />)
    const input = screen.getByRole('textbox', { name: 'Draft' })
    fireEvent.change(input, { target: { value: '東京' } })
    fireEvent.keyDown(input, { key: 'Enter', isComposing: true, keyCode: 229 })
    expect(commit).not.toHaveBeenCalled()
    fireEvent.blur(input)
    expect(commit).toHaveBeenCalledWith('東京')
  })
})
