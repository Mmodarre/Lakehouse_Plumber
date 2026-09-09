import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ChatComposer } from '../ChatComposer'
import { useChatDraftStore } from '../../../store/chatDraftStore'
vi.mock('../../../hooks/useAssistant', () => ({ useInterruptAssistant: () => ({ mutate: vi.fn(), isPending: false }) }))
beforeEach(() => useChatDraftStore.setState({ drafts: {}, positions: {} }))
describe('conversation drafts and IME', () => {
  it('retains separate drafts across conversation switches and dock remounts', () => {
    const onSend = vi.fn()
    const view = render(<ChatComposer streaming={false} conversationKey="first" onSend={onSend} />)
    fireEvent.change(screen.getByRole('textbox', { name: 'Chat message' }), { target: { value: 'First unfinished message' } })
    view.rerender(<ChatComposer streaming={false} conversationKey="second" onSend={onSend} />)
    expect(screen.getByRole('textbox', { name: 'Chat message' })).toHaveValue('')
    fireEvent.change(screen.getByRole('textbox', { name: 'Chat message' }), { target: { value: 'Second unfinished message' } })
    view.unmount()
    render(<ChatComposer streaming={false} conversationKey="first" onSend={onSend} />)
    expect(screen.getByRole('textbox', { name: 'Chat message' })).toHaveValue('First unfinished message')
    fireEvent.keyDown(screen.getByRole('textbox', { name: 'Chat message' }), { key: 'Enter' })
    expect(onSend).toHaveBeenCalledWith('First unfinished message')
    expect(useChatDraftStore.getState().drafts).toEqual({ second: 'Second unfinished message' })
  })

  it('does not send on composition Enter, the IME 229 fallback, or Shift+Enter', () => {
    const onSend = vi.fn()
    render(<ChatComposer streaming={false} conversationKey="first" onSend={onSend} />)
    const input = screen.getByRole('textbox', { name: 'Chat message' })
    fireEvent.change(input, { target: { value: '日本語' } })
    fireEvent.keyDown(input, { key: 'Enter', isComposing: true })
    fireEvent.keyDown(input, { key: 'Enter', keyCode: 229 })
    fireEvent.keyDown(input, { key: 'Enter', shiftKey: true })
    expect(onSend).not.toHaveBeenCalled()
    expect(input).toHaveValue('日本語')
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onSend).toHaveBeenCalledOnce()
  })

  it('allows composing the next message during streaming without sending it', () => {
    const onSend = vi.fn()
    render(<ChatComposer streaming conversationKey="first" onSend={onSend} />)
    const input = screen.getByRole('textbox', { name: 'Chat message' })
    fireEvent.change(input, { target: { value: 'Next message' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onSend).not.toHaveBeenCalled()
    expect(useChatDraftStore.getState().drafts.first).toBe('Next message')
  })
})
