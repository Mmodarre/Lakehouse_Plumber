import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ChatThread } from '../ChatThread'
import { useChatDraftStore } from '../../../store/chatDraftStore'
import type { MessagePart } from '../../../store/assistantStore'
vi.mock('../ChatMessage', () => ({ ChatMessage: ({ text }: { text: string }) => <p>{text}</p> }))
const parts: MessagePart[] = [{ id: 1, kind: 'text', role: 'assistant', text: 'First answer' }]
const props = { parts, streaming: false, statusState: null, failure: null, interrupted: false, profile: null, conversationKey: 'conversation' }
beforeEach(() => useChatDraftStore.setState({ drafts: {}, positions: {} }))
describe('assistant reading context', () => {
  it('does not pull the reader down on new messages and restores position after remount', () => {
    const view = render(<ChatThread {...props} />)
    let region = screen.getByRole('region', { name: 'Assistant conversation' })
    Object.defineProperties(region, { scrollHeight: { configurable: true, value: 1000 }, clientHeight: { configurable: true, value: 200 } })
    region.scrollTop = 100
    fireEvent.scroll(region)
    view.rerender(<ChatThread {...props} parts={[...parts, { id: 2, kind: 'text', role: 'assistant', text: 'New answer' }]} />)
    expect(region.scrollTop).toBe(100)
    expect(screen.getByRole('button', { name: 'Jump to latest messages' })).toBeInTheDocument()
    view.unmount()
    render(<ChatThread {...props} />)
    region = screen.getByRole('region', { name: 'Assistant conversation' })
    expect(region.scrollTop).toBe(100)
    Object.defineProperty(region, 'scrollHeight', { value: 1200 })
    fireEvent.click(screen.getByRole('button', { name: 'Jump to latest messages' }))
    expect(region.scrollTop).toBe(1200)
    expect(useChatDraftStore.getState().positions.conversation.atBottom).toBe(true)
  })
})
