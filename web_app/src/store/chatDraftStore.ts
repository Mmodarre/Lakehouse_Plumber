import { create } from 'zustand'

/** Composer text belongs to its conversation and survives closing the dock. */
export const useChatDraftStore = create<{
  drafts: Record<string, string>
  positions: Record<string, { top: number; atBottom: boolean }>
  setPosition: (key: string, position: { top: number; atBottom: boolean }) => void
  moveConversation: (from: string, to: string) => void
  setDraft: (key: string, value: string) => void
}>((set) => ({
  drafts: {},
  positions: {},
  setPosition: (key, position) => set((s) => ({ positions: { ...s.positions, [key]: position } })),
  moveConversation: (from, to) => set((s) => {
    if (from === to) return {}
    const drafts = { ...s.drafts }, positions = { ...s.positions }
    if (from in drafts) { drafts[to] = drafts[from]; delete drafts[from] }
    if (from in positions) { positions[to] = positions[from]; delete positions[from] }
    return { drafts, positions }
  }),
  setDraft: (key, value) => set((s) => {
    const drafts = { ...s.drafts }
    if (value) drafts[key] = value
    else delete drafts[key]
    return { drafts }
  }),
}))
