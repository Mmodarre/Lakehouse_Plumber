import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type {
  ApprovalAction,
  AssistantFrame,
  PermissionMode,
  SessionSnapshot,
} from '../types/assistant'
import {
  applyFrameToConversation,
  beginTurnInConversation,
  conversationFromSnapshot,
  emptyConversation,
  failTransportInConversation,
  finishStreamInConversation,
  resolveApprovalInConversation,
} from './assistantConversation'
import type { ConversationState } from './assistantConversation'

export type {
  AssistantFailure,
  ConversationState,
  MessagePart,
  PendingApproval,
  SessionMeta,
} from './assistantConversation'
export { normalizeSnapshotItem } from './assistantConversation'

// ── assistantStore — multi-tab container for the chat panel ──
//
// The frames' *meaning* lives in the pure `assistantConversation` reducer;
// this store holds MANY `ConversationState`s side by side, keyed by tab:
// a real session id (`claude_…` / `conv_…`) once known, or a `draft:<n>`
// placeholder for a tab whose first message has not been sent yet (lazy
// provisioning — no DB session exists until the backend's `session` frame
// binds the tab via `rekeyTab`). Every conversation action takes the tab
// key and delegates to the pure functions on that entry.
//
// Persisted fields: `panelOpen`, `panelWidth`, `permissionMode` (user
// preferences). Tabs and conversations are transient — a reload rebuilds
// tabs from `GET /api/assistant/sessions` and hydrates each conversation
// lazily on first activation from `GET /api/assistant/session?session_id=`.

/** Tab keys for sessions that do not exist server-side yet. The key is also
 * what the first chat request sends as `session_id`: the backend mints a
 * fresh session for any unknown id. */
export function isDraftKey(tabKey: string): boolean {
  return tabKey.startsWith('draft:')
}

/** Inline divider label for a reopened session whose SDK context is gone. */
export const RESUME_LOST_HINT =
  "Previous context can't be restored — the next message starts fresh but keeps this transcript."

/** What `syncTabsFromSessions` needs from a `GET /sessions` row. */
export interface TabSessionInfo {
  session_id: string
  title?: string | null
}

interface AssistantState {
  /** Per-tab conversations, keyed by session id or `draft:<n>`. */
  conversations: Record<string, ConversationState>
  /** Tab strip order (keys into `conversations`). */
  tabOrder: string[]
  /** The tab whose conversation the panel renders. */
  activeTabKey: string | null
  /** Server-known titles (null = untitled); drafts have no entry. */
  tabTitles: Record<string, string | null>
  /** Monotonic counter behind `draft:<n>` keys. */
  nextDraftId: number
  /** Panel visibility (persisted). */
  panelOpen: boolean
  /** Panel dock width in px (persisted; clamped by the resize handle). */
  panelWidth: number
  /** Per-turn approval policy sent with every chat request (persisted). */
  permissionMode: PermissionMode

  setPanelOpen: (open: boolean) => void
  togglePanel: () => void
  setPanelWidth: (width: number) => void
  setPermissionMode: (mode: PermissionMode) => void

  /** Open a fresh draft tab, activate it, and return its key. */
  openTab: () => string
  /** Open (or just focus) a tab for an existing session (history reopen). */
  openSessionTab: (sessionId: string, title?: string | null) => void
  activateTab: (tabKey: string) => void
  /** Remove a tab locally. Archiving the server-side session is the
   * caller's job (draft tabs have nothing to archive). Closing the last
   * tab opens a fresh draft so the panel always has a composer target. */
  closeTab: (tabKey: string) => void
  /** Re-key a tab once the `session` frame reports its real session id
   * (draft → session id, or a drift-replaced session's old id → new id).
   * Preserves the tab's position in `tabOrder`. */
  rekeyTab: (fromKey: string, sessionId: string) => void
  /** Merge the server's active-session list into the tab strip: missing
   * sessions become tabs (in the given, MRU-first order), titles refresh,
   * and an empty strip gets a draft tab. Existing tabs are never removed. */
  syncTabsFromSessions: (sessions: TabSessionInfo[]) => void

  /** Append the user message to the tab and mark its turn streaming. */
  beginTurn: (tabKey: string, text: string) => void
  /** Fold one decoded NDJSON frame into the tab's conversation. */
  applyFrame: (tabKey: string, frame: AssistantFrame) => void
  /** Record a stream-open/transport failure on the tab. */
  failTransport: (tabKey: string, error: Error) => void
  /** Mark the tab's stream closed (clean end, terminal frame, or abort). */
  finishStream: (tabKey: string) => void
  /** Mark an approval part resolved after the mutation succeeds. */
  resolveApprovalLocal: (
    tabKey: string,
    elicitationId: string,
    action: ApprovalAction,
  ) => void
  /** Replace an EMPTY, idle tab conversation with a session snapshot
   * (no-op on a tab that already has parts or is streaming). Appends the
   * resume-lost hint divider when the snapshot is not resumable. */
  hydrateFromSnapshot: (tabKey: string, snapshot: SessionSnapshot) => void
}

function withConversation(
  s: AssistantState,
  tabKey: string,
  fn: (conversation: ConversationState) => ConversationState,
): Partial<AssistantState> {
  const conversation = s.conversations[tabKey]
  if (conversation === undefined) return {}
  return { conversations: { ...s.conversations, [tabKey]: fn(conversation) } }
}

/** Resize bounds for the docked panel (px). The max stays conservative so
 * the main editor always keeps a usable width. */
export const PANEL_MIN_WIDTH = 300
export const PANEL_MAX_WIDTH = 760
export const PANEL_DEFAULT_WIDTH = 360

export const useAssistantStore = create<AssistantState>()(
  persist(
    (set, get) => ({
      conversations: {},
      tabOrder: [],
      activeTabKey: null,
      tabTitles: {},
      nextDraftId: 1,
      panelOpen: false,
      panelWidth: PANEL_DEFAULT_WIDTH,
      permissionMode: 'default',

      setPanelOpen: (open) => set({ panelOpen: open }),
      togglePanel: () => set((s) => ({ panelOpen: !s.panelOpen })),
      setPanelWidth: (width) =>
        set({
          panelWidth: Math.min(PANEL_MAX_WIDTH, Math.max(PANEL_MIN_WIDTH, width)),
        }),
      setPermissionMode: (mode) => set({ permissionMode: mode }),

      openTab: () => {
        const key = `draft:${get().nextDraftId}`
        set((s) => ({
          nextDraftId: s.nextDraftId + 1,
          conversations: { ...s.conversations, [key]: emptyConversation() },
          tabOrder: [...s.tabOrder, key],
          activeTabKey: key,
        }))
        return key
      },

      openSessionTab: (sessionId, title) =>
        set((s) => {
          const tabTitles =
            title !== undefined
              ? { ...s.tabTitles, [sessionId]: title }
              : s.tabTitles
          if (s.tabOrder.includes(sessionId)) {
            return { activeTabKey: sessionId, tabTitles }
          }
          return {
            conversations: {
              ...s.conversations,
              [sessionId]: emptyConversation(),
            },
            tabOrder: [...s.tabOrder, sessionId],
            activeTabKey: sessionId,
            tabTitles,
          }
        }),

      activateTab: (tabKey) =>
        set((s) => (s.tabOrder.includes(tabKey) ? { activeTabKey: tabKey } : {})),

      closeTab: (tabKey) =>
        set((s) => {
          const idx = s.tabOrder.indexOf(tabKey)
          if (idx === -1) return {}
          const conversations = { ...s.conversations }
          delete conversations[tabKey]
          const tabTitles = { ...s.tabTitles }
          delete tabTitles[tabKey]
          let tabOrder = s.tabOrder.filter((k) => k !== tabKey)
          let nextDraftId = s.nextDraftId
          if (tabOrder.length === 0) {
            const draftKey = `draft:${nextDraftId}`
            nextDraftId += 1
            tabOrder = [draftKey]
            conversations[draftKey] = emptyConversation()
          }
          const activeTabKey =
            s.activeTabKey === tabKey || s.activeTabKey === null
              ? tabOrder[Math.min(idx, tabOrder.length - 1)]
              : s.activeTabKey
          return { conversations, tabTitles, tabOrder, activeTabKey, nextDraftId }
        }),

      rekeyTab: (fromKey, sessionId) =>
        set((s) => {
          if (fromKey === sessionId) return {}
          const conversation = s.conversations[fromKey]
          if (conversation === undefined) return {}
          const conversations = { ...s.conversations }
          delete conversations[fromKey]
          conversations[sessionId] = conversation
          const tabTitles = { ...s.tabTitles }
          if (fromKey in tabTitles) {
            tabTitles[sessionId] = tabTitles[fromKey]
            delete tabTitles[fromKey]
          }
          const fromIdx = s.tabOrder.indexOf(fromKey)
          // Keep the rekeyed tab's position; drop any pre-existing tab that
          // already carried the target key (they are now the same session).
          const tabOrder = s.tabOrder
            .map((k) => (k === fromKey ? sessionId : k))
            .filter((k, i) => k !== sessionId || i === fromIdx)
          return {
            conversations,
            tabTitles,
            tabOrder,
            activeTabKey: s.activeTabKey === fromKey ? sessionId : s.activeTabKey,
          }
        }),

      syncTabsFromSessions: (sessions) =>
        set((s) => {
          const tabTitles = { ...s.tabTitles }
          for (const session of sessions) {
            tabTitles[session.session_id] = session.title ?? null
          }
          const additions = sessions.filter(
            (session) => !s.tabOrder.includes(session.session_id),
          )
          let conversations = s.conversations
          let tabOrder = s.tabOrder
          if (additions.length > 0) {
            conversations = { ...conversations }
            tabOrder = [...tabOrder]
            for (const session of additions) {
              conversations[session.session_id] = emptyConversation()
              tabOrder.push(session.session_id)
            }
          }
          let nextDraftId = s.nextDraftId
          if (tabOrder.length === 0) {
            const draftKey = `draft:${nextDraftId}`
            nextDraftId += 1
            tabOrder = [draftKey]
            conversations = { ...conversations, [draftKey]: emptyConversation() }
          }
          const activeTabKey =
            s.activeTabKey !== null && tabOrder.includes(s.activeTabKey)
              ? s.activeTabKey
              : tabOrder[0]
          return { conversations, tabOrder, tabTitles, activeTabKey, nextDraftId }
        }),

      beginTurn: (tabKey, text) =>
        set((s) =>
          withConversation(s, tabKey, (c) => beginTurnInConversation(c, text)),
        ),

      applyFrame: (tabKey, frame) =>
        set((s) =>
          withConversation(s, tabKey, (c) => applyFrameToConversation(c, frame)),
        ),

      failTransport: (tabKey, error) =>
        set((s) =>
          withConversation(s, tabKey, (c) => failTransportInConversation(c, error)),
        ),

      finishStream: (tabKey) =>
        set((s) => withConversation(s, tabKey, finishStreamInConversation)),

      resolveApprovalLocal: (tabKey, elicitationId, action) =>
        set((s) =>
          withConversation(s, tabKey, (c) =>
            resolveApprovalInConversation(c, elicitationId, action),
          ),
        ),

      hydrateFromSnapshot: (tabKey, snapshot) =>
        set((s) => {
          const conversation = s.conversations[tabKey]
          if (
            conversation === undefined ||
            conversation.parts.length > 0 ||
            conversation.streaming
          ) {
            return {}
          }
          let next = conversationFromSnapshot(snapshot)
          if (snapshot.resumable === false && next.parts.length > 0) {
            next = {
              ...next,
              parts: [
                ...next.parts,
                { id: next.nextPartId, kind: 'divider', label: RESUME_LOST_HINT },
              ],
              nextPartId: next.nextPartId + 1,
            }
          }
          return {
            conversations: { ...s.conversations, [tabKey]: next },
            tabTitles: { ...s.tabTitles, [tabKey]: snapshot.title ?? null },
          }
        }),
    }),
    {
      name: 'lhp-assistant',
      // Conversations and tabs are transient by design — a reload rebuilds
      // tabs from GET /sessions. Only the panel preferences survive.
      partialize: (s) => ({
        panelOpen: s.panelOpen,
        panelWidth: s.panelWidth,
        permissionMode: s.permissionMode,
      }),
    },
  ),
)

const EMPTY_CONVERSATION: ConversationState = emptyConversation()

/** The active tab's conversation (a stable empty one before any tab exists),
 * so thread/composer/footer components stay single-conversation shaped. */
export function useActiveConversation(): ConversationState {
  return useAssistantStore((s) =>
    s.activeTabKey !== null
      ? (s.conversations[s.activeTabKey] ?? EMPTY_CONVERSATION)
      : EMPTY_CONVERSATION,
  )
}

/** The active tab's REAL session id for mutations (approval/interrupt):
 * the bound session if known, else the tab key when it already is one.
 * `undefined` for drafts — the backend then falls back to the MRU session. */
export function activeSessionId(): string | undefined {
  const s = useAssistantStore.getState()
  if (s.activeTabKey === null) return undefined
  const bound = s.conversations[s.activeTabKey]?.sessionMeta?.sessionId
  if (bound !== undefined) return bound
  return isDraftKey(s.activeTabKey) ? undefined : s.activeTabKey
}
