/**
 * Per-tab session id for the `lhp web` local IDE.
 *
 * The backend attributes usage telemetry to a browser tab by this id: every
 * `/api` call carries it as `X-LHP-Session` (see `authHeaders` in
 * ../api/client) and the push channel as `?session=` (see ../api/push). It
 * is random, carries no user or machine information, and is never logged.
 *
 * It lives in `sessionStorage` under `lhp-web-session`, so it survives a
 * reload and client-side navigation while a fresh tab starts a new session.
 * When storage is unavailable (private mode, disabled storage) the id is
 * held in memory instead, so it stays stable for the tab's lifetime either
 * way.
 */
const SESSION_KEY = 'lhp-web-session'

// Lowercase RFC 4122 v4 text form — the only spelling the backend accepts;
// anything else stored under the key is replaced rather than sent.
const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

let memoryId: string | null = null

// `crypto.randomUUID` exists only in secure contexts (https or localhost);
// an `http://` LAN origin gets a Math.random v4 with the same layout.
function mintUuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.floor(Math.random() * 16)
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16)
  })
}

/** The tab's session id, minted on first use and stable for the tab's lifetime. */
export function getSessionId(): string {
  let stored: string | null = null
  try {
    stored = sessionStorage.getItem(SESSION_KEY)
  } catch {
    // Storage unavailable; the in-memory id below covers this tab.
  }
  if (stored !== null && UUID_V4.test(stored)) {
    memoryId = stored
    return stored
  }
  memoryId ??= mintUuid()
  try {
    sessionStorage.setItem(SESSION_KEY, memoryId)
  } catch {
    // Storage unavailable; the id still lives in memory for this tab.
  }
  return memoryId
}
