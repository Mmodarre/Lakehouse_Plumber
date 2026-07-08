/**
 * Session-token bootstrap for the `lhp web` local IDE.
 *
 * When the server is launched with a token it opens the browser at
 * `http://.../#token=<value>`. We lift that token out of the URL hash into
 * `sessionStorage` (so it survives client-side navigation but not a new tab)
 * and strip it from the address bar so it isn't left in history or shared.
 *
 * When the server runs without a token there is no hash and `getToken()`
 * returns `null`, so every request proceeds unchanged — this is a no-op
 * against the old (tokenless) backend.
 */
const TOKEN_KEY = 'lhp-webapp-token'

/**
 * Read a `#token=<value>` fragment from the current URL, persist it to
 * `sessionStorage`, and remove it from the address bar. Idempotent: once the
 * hash has been stripped a second call finds nothing and does nothing (safe
 * under React StrictMode double-invocation).
 */
export function bootstrapToken(): void {
  const match = /^#token=(.+)$/.exec(window.location.hash)
  if (!match) return
  try {
    sessionStorage.setItem(TOKEN_KEY, match[1])
  } catch {
    // sessionStorage may be unavailable (private mode / disabled). The token
    // is still stripped from the URL below; requests just proceed without it.
  }
  history.replaceState(null, '', window.location.pathname + window.location.search)
}

/** The stored session token, or `null` when none was provided. */
export function getToken(): string | null {
  try {
    return sessionStorage.getItem(TOKEN_KEY)
  } catch {
    return null
  }
}
