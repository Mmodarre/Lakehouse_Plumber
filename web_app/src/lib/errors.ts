import { ApiError } from '../api/client'

/**
 * Map an unknown thrown value to a user-facing message.
 *
 * `ApiError` is translated to a friendly string (write-protected paths return
 * 403); any other value falls back to the provided `fallback`.
 */
export function errorMessage(err: unknown, fallback: string): string {
  if (err instanceof ApiError) {
    // The file API returns 403 for write-protected paths
    // (.git/, generated/, .lhp/logs/, .lhp/dependencies/, .lhp_state.json).
    if (err.status === 403) {
      return 'This path is write-protected and cannot be modified.'
    }
    return err.message
  }
  return fallback
}
