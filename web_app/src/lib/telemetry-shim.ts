import type { UiAction, UiSurface, UiVia } from './telemetry'

/**
 * The only usage-telemetry entry point the eager app chunk may import.
 *
 * The client (`./telemetry`) is loaded lazily by the shell once the server
 * reports telemetry on, so components report through this forwarder and
 * never pull the client into the app chunk. Until the client binds itself,
 * calls are held (bounded) rather than dropped: a surface shown on first
 * paint, such as the init wizard, is counted once the client arrives, and
 * nothing is held past the cap for a tab whose server never opts in.
 */
export type TrackFn = (surface: UiSurface, action: UiAction, via?: UiVia) => void

/** Calls held before the client binds; beyond this the newest are dropped. */
export const PENDING_CAP = 32

let impl: TrackFn | null = null
let pending: Parameters<TrackFn>[] = []

/** Route every call to `fn` and hand it what was held so far, oldest first. */
export function bindTrack(fn: TrackFn): void {
  impl = fn
  const held = pending
  pending = []
  for (const args of held) fn(...args)
}

/** Report a surface interaction; see `track` in ./telemetry for the vocabulary. */
export function track(surface: UiSurface, action: UiAction, via?: UiVia): void {
  if (impl !== null) impl(surface, action, via)
  else if (pending.length < PENDING_CAP) pending.push([surface, action, via])
}
