import { postUiEvents } from '../api/telemetry'
import { getSessionId } from './session-id'
import { bindTrack } from './telemetry-shim'

/**
 * UI-surface usage telemetry client for the `lhp web` IDE.
 *
 * Reports WHICH parts of the IDE were opened, toggled or created — a closed
 * vocabulary mirrored from the backend's `UI_SURFACES` / `UI_ACTIONS` /
 * `UI_VIA` (`lhp.webapp.schemas.telemetry`) — and nothing about what they
 * held: no names, paths, buffer content, search text, graph node identities,
 * environment or run configuration can reach this module, because its only
 * inputs are the literal unions below.
 *
 * Nothing happens at import. The shell loads this module lazily once
 * `/api/health` reports `telemetry_enabled`, and `track` drops everything
 * until `setTelemetryEnabled(true)` has run. Posting is batched: the queue
 * drains every {@link FLUSH_INTERVAL_MS}, when the tab is hidden, and on
 * `pagehide`, in requests of at most {@link MAX_EVENTS_PER_POST} events.
 */

export type UiSurface =
  | 'file_editor'
  | 'flowgroup_graph'
  | 'flowgroup_code'
  | 'template_graph'
  | 'template_builder'
  | 'template_code'
  | 'template_preview'
  | 'config_form_project'
  | 'config_form_pipeline'
  | 'config_form_job'
  | 'config_yaml_project'
  | 'config_yaml_pipeline'
  | 'config_yaml_job'
  | 'project_map'
  | 'pipeline_dag'
  | 'table_detail'
  | 'resource_preset'
  | 'resource_template'
  | 'resource_blueprint'
  | 'resource_environment'
  | 'files_lens'
  | 'structure_lens'
  | 'tables_lens'
  | 'inspector_validation'
  | 'inspector_help'
  | 'problems'
  | 'run_stream'
  | 'run_history'
  | 'assistant_panel'
  | 'viewer_mode'
  | 'create_flowgroup_dialog'
  | 'sandbox_control'
  | 'sandbox_picker'
  | 'init_wizard'

export type UiAction = 'opened' | 'toggled' | 'created'

/** How a `created` surface was created; meaningless on any other action. */
export type UiVia = 'blank' | 'template' | 'blueprint'

interface QueuedEvent {
  surface: UiSurface
  action: UiAction
  via?: UiVia
}

/** Events held in the tab between flushes; beyond this the oldest are dropped. */
export const QUEUE_CAP = 200

/** How often the queue is posted while enabled. */
export const FLUSH_INTERVAL_MS = 15_000

/**
 * Events per request. The backend rejects a larger batch as a whole (422),
 * so exceeding this would lose the entire chunk rather than trim it.
 */
export const MAX_EVENTS_PER_POST = 100

let enabled: boolean | null = null
let queue: QueuedEvent[] = []
let interval: ReturnType<typeof setInterval> | null = null
let installed = false

/**
 * Queue one observation. A no-op until enabled. Consecutive identical
 * `opened` events collapse into one (focus churn is not a second open);
 * `toggled` and `created` always count. `via` is kept only on `created`.
 */
export function track(surface: UiSurface, action: UiAction, via?: UiVia): void {
  if (enabled !== true) return
  const last = queue[queue.length - 1]
  if (action === 'opened' && last !== undefined && last.surface === surface && last.action === action) {
    return
  }
  if (queue.length >= QUEUE_CAP) queue.shift()
  queue.push(action === 'created' && via !== undefined ? { surface, action, via } : { surface, action })
}

/** Post everything queued, in chunks the backend accepts. Fire-and-forget. */
export function flush(): void {
  if (enabled !== true || queue.length === 0) return
  const pending = queue
  queue = []
  const session_id = getSessionId()
  for (let i = 0; i < pending.length; i += MAX_EVENTS_PER_POST) {
    void postUiEvents({ session_id, events: pending.slice(i, i + MAX_EVENTS_PER_POST) })
  }
}

/**
 * Apply the server's consent state. Enabling arms the flush interval (once)
 * and binds the eager shim, which also delivers what components reported
 * before this module loaded. Disabling discards the queue and the interval,
 * so nothing queued under a stale state is ever posted.
 */
export function setTelemetryEnabled(value: boolean): void {
  enabled = value
  if (value) {
    interval ??= setInterval(flush, FLUSH_INTERVAL_MS)
    bindTrack(track)
    return
  }
  queue = []
  if (interval !== null) {
    clearInterval(interval)
    interval = null
  }
}

/**
 * Register the page-lifecycle flushes once. A hidden or unloading tab may
 * never reach the next interval tick, so both moments post immediately
 * (`keepalive` on the request lets it outlive the page).
 */
export function installTelemetry(): void {
  if (installed) return
  installed = true
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flush()
  })
  window.addEventListener('pagehide', () => flush())
}
