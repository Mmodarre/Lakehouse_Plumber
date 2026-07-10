/**
 * Main-thread replacement for `monaco-worker-manager`, aliased in
 * vite.config.ts so that monaco-yaml works with monaco-editor >= 0.53.
 *
 * monaco-worker-manager (frozen at 2.0.1, 2022 — still a dependency of
 * monaco-yaml 5.5.1) calls `monaco.editor.createWebWorker({ moduleId,
 * label, createData })`. Since monaco-editor 0.53 that API ignores
 * `moduleId`/`createData` and requires the caller to hand over the Worker
 * instance (`opts.worker`), so the call silently falls back to the plain
 * editor worker: the YAML worker never spawns and every language request
 * fails with "Missing requestHandler or method: doValidation" — i.e.
 * YAML schema validation silently dies.
 *
 * This shim reproduces monaco's own >= 0.53 language-worker bootstrap
 * (see monaco-editor esm/vs/common/workers.js, used by its JSON service):
 * spawn the worker through `MonacoEnvironment.getWorker`, post the
 * two-message handshake ("ignore" wake-up, then createData — exactly what
 * monaco-worker-manager's worker-side wrapper expects), and pass the
 * spawned worker to `createWebWorker`. The public API (createWorkerManager
 * with getWorker/updateCreateData/dispose and idle shutdown) matches the
 * original so monaco-yaml's main thread is none the wiser.
 */
import type * as monacoNs from 'monaco-editor/esm/vs/editor/editor.api'

interface WorkerManagerOptions<C> {
  label: string
  moduleId: string
  createData: C
  /** Idle-check period in milliseconds. */
  interval?: number
  /** Stop the worker after this much idle time in milliseconds. */
  stopWhenIdleFor?: number
}

export interface WorkerManager<T, C> {
  dispose: () => void
  getWorker: (...resources: monacoNs.Uri[]) => Promise<T>
  updateCreateData: (createData: C) => void
}

function spawnWorker(label: string): Worker | Promise<Worker> {
  const env = (globalThis as { MonacoEnvironment?: monacoNs.Environment }).MonacoEnvironment
  if (typeof env?.getWorker !== 'function') {
    throw new Error('MonacoEnvironment.getWorker must be defined (see monaco-setup.ts)')
  }
  return env.getWorker('workerMain.js', label)
}

export function createWorkerManager<T extends object, C>(
  monaco: Pick<typeof monacoNs, 'editor'>,
  options: WorkerManagerOptions<C>,
): WorkerManager<T, C> {
  let { createData } = options
  const { interval = 30_000, label, stopWhenIdleFor = 120_000 } = options
  let worker: monacoNs.editor.MonacoWebWorker<T> | undefined
  let lastUsedTime = 0
  let disposed = false

  const stopWorker = () => {
    worker?.dispose()
    worker = undefined
  }

  const intervalId = setInterval(() => {
    if (worker && Date.now() - lastUsedTime > stopWhenIdleFor) {
      stopWorker()
    }
  }, interval)

  return {
    dispose() {
      disposed = true
      clearInterval(intervalId)
      stopWorker()
    },
    getWorker(...resources) {
      if (disposed) {
        throw new Error('Worker manager has been disposed')
      }
      lastUsedTime = Date.now()
      if (!worker) {
        const raw = Promise.resolve(spawnWorker(label)).then((w) => {
          w.postMessage('ignore')
          w.postMessage(createData)
          return w
        })
        worker = monaco.editor.createWebWorker<T>({ worker: raw })
      }
      return worker.withSyncedResources(resources)
    },
    updateCreateData(newCreateData) {
      createData = newCreateData
      stopWorker()
    },
  }
}
