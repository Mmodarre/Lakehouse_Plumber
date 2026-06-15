// ── yamlSaveSupport — shared editor-save helpers ─────────────
//
// Small, dependency-free helpers used by both editor modals on save:
//   • detect whether a saved file is YAML (drives marker/validate behaviour);
//   • derive the owning pipeline from saved YAML text so an auto-triggered
//     validate run can be scoped to a single pipeline.
//
// `derivePipelineFromYaml` is intentionally a cheap line scan, NOT a full YAML
// parse (v1): it reads the first top-level `pipeline:` key. This is sufficient
// for LHP flowgroup files (one `pipeline:` per file at column 0) and avoids
// pulling a YAML parser into the editor bundle. It is only ever a *scoping
// hint* — callers fall back to an unscoped validate when it returns null, so a
// miss never breaks validation, it only widens its scope.

/** The subset of the run controller this module needs. */
interface ValidateRunController {
  isRunning: boolean
  startValidate: (env?: string, pipeline?: string) => void
  abort: () => void
}

/**
 * Trigger a validate run scoped to `pipeline` (or unscoped when undefined),
 * superseding any in-flight run on the same controller.
 *
 * The transport hook ignores a `start()` while a run is in flight and only
 * clears its running flag asynchronously after an `abort()`. So when a run is
 * already in flight we abort and defer the start to the next macrotask, by
 * which time the flag has cleared; otherwise we start immediately.
 */
export function startScopedValidate(
  controller: ValidateRunController,
  pipeline: string | undefined,
): void {
  if (controller.isRunning) {
    controller.abort()
    setTimeout(() => controller.startValidate(undefined, pipeline), 0)
  } else {
    controller.startValidate(undefined, pipeline)
  }
}

/** True for `.yaml` / `.yml` files (case-insensitive). */
export function isYamlPath(path: string): boolean {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  return ext === 'yaml' || ext === 'yml'
}

/**
 * Extract the top-level `pipeline:` value from YAML text, or null if none is
 * found. Scans for the first line of the form `pipeline: <value>` at column 0
 * (no leading whitespace), skipping comments. Strips surrounding quotes and
 * trailing inline comments. Returns null for block scalars / empty values.
 */
export function derivePipelineFromYaml(yaml: string): string | null {
  for (const rawLine of yaml.split('\n')) {
    // Only top-level keys (column 0). Indented `pipeline:` keys belong to
    // nested structures and are not the flowgroup's pipeline.
    if (/^\s/.test(rawLine)) continue
    const line = rawLine.trimEnd()
    const match = /^pipeline:\s*(.*)$/.exec(line)
    if (!match) continue

    let value = match[1].trim()
    if (value === '' || value === '|' || value === '>') return null

    // Strip surrounding quotes.
    if (
      (value.startsWith('"') && value.endsWith('"') && value.length >= 2) ||
      (value.startsWith("'") && value.endsWith("'") && value.length >= 2)
    ) {
      value = value.slice(1, -1)
    } else {
      // Drop a trailing inline comment on an unquoted scalar.
      const hash = value.indexOf(' #')
      if (hash !== -1) value = value.slice(0, hash).trim()
    }

    return value === '' ? null : value
  }
  return null
}
