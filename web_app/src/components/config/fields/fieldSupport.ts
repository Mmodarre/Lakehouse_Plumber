// ── Pure helpers shared by the config field primitives ───────
// (Component chrome lives in ./FieldChrome — this file must stay free of
// component exports for fast-refresh purity.)

/** Coerce any YAML scalar to the string a text control should display. */
export function displayString(value: unknown): string {
  if (value === undefined || value === null) return ''
  if (typeof value === 'string') return value
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

/** `aria-describedby` id for a field's issue line. */
export function issueId(id: string): string {
  return `${id}-issue`
}
