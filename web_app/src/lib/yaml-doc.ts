/**
 * Comment-preserving YAML round-trip layer for the Config UI.
 *
 * Forms edit `lhp.yaml` / `pipeline_config*.yaml` / `job_config*.yaml`
 * through this module so that hand-written comments, unknown keys, key
 * order, and scalar quoting survive a save. The hard guarantee: a save
 * that changed one field produces a diff touching only the intended lines.
 *
 * Mechanism. `Document.toString()` regenerates text from the AST and
 * provably cannot reproduce every source byte (the parser conflates `# `
 * with `#`, drops the column of comments that trail indented blocks, and
 * loses whitespace-only line content), so serialization is anchored to the
 * original source text instead:
 *
 * - Each parsed document owns a byte region of the source. Regions tile
 *   the file: doc N's region runs from its first token (its `---` marker
 *   when present) to the start of doc N+1, so inter-document comments
 *   belong to the preceding document, matching the AST's own attachment.
 * - Untouched documents serialize as their verbatim region slice.
 * - Setting an EXISTING scalar is patched at the CST token level
 *   (`CST.setScalarValue` via the node's `srcToken`): only that scalar's
 *   bytes change, everything else in the document stays byte-identical.
 * - Setting a NEW key whose nearest existing ancestor is a block map is a
 *   byte-surgical text splice: the new entry (or nested subtree) is
 *   serialized on its own and inserted at the end of that map's source
 *   text. No other byte moves. Later edits inside a spliced subtree only
 *   update the splice.
 * - The REMAINING structural edits — deletes, inserts into flow
 *   collections or sequences, and populating empty documents — rewrite
 *   ONLY the containing document via `Document.toString()`; sibling
 *   documents keep their exact bytes. Rewriting normalizes that
 *   document's comment whitespace (pinned by tests).
 *
 * `toString` fidelity options: `lineWidth: 0` disables re-wrapping of long
 * plain scalars and flow collections, which would otherwise reflow lines
 * that were never edited.
 *
 * These files are read back by PyYAML's SafeLoader (YAML 1.1), so strings
 * that YAML 1.1 resolves as non-strings (`yes`/`on`/`off`, `1:30`,
 * `1_000`, `0b101`, ISO dates, ...) are double-quoted when written, in
 * addition to the YAML 1.2 ambiguities (`true`, `42`, ...).
 *
 * Pinned caveats (asserted in yaml-doc.test.ts):
 * - Deleting a node deletes the comment block above it and its inline
 *   comment (they are owned by the deleted pair).
 * - A rewrite re-emits the containing document from its AST: trailing
 *   whitespace inside comments and the column of out-dented comments are
 *   normalized in that one document.
 * - Rewritten and added documents always end with a newline; an unmutated
 *   file without a trailing newline round-trips without gaining one.
 * - Files whose dominant line ending is CRLF get CRLF in all emitted text.
 * - Mutations throw on a handle with parse errors; callers must block
 *   form editing on broken files.
 */
import {
  CST,
  Document,
  isCollection,
  isMap,
  isScalar,
  isSeq,
  parse,
  parseAllDocuments,
  visit,
} from 'yaml'
import type { Node, Pair, Scalar, YAMLError, YAMLMap } from 'yaml'

/** Path into a document, as accepted by `Document.getIn`/`setIn`. */
export type YamlPath = readonly (string | number)[]

/** Opaque handle to a parsed multi-document YAML file. */
export interface ConfigFileHandle {
  /** Parse errors across all documents; mutations throw while non-empty. */
  readonly errors: readonly YAMLError[]
}

type ScalarToken = CST.FlowScalar | CST.BlockScalar

/** A pending new-map-entry insertion at a fixed source offset. */
interface Splice {
  start: number
  seq: number
  parent: YAMLMap
  pair: Pair
  indent: number
}

interface DocEntry {
  doc: Document
  /** [start, end) byte range in the source; null for documents added later. */
  region: readonly [number, number] | null
  /** A structural edit happened: serialize this doc via `toString()`. */
  rewritten: boolean
  /** Surgical scalar patches: CST token -> its original byte span. */
  patches: Map<ScalarToken, { start: number; end: number }>
  /** Surgical new-key insertions, rendered from the AST at serialize time. */
  splices: Splice[]
  /** Roots of spliced subtrees: edits inside them only update the splice. */
  splicedRoots: Set<unknown>
}

interface HandleState extends ConfigFileHandle {
  _source: string
  /** Whole source when the file parsed to zero documents, else ''. */
  _prefix: string
  _newline: '\n' | '\r\n'
  _entries: DocEntry[]
  _editSeq: number
  /** Document ranges were inconsistent; serialize source verbatim, block edits. */
  _degraded: boolean
}

const TO_STRING_OPTIONS = { lineWidth: 0 } as const

/**
 * Parse a (possibly multi-document) YAML source into a mutable handle.
 *
 * Never throws on YAML errors: they are collected on `handle.errors` so
 * callers can block form editing on broken files while still reading and
 * re-serializing them byte-identically.
 */
export function parseConfigFile(source: string): ConfigFileHandle {
  const parsed = parseAllDocuments(source, { keepSourceTokens: true })
  const docs = [...parsed]
  const errors: YAMLError[] = docs.flatMap((d) => d.errors)
  if ('empty' in parsed && parsed.errors) errors.push(...parsed.errors)

  const entries: DocEntry[] = []
  let degraded = false
  let previousStart = 0
  for (let i = 0; i < docs.length; i++) {
    const start = i === 0 ? 0 : docs[i].range[0]
    const end = i + 1 < docs.length ? docs[i + 1].range[0] : source.length
    if (start < previousStart || end < start || end > source.length) degraded = true
    previousStart = start
    entries.push({
      doc: docs[i],
      region: [start, end],
      rewritten: false,
      patches: new Map(),
      splices: [],
      splicedRoots: new Set(),
    })
  }

  const crlf = (source.match(/\r\n/g) ?? []).length
  const bareLf = (source.match(/(?<!\r)\n/g) ?? []).length
  const handle: HandleState = {
    errors,
    _source: source,
    _prefix: docs.length === 0 ? source : '',
    _newline: crlf > bareLf ? '\r\n' : '\n',
    _entries: entries,
    _editSeq: 0,
    _degraded: degraded,
  }
  return handle
}

/**
 * Serialize the handle back to YAML text.
 *
 * Byte-identical (`===`) to the original source when nothing was mutated.
 * Untouched documents always keep their exact bytes; scalar patches and
 * new-key splices change only their own bytes; rewritten/added documents
 * are emitted via `Document.toString({ lineWidth: 0 })` with a `---`
 * separator where one is required.
 */
export function serializeConfigFile(handle: ConfigFileHandle): string {
  const h = state(handle)
  if (h._degraded) return h._source
  let out = h._prefix
  for (const entry of h._entries) {
    const chunk =
      entry.region !== null && !entry.rewritten
        ? sliceWithEdits(h, entry)
        : toFileNewlines(renderDocument(entry, out), h._newline)
    if (out !== '' && !out.endsWith('\n') && chunk !== '') out += h._newline
    out += chunk
  }
  return out
}

/** Read a value (wraps `Document.getIn`; collections come back as AST nodes). */
export function getPath(handle: ConfigFileHandle, docIndex: number, path: YamlPath): unknown {
  return entryAt(state(handle), docIndex).doc.getIn(path)
}

/**
 * Surgically set `path` to `value` in one document.
 *
 * Replacing an existing scalar with a primitive is patched at the CST
 * level: only that scalar's bytes change and its original quoting style is
 * kept. Adding a key under an existing block map (any value; intermediate
 * maps are created) splices the new entry at the end of that map's text.
 * Strings that would re-parse as another type under YAML 1.1 or 1.2 are
 * double-quoted. Everything else (numeric segments creating sequences,
 * flow-collection inserts, populating empty documents) rewrites the
 * containing document only. `undefined` is treated as `null`. Throws if
 * the handle has parse errors.
 */
export function setPath(
  handle: ConfigFileHandle,
  docIndex: number,
  path: YamlPath,
  value: unknown,
): void {
  const h = state(handle)
  assertMutable(h)
  const entry = entryAt(h, docIndex)
  const normalized = value === undefined ? null : value
  const doc = entry.doc

  if (entry.region !== null && !entry.rewritten) {
    if (inSplicedSubtree(entry, path)) {
      setViaAst(doc, path, normalized)
      return
    }
    if (isPrimitive(normalized) && tryPatchScalar(h, entry, path, normalized)) return
    if (tryInsertMapEntry(h, entry, path, normalized)) return
  }

  // An "empty" document parses to a null Scalar placeholder (not a null
  // contents), which Document.setIn refuses to descend into. Replace it,
  // carrying any comments it held.
  if (path.length > 0 && isScalar(doc.contents) && doc.contents.value === null) {
    const placeholder = doc.contents as Scalar
    doc.contents = null
    setViaAst(doc, path, normalized)
    const contents = doc.contents as Node | null
    if (contents) {
      const inherited = [placeholder.commentBefore, placeholder.comment]
        .filter((c) => c != null)
        .join('\n')
      if (inherited) contents.commentBefore = inherited
    }
  } else {
    setViaAst(doc, path, normalized)
  }
  entry.rewritten = true
}

/**
 * Delete exactly the node at `path` (wraps `Document.deleteIn`).
 *
 * A missing path is a no-op that leaves the file byte-identical. Emptied
 * parents are NOT cascade-deleted (higher layers decide that). Deleting
 * inside a spliced subtree just updates the splice; any other delete
 * rewrites the containing document, so the deleted node's own comments go
 * with it. Throws if the handle has parse errors.
 */
export function deletePath(handle: ConfigFileHandle, docIndex: number, path: YamlPath): void {
  const h = state(handle)
  assertMutable(h)
  const entry = entryAt(h, docIndex)
  if (!entry.doc.hasIn(path)) return
  // Containment must be checked before deleteIn — afterwards the deleted
  // node is no longer reachable through the path.
  const contained =
    entry.region !== null && !entry.rewritten && inSplicedSubtree(entry, path)
  entry.doc.deleteIn(path)
  if (!contained) entry.rewritten = true
}

/** Plain-JS snapshot of one document (wraps `Document.toJS`). */
export function toJS(handle: ConfigFileHandle, docIndex: number): unknown {
  return entryAt(state(handle), docIndex).doc.toJS()
}

/**
 * Insert `value` into the sequence at `path` at position `index`
 * (`0 <= index <= length`); later items shift up one. The inserted subtree
 * is built quote-safe like `setPath`'s. This is a structural edit: it
 * rewrites the containing document (sibling documents keep their exact
 * bytes; see the module header for what a rewrite normalizes). Throws if
 * the handle has parse errors, `path` is not a sequence, or `index` is out
 * of range.
 */
export function insertListItem(
  handle: ConfigFileHandle,
  docIndex: number,
  path: YamlPath,
  index: number,
  value: unknown,
): void {
  const h = state(handle)
  assertMutable(h)
  const entry = entryAt(h, docIndex)
  const doc = entry.doc
  const node = path.length === 0 ? doc.contents : doc.getIn(path, true)
  if (!isSeq(node)) {
    throw new Error(`No sequence at [${path.join(', ')}] in document ${docIndex}`)
  }
  if (!Number.isInteger(index) || index < 0 || index > node.items.length) {
    throw new RangeError(
      `Insert index ${index} out of range (sequence has ${node.items.length} item(s))`,
    )
  }
  node.items.splice(index, 0, buildValueNode(doc, value === undefined ? null : value))
  entry.rewritten = true
}

/**
 * Append a new document and return its index.
 *
 * `initial` defaults to an empty mapping (`{}`). The document serializes
 * after a `---` separator unless it becomes the first content in the file.
 * Throws if the handle has parse errors.
 */
export function addDocument(handle: ConfigFileHandle, initial: unknown = {}): number {
  const h = state(handle)
  assertMutable(h)
  const doc = new Document(initial, { aliasDuplicateObjects: false })
  h._entries.push({
    doc,
    region: null,
    rewritten: true,
    patches: new Map(),
    splices: [],
    splicedRoots: new Set(),
  })
  return h._entries.length - 1
}

/**
 * Remove one document. Later documents shift down one index. Removing a
 * middle document removes its `---` and its text; the remaining documents'
 * bytes are untouched. Throws if the handle has parse errors.
 */
export function removeDocument(handle: ConfigFileHandle, docIndex: number): void {
  const h = state(handle)
  assertMutable(h)
  entryAt(h, docIndex)
  h._entries.splice(docIndex, 1)
}

/** Number of documents currently in the handle. */
export function documentCount(handle: ConfigFileHandle): number {
  return state(handle)._entries.length
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

type Primitive = string | number | boolean | null

function state(handle: ConfigFileHandle): HandleState {
  return handle as HandleState
}

function assertMutable(h: HandleState): void {
  if (h.errors.length > 0) {
    throw new Error(
      `Cannot edit a file with ${h.errors.length} YAML parse error(s); fix the YAML first`,
    )
  }
  if (h._degraded) {
    throw new Error('Cannot edit this file: document ranges could not be resolved')
  }
}

function entryAt(h: HandleState, docIndex: number): DocEntry {
  const entry = h._entries[docIndex]
  if (!Number.isInteger(docIndex) || entry === undefined) {
    throw new RangeError(
      `Document index ${docIndex} out of range (${h._entries.length} document(s))`,
    )
  }
  return entry
}

function isPrimitive(value: unknown): value is Primitive {
  return (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  )
}

function isScalarToken(token: unknown): token is ScalarToken {
  const type = (token as { type?: string } | undefined)?.type
  return (
    type === 'scalar' ||
    type === 'single-quoted-scalar' ||
    type === 'double-quoted-scalar' ||
    type === 'block-scalar'
  )
}

/** Source text a primitive should have when written as a plain scalar. */
function plainText(value: Primitive): string {
  if (value === null) return 'null'
  if (typeof value === 'number') {
    if (Number.isNaN(value)) return '.nan'
    if (value === Infinity) return '.inf'
    if (value === -Infinity) return '-.inf'
  }
  return String(value)
}

/**
 * Plain strings that YAML 1.1 (PyYAML SafeLoader — see
 * src/lhp/parsers/yaml_loader.py) resolves as non-strings but YAML 1.2
 * core leaves alone: 1.1 booleans, underscore/binary/octal/sexagesimal
 * numbers, timestamps, `=` (value) and `<<` (merge). Overlap with 1.2 is
 * harmless — anything matching gets quoted.
 */
const YAML11_AMBIGUOUS = new RegExp(
  '^(?:' +
    [
      'y|Y|n|N|yes|Yes|YES|no|No|NO|on|On|ON|off|Off|OFF',
      'true|True|TRUE|false|False|FALSE',
      '[-+]?0b[0-1_]+',
      '[-+]?0o?[0-7_]+',
      '[-+]?(?:0|[1-9][0-9_]*)',
      '[-+]?0x[0-9a-fA-F_]+',
      '[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+',
      '[-+]?[0-9][0-9_]*\\.[0-9_]*(?:[eE][-+]?[0-9]+)?',
      '\\.[0-9_]+(?:[eE][-+]?[0-9]+)?',
      '[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*',
      '[0-9]{4}-[0-9]{2}-[0-9]{2}',
      '[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}(?:[Tt]|[ \\t]+)[0-9]{1,2}:[0-9]{2}:[0-9]{2}' +
        '(?:\\.[0-9]*)?(?:[ \\t]*(?:Z|[-+][0-9]{1,2}(?::[0-9]{2})?))?',
      '=|<<|~|null|Null|NULL',
    ].join('|') +
    ')$',
)

/** Would this string re-parse as a different value if written plain? */
function stringNeedsQuote(value: string): boolean {
  if (YAML11_AMBIGUOUS.test(value)) return true
  try {
    return parse(value) !== value
  } catch {
    return true
  }
}

/** Quote a scalar node's string value if plain style would misparse it. */
function applyScalarStyle(node: Scalar, value: Primitive): void {
  if (typeof value === 'string') {
    if ((node.type === undefined || node.type === 'PLAIN') && stringNeedsQuote(value)) {
      node.type = 'QUOTE_DOUBLE'
    }
  } else {
    node.type = 'PLAIN'
  }
}

/** Wrap a JS value as AST nodes with YAML-1.1-safe string quoting. */
function buildValueNode(doc: Document, value: unknown): Node {
  const node = doc.createNode(value, { aliasDuplicateObjects: false })
  visit(node, {
    Scalar(_key, scalar) {
      if (typeof scalar.value === 'string') applyScalarStyle(scalar, scalar.value)
    },
  })
  return node
}

/**
 * AST-level set used for rewritten documents and spliced subtrees.
 * Existing scalar nodes are updated in place (keeping their comments and
 * quoting); new nodes are created quote-safe.
 */
function setViaAst(doc: Document, path: YamlPath, value: unknown): void {
  const existing = path.length > 0 ? doc.getIn(path, true) : undefined
  if (isPrimitive(value) && isScalar(existing)) {
    doc.setIn(path, value)
    applyScalarStyle(existing as Scalar, value)
  } else {
    doc.setIn(path, buildValueNode(doc, value))
  }
}

/** Is any collection on the way to `path` a flow collection? */
function pathInFlow(doc: Document, path: YamlPath): boolean {
  for (let i = 0; i < path.length; i++) {
    const node = i === 0 ? doc.contents : doc.getIn(path.slice(0, i), true)
    if (isCollection(node) && node.flow) return true
  }
  return false
}

/** Does `path` lead into a subtree created by an earlier splice? */
function inSplicedSubtree(entry: DocEntry, path: YamlPath): boolean {
  if (entry.splicedRoots.size === 0) return false
  for (let i = 1; i <= path.length; i++) {
    const node = entry.doc.getIn(path.slice(0, i), true)
    if (node !== undefined && entry.splicedRoots.has(node)) return true
  }
  return false
}

/**
 * Try to replace an existing scalar in place at the CST level. Returns
 * false when the target is missing, not a scalar, lacks a verifiable
 * source token, or sits outside the document's region — the caller then
 * tries a splice or falls back to a document rewrite.
 */
function tryPatchScalar(
  h: HandleState,
  entry: DocEntry,
  path: YamlPath,
  value: Primitive,
): boolean {
  if (path.length === 0) return false
  const doc = entry.doc
  const node = doc.getIn(path, true)
  if (!isScalar(node)) return false
  const token = node.srcToken
  if (!isScalarToken(token)) return false

  let span = entry.patches.get(token)
  if (span === undefined) {
    const oldText = CST.stringify(token)
    const start = token.offset
    const end = start + oldText.length
    const [regionStart, regionEnd] = entry.region as [number, number]
    if (start < regionStart || end > regionEnd) return false
    // The span must reproduce the source bytes exactly, or splicing the
    // new token text back in would corrupt neighbouring content.
    if (h._source.slice(start, end) !== oldText) return false
    span = { start, end }
  }

  const parentPath = path.slice(0, -1)
  const parent = parentPath.length > 0 ? doc.getIn(parentPath, true) : doc.contents
  let type: 'PLAIN' | 'QUOTE_DOUBLE' | undefined
  if (typeof value === 'string') {
    if (token.type === 'scalar' && stringNeedsQuote(value)) type = 'QUOTE_DOUBLE'
  } else {
    // Numbers, booleans, and null must not inherit a quoted style, which
    // would turn them into strings on the next parse.
    type = 'PLAIN'
  }
  CST.setScalarValue(token, typeof value === 'string' ? value : plainText(value), {
    afterKey: isMap(parent),
    inFlow: pathInFlow(doc, path),
    type,
  })
  node.value = value
  if (type) node.type = type
  entry.patches.set(token, span)
  return true
}

/**
 * Try to add a NEW entry under an existing block map as a byte-surgical
 * splice at the end of that map's source text. Handles nested creation
 * (all remaining path segments must be string keys). Returns false when
 * the nearest existing ancestor is not a verifiable block map — the
 * caller falls back to a document rewrite.
 */
function tryInsertMapEntry(
  h: HandleState,
  entry: DocEntry,
  path: YamlPath,
  value: unknown,
): boolean {
  if (path.length === 0) return false
  const doc = entry.doc
  // Only pure insertions: the addressed node must not exist yet.
  if (doc.getIn(path, true) !== undefined) return false
  let depth = 0
  for (let i = path.length - 1; i >= 1; i--) {
    if (doc.getIn(path.slice(0, i), true) !== undefined) {
      depth = i
      break
    }
  }
  const parent = depth === 0 ? doc.contents : doc.getIn(path.slice(0, depth), true)
  if (!isMap(parent) || parent.flow) return false
  const remaining = path.slice(depth)
  if (!remaining.every((seg) => typeof seg === 'string')) return false
  const token = parent.srcToken
  if (token?.type !== 'block-map') return false

  const text = CST.stringify(token)
  const start = token.offset
  const end = start + text.length
  const [regionStart, regionEnd] = entry.region as [number, number]
  if (start < regionStart || end > regionEnd) return false
  if (h._source.slice(start, end) !== text) return false

  // Build the quote-safe subtree and graft it into the AST so reads,
  // later edits, and any eventual rewrite all see it.
  let node = buildValueNode(doc, value)
  for (let i = remaining.length - 1; i >= 1; i--) {
    const wrap = doc.createNode({}, { aliasDuplicateObjects: false }) as YAMLMap
    const inner = doc.createPair(remaining[i], node)
    quoteFixKey(inner)
    wrap.items.push(inner)
    node = wrap
  }
  const pair = doc.createPair(remaining[0], node)
  quoteFixKey(pair)
  parent.items.push(pair)

  entry.splices.push({ start: end, seq: h._editSeq++, parent, pair, indent: token.indent })
  entry.splicedRoots.add(pair.value)
  return true
}

function quoteFixKey(pair: Pair): void {
  const key = pair.key
  if (isScalar(key) && typeof key.value === 'string' && stringNeedsQuote(key.value)) {
    key.type = 'QUOTE_DOUBLE'
  }
}

/** Convert emitted LF-only text to the file's newline style. */
function toFileNewlines(text: string, newline: '\n' | '\r\n'): string {
  return newline === '\n' ? text : text.replace(/(?<!\r)\n/g, newline)
}

/** Region slice with scalar patches and splices applied, back to front. */
function sliceWithEdits(h: HandleState, entry: DocEntry): string {
  const [regionStart, regionEnd] = entry.region as [number, number]
  let text = h._source.slice(regionStart, regionEnd)
  const edits: { start: number; end: number; text: string; seq: number }[] = []
  for (const [token, span] of entry.patches) {
    edits.push({ ...span, text: toFileNewlines(CST.stringify(token), h._newline), seq: -1 })
  }
  for (const splice of entry.splices) {
    const rendered = renderSplice(h, splice)
    if (rendered !== null) {
      edits.push({ start: splice.start, end: splice.start, text: rendered, seq: splice.seq })
    }
  }
  // Back to front; equal offsets apply later-added first so earlier splices
  // end up first in the output.
  edits.sort((a, b) => b.start - a.start || b.seq - a.seq)
  for (const edit of edits) {
    text = text.slice(0, edit.start - regionStart) + edit.text + text.slice(edit.end - regionStart)
  }
  return text
}

/** Render one spliced map entry from its live AST pair. */
function renderSplice(h: HandleState, splice: Splice): string | null {
  // The entry may have been deleted again after being added.
  if (!splice.parent.items.includes(splice.pair)) return null
  const tmp = new Document({})
  ;(tmp.contents as YAMLMap).items.push(splice.pair)
  let text = tmp.toString(TO_STRING_OPTIONS)
  if (splice.indent > 0) text = text.replace(/^(?!$)/gm, ' '.repeat(splice.indent))
  text = toFileNewlines(text, h._newline)
  const afterNewline = splice.start === 0 || h._source[splice.start - 1] === '\n'
  // At an unterminated last line, lead with the newline instead of
  // trailing one so a file without a final newline stays that way.
  return afterNewline ? text : h._newline + text.slice(0, -h._newline.length)
}

/** Emit a rewritten or added document, managing its `---` marker. */
function renderDocument(entry: DocEntry, emittedSoFar: string): string {
  if (entry.region === null && entry.doc.directives) {
    // Added documents need a separator unless they are the first content
    // in the file; parsed documents keep the marker state they had.
    entry.doc.directives.docStart = emittedSoFar !== '' ? true : null
  }
  return entry.doc.toString(TO_STRING_OPTIONS)
}
