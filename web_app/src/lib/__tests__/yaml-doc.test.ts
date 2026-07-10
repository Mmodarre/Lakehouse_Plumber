/**
 * Critical round-trip suite for the yaml-doc layer (Config UI Phase 1a).
 *
 * The load-bearing guarantees under test:
 *   1. Zero-mutation serialization is BYTE-identical (===) to the source,
 *      including the comment-dense packaged templates read from disk.
 *   2. Mutating one value produces a diff touching only the intended lines;
 *      sibling nodes, comments, key order, and scalar quoting are untouched.
 *   3. Unknown/passthrough keys survive mutations byte-exactly.
 *   4. `---` separators are preserved exactly; add/remove document keeps the
 *      remaining separators valid.
 *
 * All equality assertions are `toBe` (byte-exact), never "semantically equal".
 */
/// <reference types="node" />
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

import {
  addDocument,
  deletePath,
  documentCount,
  getPath,
  parseConfigFile,
  removeDocument,
  serializeConfigFile,
  setPath,
  toJS,
} from '../yaml-doc'

const REPO_ROOT = resolve(import.meta.dirname, '../../../..')
const TEMPLATE_DIR = resolve(REPO_ROOT, 'src/lhp/templates/init/config')
const TEMPLATES = [
  'pipeline_config_env.yaml.tmpl',
  'job_config_env.yaml.tmpl',
  'monitoring_job_config_env.yaml.tmpl',
] as const

function readTemplate(name: string): string {
  return readFileSync(resolve(TEMPLATE_DIR, name), 'utf8')
}

function roundTrip(source: string): string {
  return serializeConfigFile(parseConfigFile(source))
}

/** Assert `after` differs from `before` in exactly one line; return its index. */
function expectSingleLineDiff(before: string, after: string): number {
  const a = before.split('\n')
  const b = after.split('\n')
  expect(b.length).toBe(a.length)
  const changed: number[] = []
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) changed.push(i)
  expect(changed).toHaveLength(1)
  return changed[0]
}

/**
 * Assert `after` is `before` with lines purely INSERTED (every original
 * line byte-exact and in order); return the inserted lines.
 */
function expectPureInsertion(before: string, after: string): string[] {
  const a = before.split('\n')
  const b = after.split('\n')
  expect(b.length).toBeGreaterThan(a.length)
  let prefix = 0
  while (prefix < a.length && a[prefix] === b[prefix]) prefix++
  let suffix = 0
  while (suffix < a.length - prefix && a[a.length - 1 - suffix] === b[b.length - 1 - suffix]) {
    suffix++
  }
  // Nothing removed or modified — the change is a pure insertion.
  expect(a.length - prefix - suffix).toBe(0)
  return b.slice(prefix, b.length - suffix)
}

// ---------------------------------------------------------------------------
// Fixtures (module scope so template literals carry exact bytes, no indent)
// ---------------------------------------------------------------------------

const COMMENTED = `# File header comment
# second line

# About alpha
alpha: 1
beta: two # inline note
# About gamma
gamma:
  nested: true
  other: plain
delta: last
`

const MULTI_DOC = `# file header
alpha: 1

# between zero and one
---
# doc one banner
pipeline: beta
edition: PRO
---
# trailing doc
pipeline: gamma
`

const PASSTHROUGH = `job_name: orders
# schedule passthrough
trigger:
  file_arrival:
    url: s3://bucket/landing/
    min_time_between_triggers_seconds: 60
run_as:
  service_principal_name: abc-123
health:
  rules:
    - metric: RUN_DURATION_SECONDS
      op: GREATER_THAN
      value: 3600
git_source:
  git_url: https://example.com/repo.git
  git_provider: gitHub
  deep:
    unknown:
      keys: preserved
max_concurrent_runs: 1
`

const THREE_DOCS = `a: 1
---
b: 2
---
c: 3
`

const LEADING_MARKER = `---
a: 1
---
b: 2
`

const QUOTING = `name: "quoted"
count: 10
flag: true
note: plain
empty_target: value
`

const MALFORMED = `broken: [1, 2
next: : :
`

const MALFORMED_MULTI = `good: 1
---
bad: [
`

// ---------------------------------------------------------------------------
// 1. Byte-identical zero-mutation round-trip of the packaged templates
// ---------------------------------------------------------------------------

describe('zero-mutation round-trip of packaged templates', () => {
  it.each(TEMPLATES)('%s round-trips byte-identically', (name) => {
    const src = readTemplate(name)
    const handle = parseConfigFile(src)
    expect(handle.errors).toHaveLength(0)
    expect(serializeConfigFile(handle)).toBe(src)
  })

  it('parses the multi-document templates into multiple documents', () => {
    expect(
      documentCount(parseConfigFile(readTemplate('pipeline_config_env.yaml.tmpl'))),
    ).toBeGreaterThan(1)
    expect(
      documentCount(parseConfigFile(readTemplate('job_config_env.yaml.tmpl'))),
    ).toBeGreaterThan(1)
  })

  it('reading via getPath/toJS does not dirty the handle', () => {
    const src = readTemplate('job_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    expect(getPath(handle, 0, ['project_defaults', 'max_concurrent_runs'])).toBe(1)
    expect(toJS(handle, 0)).toMatchObject({
      project_defaults: { max_concurrent_runs: 1 },
    })
    expect(serializeConfigFile(handle)).toBe(src)
  })
})

// ---------------------------------------------------------------------------
// 2. Comment survival
// ---------------------------------------------------------------------------

describe('comment survival', () => {
  it('zero-mutation round-trip of the commented fixture is byte-exact', () => {
    expect(roundTrip(COMMENTED)).toBe(COMMENTED)
  })

  it('setting an existing scalar keeps every comment byte-exact', () => {
    const handle = parseConfigFile(COMMENTED)
    setPath(handle, 0, ['beta'], 'three')
    expect(serializeConfigFile(handle)).toBe(
      COMMENTED.replace('beta: two # inline note', 'beta: three # inline note'),
    )
  })

  it('adding a new key keeps every existing line byte-exact', () => {
    const handle = parseConfigFile(COMMENTED)
    setPath(handle, 0, ['epsilon'], 'new')
    expect(serializeConfigFile(handle)).toBe(COMMENTED + 'epsilon: new\n')
  })

  it('deleting one nested key keeps all other lines and comments byte-exact', () => {
    const handle = parseConfigFile(COMMENTED)
    deletePath(handle, 0, ['gamma', 'nested'])
    expect(serializeConfigFile(handle)).toBe(COMMENTED.replace('  nested: true\n', ''))
  })

  it('comments between documents survive a patch in another document', () => {
    const handle = parseConfigFile(MULTI_DOC)
    setPath(handle, 1, ['edition'], 'CORE')
    expect(serializeConfigFile(handle)).toBe(
      MULTI_DOC.replace('edition: PRO', 'edition: CORE'),
    )
  })

  it('comments between documents survive a key-add in another document', () => {
    const handle = parseConfigFile(MULTI_DOC)
    setPath(handle, 1, ['serverless'], true)
    expect(serializeConfigFile(handle)).toBe(
      MULTI_DOC.replace('edition: PRO\n', 'edition: PRO\nserverless: true\n'),
    )
  })

  it('pins the R3 caveat: deleting a key removes the comment block above it', () => {
    const src = '# on a\na: 1\n# on b\nb: 2\nc: 3\n'
    const handle = parseConfigFile(src)
    deletePath(handle, 0, ['b'])
    // The `# on b` comment is owned by the deleted pair and goes with it.
    expect(serializeConfigFile(handle)).toBe('# on a\na: 1\nc: 3\n')
  })

  it('pins the R3 caveat: deleting a key removes its inline comment', () => {
    const handle = parseConfigFile('a: 1 # gone with a\nb: 2\n')
    deletePath(handle, 0, ['a'])
    expect(serializeConfigFile(handle)).toBe('b: 2\n')
  })
})

// ---------------------------------------------------------------------------
// 3. Passthrough / unknown keys
// ---------------------------------------------------------------------------

describe('passthrough keys', () => {
  it('round-trips the passthrough fixture byte-exactly', () => {
    expect(roundTrip(PASSTHROUGH)).toBe(PASSTHROUGH)
  })

  it('patching a known key leaves all unknown subtrees byte-identical', () => {
    const handle = parseConfigFile(PASSTHROUGH)
    setPath(handle, 0, ['max_concurrent_runs'], 5)
    expect(serializeConfigFile(handle)).toBe(
      PASSTHROUGH.replace('max_concurrent_runs: 1', 'max_concurrent_runs: 5'),
    )
  })

  it('adding a known key leaves all unknown subtrees byte-identical', () => {
    const handle = parseConfigFile(PASSTHROUGH)
    setPath(handle, 0, ['tags', 'team'], 'data')
    expect(serializeConfigFile(handle)).toBe(PASSTHROUGH + 'tags:\n  team: data\n')
  })

  it('mutating a value nested inside an unknown subtree is surgical too', () => {
    const handle = parseConfigFile(PASSTHROUGH)
    setPath(handle, 0, ['trigger', 'file_arrival', 'min_time_between_triggers_seconds'], 90)
    expect(serializeConfigFile(handle)).toBe(
      PASSTHROUGH.replace(
        'min_time_between_triggers_seconds: 60',
        'min_time_between_triggers_seconds: 90',
      ),
    )
  })
})

// ---------------------------------------------------------------------------
// 4. `---` separator stability
// ---------------------------------------------------------------------------

describe('--- separator stability', () => {
  it('round-trips multi-doc files with and without a leading ---', () => {
    expect(roundTrip(THREE_DOCS)).toBe(THREE_DOCS)
    expect(roundTrip(LEADING_MARKER)).toBe(LEADING_MARKER)
  })

  it('addDocument appends exactly one separator and the new document', () => {
    const handle = parseConfigFile(THREE_DOCS)
    const index = addDocument(handle, { d: 4 })
    expect(index).toBe(3)
    const out = serializeConfigFile(handle)
    expect(out).toBe(THREE_DOCS + '---\nd: 4\n')
    expect(documentCount(parseConfigFile(out))).toBe(4)
  })

  it('addDocument does not add a leading --- to a file that had none', () => {
    const handle = parseConfigFile(THREE_DOCS)
    addDocument(handle, { d: 4 })
    expect(serializeConfigFile(handle).startsWith('a: 1\n')).toBe(true)
  })

  it('addDocument without initial content appends an empty mapping', () => {
    const handle = parseConfigFile('a: 1\n')
    addDocument(handle)
    expect(serializeConfigFile(handle)).toBe('a: 1\n---\n{}\n')
  })

  it('removeDocument(first) keeps the remaining docs byte-exact and parseable', () => {
    const handle = parseConfigFile(THREE_DOCS)
    removeDocument(handle, 0)
    const out = serializeConfigFile(handle)
    expect(out).toBe('---\nb: 2\n---\nc: 3\n')
    expect(documentCount(parseConfigFile(out))).toBe(2)
  })

  it('removeDocument(middle) keeps the remaining docs byte-exact and parseable', () => {
    const handle = parseConfigFile(THREE_DOCS)
    removeDocument(handle, 1)
    const out = serializeConfigFile(handle)
    expect(out).toBe('a: 1\n---\nc: 3\n')
    expect(documentCount(parseConfigFile(out))).toBe(2)
  })

  it('removeDocument(last) keeps the remaining docs byte-exact and parseable', () => {
    const handle = parseConfigFile(THREE_DOCS)
    removeDocument(handle, 2)
    const out = serializeConfigFile(handle)
    expect(out).toBe('a: 1\n---\nb: 2\n')
    expect(documentCount(parseConfigFile(out))).toBe(2)
  })

  it('removeDocument shifts subsequent indices (index refers to current order)', () => {
    const handle = parseConfigFile(THREE_DOCS)
    removeDocument(handle, 0)
    setPath(handle, 0, ['b'], 20)
    expect(serializeConfigFile(handle)).toBe('---\nb: 20\n---\nc: 3\n')
  })

  it('removing every document yields an empty file', () => {
    const handle = parseConfigFile(THREE_DOCS)
    removeDocument(handle, 0)
    removeDocument(handle, 0)
    removeDocument(handle, 0)
    expect(serializeConfigFile(handle)).toBe('')
    expect(documentCount(handle)).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// 5. Surgical diff shape on a real packaged template
// ---------------------------------------------------------------------------

describe('surgical diff shape on the pipeline template', () => {
  it('replacing one boolean deep in doc 0 changes exactly one line', () => {
    const src = readTemplate('pipeline_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['project_defaults', 'serverless'], false)
    const out = serializeConfigFile(handle)
    const line = expectSingleLineDiff(src, out)
    expect(src.split('\n')[line]).toBe('  serverless: true')
    expect(out.split('\n')[line]).toBe('  serverless: false')
  })

  it('replacing one string deep in doc 0 changes exactly one line', () => {
    const src = readTemplate('pipeline_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['project_defaults', 'edition'], 'PRO')
    const out = serializeConfigFile(handle)
    const line = expectSingleLineDiff(src, out)
    expect(src.split('\n')[line]).toBe('  edition: ADVANCED')
    expect(out.split('\n')[line]).toBe('  edition: PRO')
  })

  it('two patches in the same document change exactly those two lines', () => {
    const src = readTemplate('job_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['project_defaults', 'max_concurrent_runs'], 3)
    setPath(handle, 0, ['project_defaults', 'performance_target'], 'PERFORMANCE_OPTIMIZED')
    const out = serializeConfigFile(handle)
    const a = src.split('\n')
    const b = out.split('\n')
    expect(b.length).toBe(a.length)
    const changed = a.flatMap((l, i) => (l === b[i] ? [] : [i]))
    expect(changed).toHaveLength(2)
    expect(b[changed[0]]).toBe('  max_concurrent_runs: 3')
    expect(b[changed[1]]).toBe('  performance_target: PERFORMANCE_OPTIMIZED')
  })

  it('setting the same scalar twice keeps the diff at one line (last write wins)', () => {
    const handle = parseConfigFile(COMMENTED)
    setPath(handle, 0, ['alpha'], 5)
    setPath(handle, 0, ['alpha'], 7)
    expect(serializeConfigFile(handle)).toBe(COMMENTED.replace('alpha: 1', 'alpha: 7'))
  })

  it('a patch followed by a structural edit keeps both changes', () => {
    const handle = parseConfigFile(COMMENTED)
    setPath(handle, 0, ['beta'], 'three')
    setPath(handle, 0, ['epsilon'], 'new')
    expect(serializeConfigFile(handle)).toBe(
      COMMENTED.replace('beta: two', 'beta: three') + 'epsilon: new\n',
    )
  })

  it('a rewrite followed by a scalar setPath keeps both changes', () => {
    const handle = parseConfigFile(COMMENTED)
    deletePath(handle, 0, ['gamma', 'nested'])
    setPath(handle, 0, ['beta'], 'three')
    expect(serializeConfigFile(handle)).toBe(
      COMMENTED.replace('  nested: true\n', '').replace('beta: two', 'beta: three'),
    )
  })

  it('patches two different pre-existing documents of one handle', () => {
    const handle = parseConfigFile(THREE_DOCS)
    setPath(handle, 0, ['a'], 10)
    setPath(handle, 2, ['c'], 30)
    expect(serializeConfigFile(handle)).toBe('a: 10\n---\nb: 2\n---\nc: 30\n')
  })
})

// ---------------------------------------------------------------------------
// 5b. Surgical key-add (splice) on the real job template + splice lifecycle
// ---------------------------------------------------------------------------

describe('surgical key-add on the job template', () => {
  it('adding a key under project_defaults inserts exactly one line', () => {
    const src = readTemplate('job_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['project_defaults', 'timeout_seconds'], 3600)
    const out = serializeConfigFile(handle)
    expect(expectPureInsertion(src, out)).toEqual(['  timeout_seconds: 3600'])
    expect(parseConfigFile(out).errors).toHaveLength(0)
    expect(toJS(parseConfigFile(out), 0)).toMatchObject({
      project_defaults: { timeout_seconds: 3600 },
    })
  })

  it('adding a nested subtree under project_defaults inserts exactly its lines', () => {
    const src = readTemplate('job_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['project_defaults', 'schedule', 'quartz_cron_expression'], '0 0 2 * * ?')
    const out = serializeConfigFile(handle)
    expect(expectPureInsertion(src, out)).toEqual([
      '  schedule:',
      '    quartz_cron_expression: 0 0 2 * * ?',
    ])
    expect(toJS(parseConfigFile(out), 0)).toMatchObject({
      project_defaults: { schedule: { quartz_cron_expression: '0 0 2 * * ?' } },
    })
  })

  it('two key-adds into the same map keep insertion order', () => {
    const handle = parseConfigFile(COMMENTED)
    setPath(handle, 0, ['epsilon'], 'first')
    setPath(handle, 0, ['zeta'], 'second')
    expect(serializeConfigFile(handle)).toBe(COMMENTED + 'epsilon: first\nzeta: second\n')
  })

  // This fixture contains a `# ` line (hash + trailing space) that any
  // document REWRITE would normalize to `#` — so byte-exact output below
  // proves the splice machinery never fell back to a rewrite.
  const PATHOLOGICAL = '# trailing space on next comment line\n# \nalpha: 1\n'

  it('updating a value inside a spliced subtree updates only the splice', () => {
    const handle = parseConfigFile(PATHOLOGICAL)
    setPath(handle, 0, ['sched', 'cron'], 'a')
    setPath(handle, 0, ['sched', 'cron'], 'b')
    setPath(handle, 0, ['sched', 'tz'], 'UTC')
    expect(serializeConfigFile(handle)).toBe(PATHOLOGICAL + 'sched:\n  cron: b\n  tz: UTC\n')
  })

  it('deleting a key inside a spliced subtree updates only the splice', () => {
    const handle = parseConfigFile(PATHOLOGICAL)
    setPath(handle, 0, ['sched', 'cron'], 'a')
    setPath(handle, 0, ['sched', 'tz'], 'UTC')
    deletePath(handle, 0, ['sched', 'cron'])
    expect(serializeConfigFile(handle)).toBe(PATHOLOGICAL + 'sched:\n  tz: UTC\n')
  })

  it('deleting a spliced key entirely restores the original bytes', () => {
    const handle = parseConfigFile(PATHOLOGICAL)
    setPath(handle, 0, ['sched', 'cron'], 'a')
    deletePath(handle, 0, ['sched'])
    expect(serializeConfigFile(handle)).toBe(PATHOLOGICAL)
  })

  it('adding a key to a file without a trailing newline keeps it newline-less', () => {
    const handle = parseConfigFile('a: 1\nb: 2')
    setPath(handle, 0, ['c'], 3)
    expect(serializeConfigFile(handle)).toBe('a: 1\nb: 2\nc: 3')
  })
})

// ---------------------------------------------------------------------------
// 5c. Remaining rewrite blast radius, pinned on the real job template
// ---------------------------------------------------------------------------

describe('rewrite blast radius on the job template (pinned)', () => {
  it('a key-delete in doc 0 normalizes only doc 0; sibling docs stay byte-exact', () => {
    const src = readTemplate('job_config_env.yaml.tmpl')
    const handle = parseConfigFile(src)
    deletePath(handle, 0, ['project_defaults', 'queue'])
    const out = serializeConfigFile(handle)
    // Docs 1..3 keep their exact bytes (everything from the first `---`).
    const tail = src.slice(src.indexOf('\n---\n') + 1)
    expect(out.endsWith(tail)).toBe(true)
    // The deleted key (and the comment block it owned) is gone.
    expect(out).not.toMatch(/^ {2}queue:/m)
    expect(parseConfigFile(out).errors).toHaveLength(0)
    // Exact normalized output of the whole file, so any change to the
    // rewrite blast radius is regression-visible.
    expect(out).toMatchSnapshot()
  })
})

// ---------------------------------------------------------------------------
// 6. Nested creation
// ---------------------------------------------------------------------------

describe('nested creation', () => {
  it('setPath creates intermediate maps for missing path segments', () => {
    const handle = parseConfigFile('top: 1\n')
    setPath(handle, 0, ['cfg', 'nested', 'deep'], 'v')
    expect(serializeConfigFile(handle)).toBe('top: 1\ncfg:\n  nested:\n    deep: v\n')
  })

  it('numeric path segments create sequences', () => {
    const handle = parseConfigFile('top: 1\n')
    setPath(handle, 0, ['list', 0], 'item')
    expect(serializeConfigFile(handle)).toBe('top: 1\nlist:\n  - item\n')
  })

  it('creating a nested path keeps existing comments intact', () => {
    const handle = parseConfigFile('# head\nexisting: 1\n')
    setPath(handle, 0, ['obj', 'a'], 1)
    expect(serializeConfigFile(handle)).toBe('# head\nexisting: 1\nobj:\n  a: 1\n')
  })

  it('setPath populates a document whose contents are empty', () => {
    const handle = parseConfigFile('a: 1\n---\n# placeholder doc\n---\nc: 3\n')
    setPath(handle, 1, ['pipeline'], 'p2')
    // Sibling docs stay byte-exact; the placeholder comment is relocated
    // above the new contents of the rewritten document.
    expect(serializeConfigFile(handle)).toBe(
      'a: 1\n---\n# placeholder doc\npipeline: p2\n---\nc: 3\n',
    )
  })

  it('setPath can replace a whole subtree with a plain object', () => {
    const handle = parseConfigFile('wrap:\n  old: 1\nkeep: 2\n')
    setPath(handle, 0, ['wrap'], { fresh: true, items: [1, 2] })
    expect(serializeConfigFile(handle)).toBe(
      'wrap:\n  fresh: true\n  items:\n    - 1\n    - 2\nkeep: 2\n',
    )
  })
})

// ---------------------------------------------------------------------------
// 7. Error surfacing
// ---------------------------------------------------------------------------

describe('error surfacing', () => {
  it('does not throw on malformed YAML and exposes errors on the handle', () => {
    const handle = parseConfigFile(MALFORMED)
    expect(handle.errors.length).toBeGreaterThan(0)
  })

  it('serializes malformed sources byte-identically when unmutated', () => {
    expect(roundTrip(MALFORMED)).toBe(MALFORMED)
    expect(roundTrip(MALFORMED_MULTI)).toBe(MALFORMED_MULTI)
  })

  it('blocks every mutation on a handle with parse errors', () => {
    const handle = parseConfigFile(MALFORMED_MULTI)
    expect(() => setPath(handle, 0, ['good'], 2)).toThrow(/parse error/)
    expect(() => deletePath(handle, 0, ['good'])).toThrow(/parse error/)
    expect(() => addDocument(handle, { x: 1 })).toThrow(/parse error/)
    expect(() => removeDocument(handle, 0)).toThrow(/parse error/)
  })

  it('still allows reading a broken file', () => {
    const handle = parseConfigFile(MALFORMED_MULTI)
    expect(getPath(handle, 0, ['good'])).toBe(1)
    expect(toJS(handle, 0)).toEqual({ good: 1 })
  })
})

// ---------------------------------------------------------------------------
// 8. Edge files
// ---------------------------------------------------------------------------

describe('edge files', () => {
  it('round-trips the empty string', () => {
    const handle = parseConfigFile('')
    expect(documentCount(handle)).toBe(0)
    expect(handle.errors).toHaveLength(0)
    expect(serializeConfigFile(handle)).toBe('')
  })

  it('adding the first document to an empty file emits no separator', () => {
    const handle = parseConfigFile('')
    expect(addDocument(handle, { a: 1 })).toBe(0)
    expect(serializeConfigFile(handle)).toBe('a: 1\n')
  })

  it('round-trips a comments-only file byte-identically (zero documents)', () => {
    const src = '# note a\n# note b\n'
    const handle = parseConfigFile(src)
    expect(documentCount(handle)).toBe(0)
    expect(serializeConfigFile(handle)).toBe(src)
  })

  it('adding a document to a comments-only file keeps the comments', () => {
    const handle = parseConfigFile('# note a\n# note b\n')
    addDocument(handle, { x: 1 })
    const out = serializeConfigFile(handle)
    expect(out).toBe('# note a\n# note b\n---\nx: 1\n')
    expect(documentCount(parseConfigFile(out))).toBe(1)
  })

  it('round-trips a file without a trailing newline byte-identically', () => {
    expect(roundTrip('a: 1\nb: 2')).toBe('a: 1\nb: 2')
  })

  it('patching a file without a trailing newline does not add one', () => {
    const handle = parseConfigFile('a: 1\nb: 2')
    setPath(handle, 0, ['b'], 3)
    expect(serializeConfigFile(handle)).toBe('a: 1\nb: 3')
  })

  it('appending a document to a file without a trailing newline inserts one', () => {
    const handle = parseConfigFile('a: 1\nb: 2')
    addDocument(handle, { c: 3 })
    expect(serializeConfigFile(handle)).toBe('a: 1\nb: 2\n---\nc: 3\n')
  })

  it('round-trips and patches CRLF files without normalizing line endings', () => {
    const src = 'a: 1\r\nb: two\r\nc: 3\r\n'
    expect(roundTrip(src)).toBe(src)
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['b'], 'dos')
    expect(serializeConfigFile(handle)).toBe('a: 1\r\nb: dos\r\nc: 3\r\n')
  })

  it('a key-add on a CRLF file emits CRLF (no mixed endings)', () => {
    const handle = parseConfigFile('a: 1\r\nb: 2\r\n')
    setPath(handle, 0, ['c'], 3)
    expect(serializeConfigFile(handle)).toBe('a: 1\r\nb: 2\r\nc: 3\r\n')
  })

  it('a rewrite on a CRLF file emits CRLF throughout (no mixed endings)', () => {
    const handle = parseConfigFile('a: 1\r\nb: 2\r\nwrap:\r\n  only: 1\r\n')
    deletePath(handle, 0, ['b'])
    const out = serializeConfigFile(handle)
    expect(out).toBe('a: 1\r\nwrap:\r\n  only: 1\r\n')
    expect(out).not.toMatch(/[^\r]\n/)
  })

  it('an added document on a CRLF file emits CRLF', () => {
    const handle = parseConfigFile('a: 1\r\n')
    addDocument(handle, { b: 2 })
    expect(serializeConfigFile(handle)).toBe('a: 1\r\n---\r\nb: 2\r\n')
  })
})

// ---------------------------------------------------------------------------
// 9. Scalar quoting and typing
// ---------------------------------------------------------------------------

describe('scalar quoting and typing', () => {
  it('keeps double quotes when replacing a double-quoted scalar', () => {
    const handle = parseConfigFile(QUOTING)
    setPath(handle, 0, ['name'], 'main')
    expect(serializeConfigFile(handle)).toBe(QUOTING.replace('"quoted"', '"main"'))
  })

  it('keeps plain style for plain replacements', () => {
    const handle = parseConfigFile(QUOTING)
    setPath(handle, 0, ['note'], 'still_plain')
    expect(serializeConfigFile(handle)).toBe(QUOTING.replace('note: plain', 'note: still_plain'))
  })

  it('quotes strings that would otherwise re-parse as another type', () => {
    for (const [text, quoted] of [
      ['true', '"true"'],
      ['42', '"42"'],
      ['null', '"null"'],
      ['', '""'],
    ] as const) {
      const handle = parseConfigFile(QUOTING)
      setPath(handle, 0, ['note'], text)
      const out = serializeConfigFile(handle)
      expect(out).toBe(QUOTING.replace('note: plain', `note: ${quoted}`))
      expect(toJS(parseConfigFile(out), 0)).toMatchObject({ note: text })
    }
  })

  it('quotes strings that YAML 1.1 (PyYAML) resolves as non-strings', () => {
    // These files are read back by PyYAML SafeLoader (YAML 1.1), which
    // resolves more plain scalars than YAML 1.2 core does.
    for (const [text, quoted] of [
      ['yes', '"yes"'],
      ['Yes', '"Yes"'],
      ['YES', '"YES"'],
      ['no', '"no"'],
      ['On', '"On"'],
      ['off', '"off"'],
      ['OFF', '"OFF"'],
      ['y', '"y"'],
      ['N', '"N"'],
      ['1:30', '"1:30"'],
      ['1:30:00', '"1:30:00"'],
      ['1_000', '"1_000"'],
      ['0b101', '"0b101"'],
      ['0o777', '"0o777"'],
      ['1_0.5', '"1_0.5"'],
      ['2024-01-01', '"2024-01-01"'],
      ['2024-01-01 10:20:30', '"2024-01-01 10:20:30"'],
      ['<<', '"<<"'],
    ] as const) {
      const handle = parseConfigFile(QUOTING)
      setPath(handle, 0, ['note'], text)
      const out = serializeConfigFile(handle)
      expect(out).toBe(QUOTING.replace('note: plain', `note: ${quoted}`))
      expect(toJS(parseConfigFile(out), 0)).toMatchObject({ note: text })
    }
  })

  it('leaves 1.1-lookalike strings plain when they are unambiguous', () => {
    for (const text of ['yesterday', 'onward', 'Nope', 'y2k', 'v1:30', '${catalog}']) {
      const handle = parseConfigFile(QUOTING)
      setPath(handle, 0, ['note'], text)
      expect(serializeConfigFile(handle)).toBe(
        QUOTING.replace('note: plain', `note: ${text}`),
      )
    }
  })

  it('quotes 1.1-ambiguous strings in spliced new keys too', () => {
    const handle = parseConfigFile(QUOTING)
    setPath(handle, 0, ['extra'], 'on')
    expect(serializeConfigFile(handle)).toBe(QUOTING + 'extra: "on"\n')
  })

  it('patches block scalars in place', () => {
    const src = 'msg: |\n  line one\n  line two\nother: 1\n'
    const handle = parseConfigFile(src)
    setPath(handle, 0, ['msg'], 'replaced text')
    const out = serializeConfigFile(handle)
    // The chomp indicator becomes |- because the new value has no
    // trailing newline; `other` is untouched.
    expect(out).toBe('msg: |-\n  replaced text\nother: 1\n')
    expect(toJS(parseConfigFile(out), 0)).toEqual({ msg: 'replaced text', other: 1 })
  })

  it('writes numbers, booleans, and null as plain scalars', () => {
    const handle = parseConfigFile(QUOTING)
    setPath(handle, 0, ['count'], 42)
    setPath(handle, 0, ['flag'], false)
    setPath(handle, 0, ['empty_target'], null)
    const out = serializeConfigFile(handle)
    expect(out).toBe(
      QUOTING.replace('count: 10', 'count: 42')
        .replace('flag: true', 'flag: false')
        .replace('empty_target: value', 'empty_target: null'),
    )
    expect(toJS(parseConfigFile(out), 0)).toMatchObject({
      count: 42,
      flag: false,
      empty_target: null,
    })
  })

  it('drops quotes when a quoted slot receives a number', () => {
    const handle = parseConfigFile(QUOTING)
    setPath(handle, 0, ['name'], 99)
    const out = serializeConfigFile(handle)
    expect(out).toBe(QUOTING.replace('"quoted"', '99'))
    expect(toJS(parseConfigFile(out), 0)).toMatchObject({ name: 99 })
  })

  it('treats undefined as null', () => {
    const handle = parseConfigFile(QUOTING)
    setPath(handle, 0, ['count'], undefined)
    expect(serializeConfigFile(handle)).toBe(QUOTING.replace('count: 10', 'count: null'))
  })

  it('patches values inside flow collections and keeps them valid', () => {
    const handle = parseConfigFile('flow: { a: 1, b: 2 }\n')
    setPath(handle, 0, ['flow', 'a'], 9)
    expect(serializeConfigFile(handle)).toBe('flow: { a: 9, b: 2 }\n')

    const handle2 = parseConfigFile('flow: { a: 1, b: 2 }\n')
    setPath(handle2, 0, ['flow', 'a'], 'x,y')
    const out = serializeConfigFile(handle2)
    expect(toJS(parseConfigFile(out), 0)).toEqual({ flow: { a: 'x,y', b: 2 } })
  })

  it('patches sequence items', () => {
    const handle = parseConfigFile('items:\n  - one\n  - two\n')
    setPath(handle, 0, ['items', 1], 'dos')
    expect(serializeConfigFile(handle)).toBe('items:\n  - one\n  - dos\n')
  })
})

// ---------------------------------------------------------------------------
// 10. API basics
// ---------------------------------------------------------------------------

describe('API basics', () => {
  it('getPath reads scalars and returns undefined for missing paths', () => {
    const handle = parseConfigFile(COMMENTED)
    expect(getPath(handle, 0, ['alpha'])).toBe(1)
    expect(getPath(handle, 0, ['gamma', 'nested'])).toBe(true)
    expect(getPath(handle, 0, ['missing'])).toBeUndefined()
    expect(getPath(handle, 0, ['gamma', 'missing'])).toBeUndefined()
  })

  it('toJS returns a plain-JS snapshot of one document', () => {
    const handle = parseConfigFile(MULTI_DOC)
    expect(toJS(handle, 0)).toEqual({ alpha: 1 })
    expect(toJS(handle, 1)).toEqual({ pipeline: 'beta', edition: 'PRO' })
    expect(toJS(handle, 2)).toEqual({ pipeline: 'gamma' })
  })

  it('deletePath of a missing path is a no-op that keeps bytes identical', () => {
    const handle = parseConfigFile(COMMENTED)
    deletePath(handle, 0, ['nope'])
    deletePath(handle, 0, ['gamma', 'nope'])
    expect(serializeConfigFile(handle)).toBe(COMMENTED)
  })

  it('deletePath does not cascade-delete emptied parents', () => {
    const handle = parseConfigFile('wrap:\n  only: 1\nkeep: 2\n')
    deletePath(handle, 0, ['wrap', 'only'])
    expect(serializeConfigFile(handle)).toBe('wrap: {}\nkeep: 2\n')
  })

  it('rejects out-of-range document indices', () => {
    const handle = parseConfigFile('a: 1\n')
    expect(() => getPath(handle, 1, ['a'])).toThrow(RangeError)
    expect(() => setPath(handle, -1, ['a'], 2)).toThrow(RangeError)
    expect(() => deletePath(handle, 1, ['a'])).toThrow(RangeError)
    expect(() => toJS(handle, 1)).toThrow(RangeError)
    expect(() => removeDocument(handle, 1)).toThrow(RangeError)
  })

  it('documentCount tracks adds and removes', () => {
    const handle = parseConfigFile(THREE_DOCS)
    expect(documentCount(handle)).toBe(3)
    addDocument(handle, { d: 4 })
    expect(documentCount(handle)).toBe(4)
    removeDocument(handle, 1)
    expect(documentCount(handle)).toBe(3)
  })

  it('mutations on an added document serialize correctly', () => {
    const handle = parseConfigFile('a: 1\n')
    const index = addDocument(handle, { pipeline: 'p' })
    setPath(handle, index, ['serverless'], true)
    expect(serializeConfigFile(handle)).toBe('a: 1\n---\npipeline: p\nserverless: true\n')
  })
})
