import { describe, expect, it } from 'vitest'
import { computeIssues, groupHasValueOrIssue, pathKey } from '../formModel'
import type { ActionSubTypeSpec, FieldGroup } from '../specs/types'

// Wrap a single group so the REAL `computeIssues` can be exercised against it
// (the helper filters the whole-spec issue map down to this group's fields).
const specOf = (group: FieldGroup): ActionSubTypeSpec => ({
  kind: 'load',
  subType: 'test',
  title: 'Test',
  groups: [group],
})

describe('formModel — groupHasValueOrIssue (auto-expand)', () => {
  it('true when a visible field has a present value in raw', () => {
    const group: FieldGroup = {
      title: 'Options',
      fields: [{ path: ['note'], label: 'Note', widget: 'text' }],
    }
    expect(groupHasValueOrIssue(group, { note: 'hi' })).toBe(true)
  })

  it('true when computeIssues flags one of the group fields', () => {
    const group: FieldGroup = {
      title: 'Required',
      fields: [{ path: ['name'], label: 'Name', widget: 'text', required: true }],
    }
    // Guard the premise with the real computeIssues: an absent required field
    // is genuinely flagged, and no field carries a present value.
    expect(computeIssues(specOf(group), {}).has(pathKey(['name']))).toBe(true)
    expect(groupHasValueOrIssue(group, {})).toBe(true)
  })

  it('false for an all-empty, issue-free collapsed/advanced group', () => {
    const group: FieldGroup = {
      title: 'Advanced',
      collapsed: true,
      advanced: true,
      fields: [{ path: ['note'], label: 'Note', widget: 'text' }],
    }
    expect(computeIssues(specOf(group), {}).size).toBe(0)
    expect(groupHasValueOrIssue(group, {})).toBe(false)
  })

  it('a present value on a field hidden by field.visibleWhen does not count', () => {
    const group: FieldGroup = {
      title: 'Options',
      fields: [
        {
          path: ['adv'],
          label: 'Advanced',
          widget: 'text',
          visibleWhen: (raw) => raw.mode === 'advanced',
        },
      ],
    }
    // `adv` has a value but the field is hidden (mode !== 'advanced').
    expect(groupHasValueOrIssue(group, { adv: 'x' })).toBe(false)
  })

  it('a group hidden by its own visibleWhen returns false even with a present value', () => {
    const group: FieldGroup = {
      title: 'Conditional',
      visibleWhen: (raw) => raw.mode === 'on',
      fields: [{ path: ['note'], label: 'Note', widget: 'text' }],
    }
    expect(groupHasValueOrIssue(group, { note: 'hi' })).toBe(false)
  })

  it('non-object raw is treated as empty', () => {
    const group: FieldGroup = {
      title: 'Options',
      fields: [{ path: ['note'], label: 'Note', widget: 'text' }],
    }
    expect(groupHasValueOrIssue(group, undefined)).toBe(false)
    expect(groupHasValueOrIssue(group, 'nope')).toBe(false)
  })
})
