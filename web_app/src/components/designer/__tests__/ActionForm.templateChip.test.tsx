// The chip routing: a string field whose value contains `{{ param }}` renders
// through the param-variant TokenField (a mono chip), while a literal value
// renders the ordinary text field. A minimal inline spec isolates the engine
// behaviour from any real action spec.

import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { listActions, parseFlowgroupFile, selectFlowgroup } from '@/lib/flowgroup-doc'
import { ActionForm } from '../ActionForm'
import { flowgroupProviders } from './actionFormTestSupport'
import type { ActionSubTypeSpec } from '../specs/types'

const SPEC: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'sql',
  title: 'SQL transform',
  groups: [{ fields: [{ path: ['target'], label: 'Target', widget: 'text' }] }],
}

function renderTarget(target: string) {
  const yaml = `pipeline: p
flowgroup: fg
actions:
  - name: t1
    type: transform
    transform_type: sql
    target: "${target}"
`
  const file = parseFlowgroupFile(yaml)
  const fg = selectFlowgroup(file, 'fg')!
  const action = listActions(fg).find((a) => a.name === 't1')!
  render(
    <ActionForm
      spec={SPEC}
      action={action}
      actionId="t1"
      commit={() => {}}
      rename={async () => true}
      readOnly={false}
      presetBadges={[]}
      onRenamed={() => {}}
      onEditCode={() => {}}
    />,
    { wrapper: flowgroupProviders() },
  )
}

describe('ActionForm — {{ param }} chip', () => {
  it('renders a param chip for a field bound to a template parameter', () => {
    renderTarget('{{ tbl }}')
    expect(screen.getByText('Bound to template parameter tbl.')).toBeDefined()
    expect(screen.getByText('tbl')).toBeDefined()
  })

  it('renders a plain text field for a literal value (no chip)', () => {
    renderTarget('v_out')
    expect(screen.queryByText(/Bound to template parameter/)).toBeNull()
    expect(screen.getByDisplayValue('v_out')).toBeDefined()
  })
})
