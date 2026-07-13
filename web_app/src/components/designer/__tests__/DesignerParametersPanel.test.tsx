// Panel tests: a real DesignerParametersPanel rendered against a live
// template handle whose `commit` applies the mutator and re-derives the
// parameters — the same write-through the designer uses, minus the network.

import { useReducer, type ReactNode } from 'react'
import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { TooltipProvider } from '@/components/ui/tooltip'
import {
  parseFlowgroupFile,
  readTemplateParams,
  selectTemplate,
  serializeFlowgroupFile,
} from '@/lib/flowgroup-doc'
import type { FlowgroupFileHandle } from '@/lib/flowgroup-doc'
import { DesignerParametersPanel } from '../DesignerParametersPanel'

const TEMPLATE = `name: csv_ingestion_template
parameters:
  - name: table_name
    required: true
  - name: table_properties
    default: {}
actions:
  - name: load_{{ table_name }}
    type: load
    source:
      type: cloudfiles
      path: "/land/{{ table_name }}"
      format: csv
    target: v_{{ table_name }}
`

function renderPanel(yaml: string, readOnly = false) {
  const file = parseFlowgroupFile(yaml)
  const t = selectTemplate(file)!
  function Harness() {
    const [, force] = useReducer((x: number) => x + 1, 0)
    return (
      <DesignerParametersPanel
        params={readTemplateParams(t)}
        templateName={t.info.name}
        readOnly={readOnly}
        commit={(mutator) => {
          mutator(t.body)
          force()
        }}
      />
    )
  }
  // DesignerParametersPanel renders config field primitives whose "(i)" help
  // icons need a TooltipProvider ancestor (production has the global one in
  // main.tsx). No SchemaKindProvider here, so no QueryClient is needed.
  render(<Harness />, {
    wrapper: ({ children }: { children: ReactNode }) => (
      <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
    ),
  })
  return { file }
}

function currentParams(file: FlowgroupFileHandle) {
  return readTemplateParams(selectTemplate(parseFlowgroupFile(serializeFlowgroupFile(file)))!)
}

describe('DesignerParametersPanel', () => {
  it('renders a row per declared parameter', () => {
    renderPanel(TEMPLATE)
    expect(screen.getByDisplayValue('table_name')).toBeDefined()
    expect(screen.getByDisplayValue('table_properties')).toBeDefined()
    expect(screen.getByText('Parameters (2)')).toBeDefined()
  })

  it('add parameter appends a uniquely named row', () => {
    const { file } = renderPanel(TEMPLATE)
    fireEvent.click(screen.getByRole('button', { name: 'Add parameter' }))
    const params = currentParams(file)
    expect(params).toHaveLength(3)
    expect(params[2].name).toBe('parameter')
  })

  it('renaming a parameter writes through', () => {
    const { file } = renderPanel(TEMPLATE)
    const nameInput = screen.getByLabelText('Parameter 1 name')
    fireEvent.change(nameInput, { target: { value: 'entity_name' } })
    fireEvent.blur(nameInput)
    expect(currentParams(file).map((p) => p.name)).toEqual(['entity_name', 'table_properties'])
  })

  it('editing the default parses YAML flow values (array round-trips)', () => {
    const { file } = renderPanel(TEMPLATE)
    const defaults = screen.getAllByLabelText('Default')
    fireEvent.change(defaults[0], { target: { value: '[]' } })
    fireEvent.blur(defaults[0])
    expect(currentParams(file)[0].default).toEqual([])
  })

  it('clearing the default deletes the key', () => {
    const { file } = renderPanel(TEMPLATE)
    // table_properties has default {}; clearing removes it.
    const defaults = screen.getAllByLabelText('Default')
    fireEvent.change(defaults[1], { target: { value: '' } })
    fireEvent.blur(defaults[1])
    expect('default' in currentParams(file)[1].raw).toBe(false)
  })

  it('deletes a parameter', () => {
    const { file } = renderPanel(TEMPLATE)
    fireEvent.click(screen.getByRole('button', { name: 'Delete parameter table_name' }))
    expect(currentParams(file).map((p) => p.name)).toEqual(['table_properties'])
  })

  it('read-only (dirty guard) disables add and delete', () => {
    renderPanel(TEMPLATE, true)
    expect(screen.getByRole('button', { name: 'Add parameter' })).toHaveProperty('disabled', true)
    expect(screen.getByRole('button', { name: 'Delete parameter table_name' })).toHaveProperty(
      'disabled',
      true,
    )
  })
})
