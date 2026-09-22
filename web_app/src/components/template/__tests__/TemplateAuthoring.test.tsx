import { useState } from 'react'
import { TooltipProvider } from '@/components/ui/tooltip'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ParameterValueInput } from '../ParameterValueInput'
import { TemplateFieldBinding } from '../TemplateFieldBinding'
import { TemplateParameterProvider } from '../TemplateParameterContext'
import { CreateTemplateDialog } from '../CreateTemplateDialog'
import { TemplateParamsCard } from '@/components/entity/TemplateParamsCard'
import { parseFlowgroupFile, readTemplateParams, selectTemplate, serializeFlowgroupFile } from '@/lib/flowgroup-doc'
import { buildTemplateDraft, inspectTemplateReferences } from '@/lib/template-document'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useLayoutStore } from '@/store/layoutStore'
vi.mock('@/hooks/useFiles', () => ({ useFileList: () => ({ data: { name: '', path: '', type: 'directory', children: [] }, isLoading: false }) }))
function wrapper({ children }: { children: React.ReactNode }) { return <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><TooltipProvider>{children}</TooltipProvider></QueryClientProvider> }
beforeEach(() => { useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null }); useLayoutStore.setState({ viewerMode: false }); Element.prototype.scrollIntoView = vi.fn() })
const yaml = '# keep me\nname: ingest\nparameters:\n  - name: columns\n    type: array\n    required: true\nactions:\n  - name: load\n    type: load\n    source: "{{ columns }}"\n'
describe('template authoring interactions', () => {
  it('edits nested values and reports malformed input before any commit', () => {
    const changes = vi.fn(), validity = vi.fn()
    render(<ParameterValueInput id="value" label="Columns" value={[]} onChange={changes} onValidityChange={validity} />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Edit Columns as YAML' }))
    expect(screen.getByLabelText('Columns')).toHaveAttribute('aria-describedby', 'value-hint value-issue')
    fireEvent.change(screen.getByLabelText('Columns'), { target: { value: '[unclosed' } })
    expect(validity).toHaveBeenLastCalledWith(false)
    fireEvent.blur(screen.getByLabelText('Columns'))
    expect(changes).not.toHaveBeenCalled()
    fireEvent.keyDown(screen.getByLabelText('Columns'), { key: 'Escape' })
    expect(validity).toHaveBeenLastCalledWith(true)
    expect(screen.getByLabelText('Columns')).toHaveValue('[]')
    fireEvent.change(screen.getByLabelText('Columns'), { target: { value: '[{name: id}, 0, false]' } })
    fireEvent.blur(screen.getByLabelText('Columns'))
    expect(changes).toHaveBeenCalledWith([{ name: 'id' }, 0, false])
  })
  it('distinguishes an empty string, null and false without deletion', () => {
    const changes = vi.fn()
    render(<ParameterValueInput id="value" label="Default" value="text" onChange={changes} />, { wrapper })
    fireEvent.change(screen.getByLabelText('Default'), { target: { value: '' } })
    fireEvent.blur(screen.getByLabelText('Default'))
    expect(changes).toHaveBeenLastCalledWith('')
    fireEvent.change(screen.getByLabelText('Default value format'), { target: { value: 'null' } })
    expect(changes).toHaveBeenLastCalledWith(null)
    fireEvent.change(screen.getByLabelText('Default value format'), { target: { value: 'boolean' } })
    expect(changes).toHaveBeenLastCalledWith(false)
  })
  it('inserts a declared parameter into an expression without saving a file', () => {
    const onSet = vi.fn()
    const params = readTemplateParams(selectTemplate(parseFlowgroupFile(yaml))!)
    render(<TemplateParameterProvider value={params}><TemplateFieldBinding id="columns" label="Partition columns" value={[]} disabled={false} onSet={onSet} onUnset={vi.fn()} helpPath={['partition_columns']}><span>Literal list</span></TemplateFieldBinding></TemplateParameterProvider>, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Expression' }))
    fireEvent.change(screen.getByLabelText('Insert parameter into Partition columns'), { target: { value: 'columns' } })
    expect(onSet).toHaveBeenCalledWith('{{ columns }}')
  })
  it('shows affected uses before deleting only a declaration', () => {
    const file = parseFlowgroupFile(yaml), template = selectTemplate(file)!
    const commit = vi.fn((fn) => { fn(template.body); return true })
    render(<TemplateParamsCard params={readTemplateParams(template)} templateName="ingest" readOnly={false} references={inspectTemplateReferences(template.body)} commit={commit} />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Delete parameter columns' }))
    expect(commit).not.toHaveBeenCalled()
    expect(screen.getByRole('alertdialog')).toHaveTextContent('actions.0.source')
    fireEvent.click(screen.getByRole('button', { name: 'Delete declaration' }))
    expect(serializeFlowgroupFile(file)).toContain('{{ columns }}')
    expect(readTemplateParams(template)).toHaveLength(0)
  })
  it('creates a dirty unsaved draft and prevents reusing an occupied draft path', () => {
    const onCreated = vi.fn()
    render(<CreateTemplateDialog open onOpenChange={vi.fn()} onCreated={onCreated} />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Create draft' }))
    const buffer = useWorkspaceStore.getState().buffers[0]
    expect(buffer).toMatchObject({ path: 'templates/new_template.yaml', isDirty: true, isNew: true, exists: false })
    expect(onCreated).toHaveBeenCalledWith(buffer.path)
    expect(screen.getByRole('button', { name: 'Create draft' })).toBeDisabled()
  })
  it('duplicates from the existing source while retaining comments and expressions', () => {
    const output = buildTemplateDraft('ingest_copy', yaml)
    expect(output).toContain('# keep me')
    expect(output).toContain('{{ columns }}')
    expect(output).toContain('name: ingest_copy')
  })
  it('changing value format preserves distinct empty values in the controlled parent', () => {
    function Fixture() { const [value, setValue] = useState<unknown>(null); return <><ParameterValueInput id="v" label="Value" value={value} onChange={setValue} /><output>{JSON.stringify(value)}</output></> }
    render(<Fixture />, { wrapper })
    fireEvent.change(screen.getByLabelText('Value value format'), { target: { value: 'array' } })
    expect(screen.getByRole('status')).toHaveTextContent('[]')
    fireEvent.change(screen.getByLabelText('Value value format'), { target: { value: 'object' } })
    expect(screen.getByRole('status')).toHaveTextContent('{}')
    fireEvent.change(screen.getByLabelText('New Value property'), { target: { value: 'columns' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add property' }))
    fireEvent.change(screen.getByLabelText('Value.columns value format'), { target: { value: 'array' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add Value.columns item' }))
    fireEvent.change(screen.getByLabelText('Value.columns item 1'), { target: { value: 'customer_id' } })
    fireEvent.blur(screen.getByLabelText('Value.columns item 1'))
    expect(screen.getByRole('status')).toHaveTextContent('{"columns":["customer_id"]}')
  })
  it('propagates invalid nested numeric input and restores it with Escape', () => {
    const validity = vi.fn()
    function Fixture() { const [value, setValue] = useState<unknown>({ count: 1 }); return <ParameterValueInput id="v" label="Value" value={value} onChange={setValue} onValidityChange={validity} /> }
    render(<Fixture />, { wrapper })
    const input = screen.getByLabelText('Value.count')
    fireEvent.change(input, { target: { value: 'not a number' } })
    expect(validity).toHaveBeenLastCalledWith(false)
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(input).toHaveValue('1')
    expect(validity).toHaveBeenLastCalledWith(true)
  })
  it('rejects duplicate object property names and keeps their values', () => {
    const changes = vi.fn(), validity = vi.fn()
    render(<ParameterValueInput id="v" label="Value" value={{ one: 1, two: 2 }} onChange={changes} onValidityChange={validity} />, { wrapper })
    const input = screen.getByLabelText('Value property 1 name')
    fireEvent.change(input, { target: { value: 'two' } })
    fireEvent.blur(input)
    expect(validity).toHaveBeenLastCalledWith(false)
    expect(changes).not.toHaveBeenCalled()
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(input).toHaveValue('one')
    expect(validity).toHaveBeenLastCalledWith(true)
  })

})
