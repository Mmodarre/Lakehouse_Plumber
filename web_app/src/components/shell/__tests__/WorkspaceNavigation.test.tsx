import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it } from 'vitest'
import { WorkspaceNavigation } from '../WorkspaceNavigation'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useLayoutStore } from '../../../store/layoutStore'
import { encodeTab, useNavigationStore } from '../../../workspace/navigation'
beforeEach(() => {
  window.history.replaceState({}, '', '/')
  useNavigationStore.getState().reset()
  useWorkspaceStore.getState().closeAllBuffers()
  useWorkspaceStore.setState({ projectRoot: '/project' })
  useLayoutStore.setState({ focusMode: false })
})
describe('navigation integration', () => {
  it('waits for initial project binding before applying the requested deep link', async () => {
    const linked = { kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'yaml' } as const
    window.history.replaceState({}, '', `/?tab=${encodeURIComponent(encodeTab(linked))}`)
    useWorkspaceStore.setState({ projectRoot: null })
    render(<WorkspaceNavigation />)
    expect(useWorkspaceStore.getState().activePath).toBeNull()
    act(() => useWorkspaceStore.setState({ projectRoot: '/project' }))
    await waitFor(() => expect(useNavigationStore.getState().entries).toEqual([linked]))
    expect(new URLSearchParams(window.location.search).get('tab')).toBe(encodeTab(linked))
  })

  it('keeps forward history on back/forward and leaves word navigation inside text inputs alone', async () => {
    render(<><WorkspaceNavigation /><textarea aria-label="Editor" /><div tabIndex={-1} data-workspace-center /></>)
    act(() => useWorkspaceStore.getState().openProjectMap())
    act(() => useWorkspaceStore.getState().openConfigTab('lhp.yaml', 'project'))
    await waitFor(() => expect(useNavigationStore.getState().entries).toHaveLength(2))
    fireEvent.click(screen.getByRole('button', { name: 'Go back' }))
    expect(useNavigationStore.getState().index).toBe(0)
    expect(screen.getByRole('button', { name: 'Go forward' })).not.toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Go forward' }))
    expect(useNavigationStore.getState().index).toBe(1)
    const input = screen.getByRole('textbox', { name: 'Editor' })
    input.focus()
    fireEvent.keyDown(input, { altKey: true, key: 'ArrowLeft' })
    expect(useNavigationStore.getState().index).toBe(1)
    fireEvent.keyDown(window, { altKey: true, key: 'ArrowLeft' })
    expect(useNavigationStore.getState().index).toBe(0)
    fireEvent.keyDown(input, { ctrlKey: true, shiftKey: true, key: 'F' })
    expect(useLayoutStore.getState().focusMode).toBe(true)
    expect(document.querySelector('[data-workspace-center]')).toHaveFocus()
  })
})
