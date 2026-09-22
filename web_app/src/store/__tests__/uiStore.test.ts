import { beforeEach, describe, expect, it } from 'vitest'
import { useUIStore } from '@/store/uiStore'

// The run-config selection (selectedPipelineConfig) is a persisted field
// driven only by the explicit "Use for runs" toggle.

beforeEach(() => {
  useUIStore.setState({
    selectedPipelineConfig: null,
    selectedEnv: '',
    environmentProject: null,
    environmentByProject: {},
    sandboxEnabled: false,
  })
})

describe('uiStore — selectedPipelineConfig', () => {
  it('defaults to null and round-trips through the setter', () => {
    expect(useUIStore.getState().selectedPipelineConfig).toBeNull()
    useUIStore.getState().setSelectedPipelineConfig('config/pipeline_config_dev.yaml')
    expect(useUIStore.getState().selectedPipelineConfig).toBe(
      'config/pipeline_config_dev.yaml',
    )
    useUIStore.getState().setSelectedPipelineConfig(null)
    expect(useUIStore.getState().selectedPipelineConfig).toBeNull()
  })

  it('persists run-config, sandbox and per-project environment choices', () => {
    useUIStore.getState().setSelectedPipelineConfig('config/pipeline_config_dev.yaml')
    const { partialize } = useUIStore.persist.getOptions()
    const slice = partialize!(useUIStore.getState())
    // Exact-equality pins the persisted surface: nothing else may leak into
    // localStorage without a deliberate test change.
    expect(slice).toEqual({
      environmentByProject: {},
      selectedPipelineConfig: 'config/pipeline_config_dev.yaml',
      sandboxEnabled: false,
    })
  })
})

describe('uiStore — sandboxEnabled', () => {
  it('defaults to false and round-trips through setter and toggle', () => {
    expect(useUIStore.getState().sandboxEnabled).toBe(false)
    useUIStore.getState().setSandboxEnabled(true)
    expect(useUIStore.getState().sandboxEnabled).toBe(true)
    useUIStore.getState().toggleSandbox()
    expect(useUIStore.getState().sandboxEnabled).toBe(false)
  })

  it('is part of the persisted slice', () => {
    useUIStore.getState().setSandboxEnabled(true)
    const { partialize } = useUIStore.persist.getOptions()
    expect(partialize!(useUIStore.getState())).toMatchObject({ sandboxEnabled: true })
  })
})

describe('project environments', () => {
  it('chooses an available environment when dev is absent and handles removal/empty lists', () => {
    const reconcile = useUIStore.getState().reconcileEnvironments
    reconcile('/project-a', ['test', 'prod'])
    expect(useUIStore.getState().selectedEnv).toBe('test')
    useUIStore.getState().setSelectedEnv('prod')
    reconcile('/project-a', ['test'])
    expect(useUIStore.getState().selectedEnv).toBe('test')
    reconcile('/project-a', [])
    expect(useUIStore.getState().selectedEnv).toBe('')
  })
  it('restores independent valid choices per project across reload reconciliation', () => {
    const reconcile = useUIStore.getState().reconcileEnvironments
    reconcile('/project-a', ['test', 'prod'])
    useUIStore.getState().setSelectedEnv('prod')
    reconcile('/project-b', ['dev', 'test'])
    expect(useUIStore.getState().selectedEnv).toBe('dev')
    useUIStore.setState({ selectedEnv: '', environmentProject: null })
    reconcile('/project-a', ['test', 'prod'])
    expect(useUIStore.getState().selectedEnv).toBe('prod')
  })
})
