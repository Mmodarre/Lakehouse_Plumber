import { beforeEach, describe, expect, it } from 'vitest'
import { useUIStore } from '@/store/uiStore'

// The run-config selection (selectedPipelineConfig) is a persisted field
// driven only by the explicit "Use for runs" toggle.

beforeEach(() => {
  useUIStore.setState({
    selectedPipelineConfig: null,
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

  it('persists exactly the run-config selection plus the sandbox toggle', () => {
    useUIStore.getState().setSelectedPipelineConfig('config/pipeline_config_dev.yaml')
    const { partialize } = useUIStore.persist.getOptions()
    const slice = partialize!(useUIStore.getState())
    // Exact-equality pins the persisted surface: nothing else may leak into
    // localStorage without a deliberate test change.
    expect(slice).toEqual({
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
