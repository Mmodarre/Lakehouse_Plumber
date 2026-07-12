import { beforeEach, describe, expect, it } from 'vitest'
import { useUIStore } from '@/store/uiStore'

// The run-config selection (selectedPipelineConfig) is a separate persisted
// field from the Config tabs' picker state — picking a file in the editor
// must never implicitly change what runs use.

beforeEach(() => {
  useUIStore.setState({
    selectedConfigFiles: { pipeline: null, job: null },
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

  it('is independent of the pipeline tab picker selection', () => {
    useUIStore.getState().setSelectedConfigFile('pipeline', 'config/a.yaml')
    expect(useUIStore.getState().selectedPipelineConfig).toBeNull()
    useUIStore.getState().setSelectedPipelineConfig('config/b.yaml')
    expect(useUIStore.getState().selectedConfigFiles.pipeline).toBe('config/a.yaml')
  })

  it('persists exactly the picker slice plus the run-config selection', () => {
    useUIStore.getState().setSelectedPipelineConfig('config/pipeline_config_dev.yaml')
    const { partialize } = useUIStore.persist.getOptions()
    const slice = partialize!(useUIStore.getState())
    // Exact-equality pins the persisted surface: nothing else may leak into
    // localStorage without a deliberate test change.
    expect(slice).toEqual({
      selectedConfigFiles: { pipeline: null, job: null },
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
