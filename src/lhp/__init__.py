import os
import klyne

# Only initialize Klyne if not in test environment or explicitly disabled
if not os.environ.get('LHP_DISABLE_ANALYTICS') and not os.environ.get('PYTEST_CURRENT_TEST'):
    klyne.init(
        api_key='klyne_6k-6xLUerb5DvATHISDO4aRoVZybVdabuQuI-m5tAxg',
        project='lakehouse-plumber',
        package_version='0.6.4'
    )