"""Main sink write generator (dispatcher)."""

from typing import Dict, Any
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from .sinks import (
    DeltaSinkWriteGenerator,
    KafkaSinkWriteGenerator,
    CustomSinkWriteGenerator
)


class SinkWriteGenerator(BaseActionGenerator):
    """Dispatcher for sink write actions."""
    
    def __init__(self):
        super().__init__(use_import_manager=True)
        self.generators = {
            "delta": DeltaSinkWriteGenerator(),
            "kafka": KafkaSinkWriteGenerator(),
            "custom": CustomSinkWriteGenerator()
        }
    
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Dispatch to specific sink generator.
        
        Args:
            action: Action configuration
            context: Context dictionary with flowgroup and project info
            
        Returns:
            Generated Python code for the sink
            
        Raises:
            ValueError: If sink_type is unsupported
        """
        sink_config = action.write_target
        sink_type = sink_config.get("sink_type")
        
        if sink_type not in self.generators:
            raise ValueError(f"Unsupported sink_type: {sink_type}")
        
        # Delegate to specific generator
        generator = self.generators[sink_type]
        generated_code = generator.generate(action, context)
        
        # Merge imports from the specific generator to this dispatcher
        import_manager = self.get_import_manager()
        gen_import_manager = generator.get_import_manager()
        if import_manager and gen_import_manager:
            for import_stmt in gen_import_manager.get_consolidated_imports():
                import_manager.add_import(import_stmt)
        
        # For custom sinks, store the custom code for orchestrator
        if sink_type == "custom" and hasattr(generator, "custom_sink_code"):
            self.custom_sink_code = generator.custom_sink_code
            self.sink_file_path = generator.sink_file_path
        
        return generated_code

