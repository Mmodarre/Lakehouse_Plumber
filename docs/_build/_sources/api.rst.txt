API Reference
=============

The sections below document the public classes and functions that you might import
in advanced use-cases (e.g., custom generators, validators, or notebook
integration).

.. note::
   The public API is still **alpha** and may change without notice until version
   1.0.

Key Modules
-----------

The main entry points for programmatic usage:

* ``lhp.core.orchestrator.ActionOrchestrator`` – Main pipeline generation engine
* ``lhp.parsers.yaml_parser.YAMLParser`` – Parse YAML configurations  
* ``lhp.core.state_manager.StateManager`` – Track generated files
* ``lhp.utils.substitution.EnhancedSubstitutionManager`` – Token/secret handling

For complete API documentation, see the inline docstrings or use Python's ``help()`` function. 