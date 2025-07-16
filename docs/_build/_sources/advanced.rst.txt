Advanced Topics
===============

This section covers advanced LakehousePlumber features and internal workings 
that are useful for power users, troubleshooting, and understanding how LHP 
optimizes pipeline generation.

State Management & Smart Generation
-----------------------------------

Lakehouse Plumber keeps a small **state file** under ``.lhp_state.json`` that
maps generated Python files to their source YAML.  It records checksums and
dependency links so that future `lhp generate` runs can:

* re-process only *new* or *stale* FlowGroups.
* skip files whose inputs did not change.
* optionally clean up orphaned files when you delete YAML.

This behaviour is similar to Gradle's incremental build or Terraform's state
management.

**How state management works:**

.. code-block:: json
   :caption: .lhp_state.json example
   :linenos:

   {
     "version": "1.0",
     "generated_files": {
       "customer_ingestion.py": {
         "source_yaml": "pipelines/bronze/customer_ingestion.yaml",
         "checksum": "a1b2c3d4e5f6",
         "environment": "dev",
         "dependencies": ["presets/bronze_layer.yaml"]
       }
     }
   }

**Benefits:**

- **Faster regeneration** - Only changed files are processed
- **Dependency tracking** - Upstream changes trigger downstream regeneration  
- **Cleanup support** - Detect and remove orphaned generated files
- **CI/CD optimization** - Skip unchanged pipeline generation in builds

Dependency Resolver
-------------------

Transforms may reference earlier views (or tables) via the ``source`` field.
Plumber's resolver builds a DAG, checks for cycles, and ensures downstream
FlowGroups regenerate when upstream definitions change.

**Dependency resolution process:**

1. **Parse source references** - Extract view/table dependencies from actions
2. **Build dependency graph** - Create directed acyclic graph (DAG) of dependencies
3. **Cycle detection** - Prevent circular dependencies that would cause runtime errors
4. **Topological ordering** - Generate actions in correct execution order
5. **Change propagation** - Mark downstream FlowGroups for regeneration when dependencies change

**Example dependency chain:**

.. code-block:: yaml
   :caption: Dependency example
   :linenos:

   # raw_data.yaml - No dependencies (source)
   actions:
     - name: load_files
       type: load
       source: { type: cloudfiles, path: "/data/*.json" }
       target: v_raw_data

   # clean_data.yaml - Depends on v_raw_data  
   actions:
     - name: clean_data
       type: transform
       source: v_raw_data  # ← Dependency
       target: v_clean_data

   # aggregated.yaml - Depends on v_clean_data
   actions:
     - name: aggregate
       type: transform  
       source: v_clean_data  # ← Dependency
       target: v_aggregated

Pipeline Generation Workflow
----------------------------

The complete pipeline generation process follows this workflow:

.. mermaid::

   graph TD
       subgraph "Discovery Phase"
           A[Scan YAML Files] --> B[Apply Include Patterns]
           B --> C[Parse FlowGroups]
       end
       
       subgraph "Resolution Phase"  
           C --> D[Apply Presets]
           D --> E[Expand Templates]
           E --> F[Apply Substitutions]
           F --> G[Validate Configuration]
       end
       
       subgraph "Generation Phase"
           G --> H[Resolve Dependencies]
           H --> I[Check State]
           I --> J{Changed?}
           J -->|Yes| K[Generate Code]
           J -->|No| L[Skip Generation]
           K --> M[Update State]
           L --> M
       end
       
       subgraph "Output"
           M --> N[Python DLT Files]
       end

**Key optimization points:**

- **Smart discovery** - Include patterns reduce files to process
- **Incremental generation** - State tracking skips unchanged files  
- **Dependency awareness** - Changes propagate to affected downstream files
- **Validation early** - Catch errors before code generation
- **Parallel processing** - Independent FlowGroups can be processed simultaneously

Troubleshooting
---------------

**Common state management issues:**

.. code-block:: bash
   :caption: Debugging state issues

   # Force regeneration of all files
   lhp generate --force-all --env dev

   # Clear state and regenerate everything
   rm .lhp_state.json
   lhp generate --env dev

   # Check what files would be regenerated
   lhp generate --dry-run --env dev --verbose

**Debugging dependency issues:**

.. code-block:: bash
   :caption: Dependency debugging

   # Show dependency graph
   lhp validate --env dev --show-dependencies

   # Validate for circular dependencies  
   lhp validate --env dev --check-cycles

**Performance optimization:**

- Use **include patterns** to limit file scanning scope
- Keep **FlowGroups focused** - avoid overly large YAML files
- Leverage **state management** - don't force regeneration unless needed
- Use **specific targets** when possible instead of full pipeline generation 