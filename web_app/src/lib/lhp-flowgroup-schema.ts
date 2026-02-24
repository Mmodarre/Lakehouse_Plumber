/**
 * Tailored JSON Schema for LHP FlowGroup YAML files.
 *
 * Based on src/lhp/schemas/flowgroup.schema.json, enhanced with:
 * - test action type + all test-specific fields
 * - variables field on FlowGroup
 * - batch_handler + foreachbatch sink_type on WriteTarget
 * - Richer descriptions with examples
 *
 * Exported as a TS const (not .json) because tsconfig.app.json
 * lacks resolveJsonModule.
 */

export const lhpFlowgroupSchema = {
  $schema: 'http://json-schema.org/draft-07/schema#',
  title: 'LakehousePlumber FlowGroup',
  description:
    'Schema for LakehousePlumber pipeline configuration files. Supports single flowgroup, multi-document (---), and array syntax (flowgroups: []).',
  oneOf: [
    { $ref: '#/definitions/SingleFlowGroup' },
    { $ref: '#/definitions/MultiFlowGroupArray' },
  ],
  definitions: {
    // ── Top-level structures ────────────────────────────────

    SingleFlowGroup: {
      type: 'object',
      required: ['pipeline', 'flowgroup'],
      properties: {
        pipeline: {
          type: 'string',
          description: 'Name of the DLT pipeline this flowgroup belongs to. Must match a pipeline defined in lhp.yaml.',
        },
        flowgroup: {
          type: 'string',
          description: 'Unique name for this flowgroup within the pipeline.',
        },
        job_name: { $ref: '#/definitions/JobName' },
        presets: { $ref: '#/definitions/Presets' },
        use_template: { $ref: '#/definitions/UseTemplate' },
        template_parameters: { $ref: '#/definitions/TemplateParameters' },
        actions: { $ref: '#/definitions/Actions' },
        operational_metadata: { $ref: '#/definitions/OperationalMetadata' },
        variables: { $ref: '#/definitions/Variables' },
      },
      additionalProperties: false,
    },

    MultiFlowGroupArray: {
      type: 'object',
      required: ['pipeline', 'flowgroups'],
      properties: {
        pipeline: {
          type: 'string',
          description: 'Pipeline name inherited by all flowgroups in this document.',
        },
        use_template: { $ref: '#/definitions/UseTemplate' },
        presets: { $ref: '#/definitions/Presets' },
        operational_metadata: { $ref: '#/definitions/OperationalMetadata' },
        variables: { $ref: '#/definitions/Variables' },
        flowgroups: {
          type: 'array',
          description: 'Array of flowgroup definitions. Each inherits document-level fields unless explicitly overridden.',
          minItems: 1,
          items: {
            type: 'object',
            required: ['flowgroup'],
            properties: {
              flowgroup: {
                type: 'string',
                description: 'Name of the flowgroup.',
              },
              pipeline: {
                type: 'string',
                description: 'Override pipeline name for this flowgroup (optional, inherits from document level).',
              },
              job_name: { $ref: '#/definitions/JobName' },
              use_template: { $ref: '#/definitions/UseTemplate' },
              template_parameters: { $ref: '#/definitions/TemplateParameters' },
              presets: { $ref: '#/definitions/Presets' },
              actions: { $ref: '#/definitions/Actions' },
              operational_metadata: { $ref: '#/definitions/OperationalMetadata' },
              variables: { $ref: '#/definitions/Variables' },
            },
            additionalProperties: false,
          },
        },
      },
      additionalProperties: false,
    },

    // ── Shared definitions ──────────────────────────────────

    JobName: {
      oneOf: [
        { type: 'string', description: 'Single job name for grouping flowgroups.' },
        {
          type: 'array',
          items: { type: 'string' },
          minItems: 1,
          description: 'Multiple job names (for job_config.yaml).',
        },
      ],
      description: 'Job name(s) for grouping flowgroups into separate orchestration jobs.',
    },

    Presets: {
      type: 'array',
      description:
        'List of preset names to apply. Presets are matched by action type and deep-merged — explicit action config wins over preset values.',
      items: { type: 'string' },
      default: [],
    },

    UseTemplate: {
      type: 'string',
      description:
        'Name of the template to use. Templates define reusable action patterns with {{ parameter }} placeholders.',
    },

    TemplateParameters: {
      type: 'object',
      description: 'Parameters to pass to the template, filling {{ parameter }} placeholders.',
      additionalProperties: true,
    },

    Variables: {
      type: 'object',
      description:
        'Local variables for substitution within this flowgroup. Referenced as %{variable_name}. Processed before template parameters and environment tokens.',
      additionalProperties: {
        type: 'string',
      },
    },

    Actions: {
      type: 'array',
      description: 'List of actions (load, transform, write, test) to perform in this flowgroup.',
      items: { $ref: '#/definitions/Action' },
      default: [],
    },

    OperationalMetadata: {
      oneOf: [
        { type: 'boolean', description: 'true to include all default operational metadata columns.' },
        {
          type: 'array',
          items: { type: 'string' },
          description: 'Specific metadata columns to include (e.g. ["source_file", "ingest_timestamp"]).',
        },
      ],
      description:
        'Operational metadata configuration. Additive across preset, flowgroup, and action levels.',
    },

    // ── Action ──────────────────────────────────────────────

    Action: {
      type: 'object',
      required: ['name', 'type'],
      properties: {
        name: {
          type: 'string',
          description: 'Unique name for this action. Used as the view/table name unless target is specified.',
        },
        type: {
          type: 'string',
          enum: ['load', 'transform', 'write', 'test'],
          description:
            'Action type: "load" ingests from external sources, "transform" processes views, "write" persists to tables/sinks, "test" defines data quality checks.',
        },
        source: {
          oneOf: [
            { type: 'string', description: 'Single input view name (transform) or source path.' },
            {
              type: 'array',
              items: {
                oneOf: [{ type: 'string' }, { type: 'object' }],
              },
              description: 'Multiple input view names for multi-source transforms.',
            },
            { $ref: '#/definitions/SourceObject' },
          ],
          description:
            'Source configuration. For load actions: object with type (cloudfiles, delta, sql, jdbc, python, custom_datasource, kafka). For transforms: string or array of input view names. Python transforms use action-level module_path/function_name.',
        },
        target: {
          type: 'string',
          description: 'Target view or table name. Defaults to the action name if not specified.',
        },
        description: {
          type: 'string',
          description: 'Human-readable description of this action (added as a comment in generated code).',
        },
        readMode: {
          type: 'string',
          enum: ['batch', 'stream'],
          description:
            'Read mode: "stream" uses spark.readStream (continuous incremental processing), "batch" uses spark.read (full re-read each run). Default depends on source type.',
        },
        write_target: { $ref: '#/definitions/WriteTarget' },
        transform_type: {
          type: 'string',
          enum: ['sql', 'python', 'data_quality', 'temp_table', 'schema'],
          description:
            'Type of transformation. "sql": inline/file SQL, "python": external module, "data_quality": expectation checks, "temp_table": session-scoped temp view, "schema": column enforcement/casting.',
        },
        sql: {
          type: 'string',
          description: 'Inline SQL query. For streaming sources, wrap view names in stream(): e.g. SELECT * FROM stream(my_view).',
        },
        sql_path: {
          type: 'string',
          description: 'Path to external SQL file, relative to project root.',
        },
        operational_metadata: { $ref: '#/definitions/OperationalMetadata' },
        expectations_file: {
          type: 'string',
          description: 'Path to expectations YAML file for data_quality transforms.',
        },
        schema_inline: {
          type: 'string',
          description: 'Inline schema definition for schema transforms (arrow notation, e.g. "col1: string, col2: int").',
        },
        schema_file: {
          type: 'string',
          description: 'Path to external schema file for schema transforms.',
        },
        enforcement: {
          type: 'string',
          enum: ['strict', 'permissive'],
          description:
            'Schema enforcement mode: "strict" keeps only defined columns, "permissive" allows extra columns. Default: permissive.',
        },
        once: {
          type: 'boolean',
          description: 'Set to true for one-time backfill flows that should not re-execute on subsequent pipeline runs.',
        },
        module_path: {
          type: 'string',
          description:
            'Path to Python module file relative to project root (e.g. "transformations/customer_transforms.py"). Required for Python transforms and loads.',
        },
        function_name: {
          type: 'string',
          description: 'Name of the Python function to call. Required for Python transforms and loads.',
        },
        parameters: {
          type: 'object',
          description: 'Parameters dictionary passed to the Python function.',
          additionalProperties: true,
        },

        // ── Test action fields ────────────────────────────
        test_type: {
          type: 'string',
          enum: [
            'not_null',
            'unique',
            'accepted_values',
            'referential_integrity',
            'row_count',
            'column_values',
            'custom_sql',
            'range_check',
            'lookup',
            'schema_check',
          ],
          description:
            'Type of data quality test. "not_null": columns must not contain nulls, "unique": column values must be unique, "accepted_values": column values in allowed list, "referential_integrity": foreign key check, "row_count": min/max row bounds, "column_values": expression-based checks, "custom_sql": arbitrary SQL assertion, "range_check": numeric range validation, "lookup": value exists in reference table, "schema_check": required columns exist.',
        },
        on_violation: {
          type: 'string',
          enum: ['fail', 'warn', 'drop'],
          description:
            'Action on test failure: "fail" aborts pipeline, "warn" logs and continues, "drop" removes violating rows. Default: fail.',
        },
        tolerance: {
          type: 'number',
          description: 'Allowed fraction of violations (0.0-1.0). E.g. 0.01 allows 1% null rate for not_null tests.',
        },
        columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to test (for not_null, unique, accepted_values tests).',
        },
        filter: {
          type: 'string',
          description: 'SQL WHERE clause to filter rows before testing (e.g. "status != \'deleted\'").',
        },
        reference: {
          type: 'string',
          description: 'Reference table/view for referential_integrity and lookup tests.',
        },
        source_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Source columns for referential_integrity join.',
        },
        reference_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Reference table columns for referential_integrity join.',
        },
        required_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns that must exist for schema_check tests.',
        },
        column: {
          type: 'string',
          description: 'Single column for range_check or column_values tests.',
        },
        min_value: {
          type: 'number',
          description: 'Minimum allowed value for range_check tests.',
        },
        max_value: {
          type: 'number',
          description: 'Maximum allowed value for range_check tests.',
        },
        lookup_table: {
          type: 'string',
          description: 'Table to look up values in for lookup tests.',
        },
        lookup_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Source columns to match against the lookup table.',
        },
        lookup_result_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to return from the lookup table.',
        },
        expectations: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string', description: 'Expectation name.' },
              expression: { type: 'string', description: 'SQL boolean expression.' },
              action: {
                type: 'string',
                enum: ['fail', 'warn', 'drop'],
                description: 'Action on violation.',
              },
            },
          },
          description: 'List of custom expectation rules for data_quality transforms.',
        },
      },
    },

    // ── Source object (for load actions) ─────────────────────

    SourceObject: {
      type: 'object',
      properties: {
        type: {
          type: 'string',
          enum: ['cloudfiles', 'delta', 'sql', 'jdbc', 'python', 'custom_datasource', 'kafka'],
          description:
            'Source type: "cloudfiles" for Auto Loader (cloud storage), "delta" for Delta tables, "sql" for SQL queries, "jdbc" for external databases, "python" for custom Python readers, "custom_datasource" for Spark DataSource V2, "kafka" for Kafka/Event Hubs.',
        },
        path: {
          type: 'string',
          description: 'Path to data files (cloudfiles) or Delta table location.',
        },
        format: {
          type: 'string',
          enum: ['json', 'parquet', 'csv', 'avro', 'orc', 'text', 'binaryfile', 'xml'],
          description: 'File format for cloudfiles sources.',
        },
        database: { type: 'string', description: 'Database/schema name for delta sources.' },
        table: { type: 'string', description: 'Table name for delta sources.' },
        catalog: { type: 'string', description: 'Unity Catalog name (three-level namespace: catalog.database.table).' },
        sql: { type: 'string', description: 'SQL query for sql-type sources.' },
        sql_path: { type: 'string', description: 'Path to SQL file for sql-type sources.' },
        url: { type: 'string', description: 'JDBC connection URL.' },
        user: { type: 'string', description: 'Database user for JDBC connections.' },
        password: { type: 'string', description: 'Database password for JDBC (use ${secret:scope/key} for secrets).' },
        driver: { type: 'string', description: 'JDBC driver class name.' },
        query: { type: 'string', description: 'SQL query for JDBC sources.' },
        module_path: {
          type: 'string',
          description: 'Python module path for python/custom_datasource load sources.',
        },
        function_name: {
          type: 'string',
          description: 'Python function name for python load sources.',
        },
        custom_datasource_class: {
          type: 'string',
          description: 'DataSource V2 class name for custom_datasource sources.',
        },
        parameters: {
          type: 'object',
          description: 'Parameters passed to the Python load function.',
          additionalProperties: true,
        },
        options: {
          type: 'object',
          description: 'Reader options (spark.readStream.options() / spark.read.options()).',
          additionalProperties: true,
        },
        format_options: {
          type: 'object',
          description: 'Format-specific options passed to the reader.',
          additionalProperties: true,
        },
        schema: {
          type: 'string',
          description: 'Schema definition as DDL string or file path.',
        },
        schema_file: {
          type: 'string',
          description: 'Path to external schema file.',
        },
        readMode: {
          type: 'string',
          enum: ['batch', 'stream'],
          description:
            'Read mode: "stream" for continuous processing (spark.readStream), "batch" for full re-read (spark.read).',
        },
        schema_location: { type: 'string', description: 'Cloud storage path for Auto Loader schema inference state.' },
        schema_infer_column_types: {
          type: 'boolean',
          description: 'Whether Auto Loader should infer column types (vs. all-string).',
        },
        max_files_per_trigger: {
          type: 'integer',
          description: 'Maximum files per micro-batch trigger (Auto Loader).',
        },
        schema_evolution_mode: {
          type: 'string',
          enum: ['addNewColumns', 'rescue', 'failOnNewColumns'],
          description: 'Schema evolution strategy for Auto Loader.',
        },
        rescue_data_column: {
          type: 'string',
          description: 'Column name for rescued data that does not match the schema.',
        },
        where_clause: { type: 'string', description: 'WHERE clause for filtering source data.' },
        select_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Specific columns to select from the source.',
        },
      },
    },

    // ── Write Target ────────────────────────────────────────

    WriteTarget: {
      type: 'object',
      required: ['type'],
      properties: {
        type: {
          type: 'string',
          enum: ['streaming_table', 'materialized_view', 'sink'],
          description:
            'Write target type: "streaming_table" for append-only/CDC tables, "materialized_view" for query-based refreshable views, "sink" for external outputs (Delta, Kafka, custom).',
        },
        database: { type: 'string', description: 'Target database/schema name.' },
        table: { type: 'string', description: 'Target table name (overrides action name).' },
        create_table: {
          type: 'boolean',
          default: true,
          description: 'Whether to create the table if it does not exist.',
        },
        comment: { type: 'string', description: 'Table comment (visible in Unity Catalog).' },
        table_properties: {
          type: 'object',
          additionalProperties: true,
          description: 'Delta table properties (e.g. delta.autoOptimize.optimizeWrite: true).',
        },
        partition_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to partition the table by.',
        },
        cluster_columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns for liquid clustering.',
        },
        spark_conf: {
          type: 'object',
          additionalProperties: true,
          description: 'Spark configuration for this write target.',
        },
        schema: { type: 'string', description: 'Table schema (legacy, prefer table_schema).' },
        table_schema: {
          type: 'string',
          description: 'Table schema definition (DDL string or file path).',
        },
        row_filter: { type: 'string', description: 'Row filter SQL expression.' },
        temporary: {
          type: 'boolean',
          default: false,
          description: 'Whether this is a temporary (session-scoped) table.',
        },
        path: { type: 'string', description: 'External storage location for the table.' },
        refresh_schedule: {
          type: 'string',
          description: 'Cron schedule for materialized view refresh.',
        },
        sql: { type: 'string', description: 'SQL query for materialized views.' },

        // Sink fields
        sink_type: {
          type: 'string',
          enum: ['delta', 'kafka', 'custom', 'foreachbatch'],
          description:
            'Sink type: "delta" writes to an external Delta table, "kafka" publishes to Kafka/Event Hubs, "custom" uses a user-defined sink class, "foreachbatch" invokes a batch_handler function per micro-batch.',
        },
        sink_name: { type: 'string', description: 'Name for the sink output.' },
        bootstrap_servers: {
          type: 'string',
          description: 'Kafka bootstrap servers (for kafka sinks).',
        },
        topic: { type: 'string', description: 'Kafka topic (for kafka sinks).' },
        module_path: {
          type: 'string',
          description: 'Path to custom sink module (for custom/foreachbatch sinks).',
        },
        custom_sink_class: {
          type: 'string',
          description: 'Custom sink class name (for custom sinks).',
        },
        batch_handler: {
          type: 'string',
          description:
            'Function name for foreachBatch processing. The function receives (batch_df, batch_id) arguments. Used with sink_type: foreachbatch.',
        },
        options: {
          type: 'object',
          additionalProperties: true,
          description:
            'Sink-specific options. Delta sinks: must contain "tableName" OR "path" (not both). Kafka sinks: kafka.* prefixed options.',
        },
        mode: {
          type: 'string',
          enum: ['cdc', 'snapshot_cdc'],
          description: 'Table mode for change data capture operations.',
        },
        cdc_config: { $ref: '#/definitions/CdcConfig' },
        snapshot_cdc_config: { $ref: '#/definitions/SnapshotCdcConfig' },
      },
    },

    // ── CDC configs ─────────────────────────────────────────

    CdcConfig: {
      type: 'object',
      description: 'CDC (Change Data Capture) configuration for streaming tables.',
      properties: {
        keys: {
          type: 'array',
          items: { type: 'string' },
          description: 'Primary key columns for CDC merge.',
        },
        sequence_by: {
          type: 'string',
          description: 'Column to sequence/order changes by (e.g. timestamp).',
        },
        ignore_null_updates: {
          type: 'boolean',
          description: 'Skip updates where all non-key columns are null.',
        },
        apply_as_deletes: {
          type: 'string',
          description: 'SQL expression to identify delete records (e.g. "operation = \'DELETE\'").',
        },
        apply_as_truncates: {
          type: 'string',
          description: 'SQL expression to identify truncate records.',
        },
        except_column_list: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to exclude from change detection.',
        },
        stored_as_scd_type: {
          type: 'string',
          enum: ['1', '2'],
          description: 'SCD type: "1" overwrites, "2" preserves history.',
        },
        track_history_column_list: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to track history for (SCD Type 2).',
        },
        track_history_except_column_list: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to exclude from history tracking (SCD Type 2).',
        },
      },
    },

    SnapshotCdcConfig: {
      type: 'object',
      description: 'Snapshot CDC configuration for periodic snapshot-based change detection.',
      properties: {
        source: { type: 'string', description: 'Source table for snapshot CDC.' },
        source_function: {
          type: 'object',
          description: 'External function that returns the snapshot DataFrame.',
          properties: {
            file: { type: 'string', description: 'File containing the source function.' },
            function: { type: 'string', description: 'Function name.' },
          },
        },
        keys: {
          type: 'array',
          items: { type: 'string' },
          description: 'Primary key columns.',
        },
        stored_as_scd_type: {
          type: 'string',
          enum: ['1', '2'],
          description: 'SCD type: "1" overwrites, "2" preserves history.',
        },
        track_history_column_list: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to track history for.',
        },
        track_history_except_column_list: {
          type: 'array',
          items: { type: 'string' },
          description: 'Columns to exclude from history tracking.',
        },
      },
    },
  },
}
// Not `as const` — monaco-yaml's JSONSchema type requires mutable arrays (e.g. string[] for `required`)
