"""Test action generator for Lakehouse Plumber."""

from typing import Dict, Any, List, Optional
from lhp.core.base_generator import BaseActionGenerator
from lhp.models.config import Action, TestActionType, ViolationAction


class TestActionGenerator(BaseActionGenerator):
    """Generator for test actions using existing transform infrastructure."""
    __test__ = False  # Tell pytest this is not a test class
    
    # SQL templates for each test type
    TEST_SQL_TEMPLATES = {
        'row_count': """
            SELECT * FROM 
              (SELECT COUNT(*) AS source_count FROM {source[0]}),
              (SELECT COUNT(*) AS target_count FROM {source[1]})
        """,
        'uniqueness': """
            SELECT {columns}, COUNT(*) as duplicate_count
            FROM {source}
            GROUP BY {columns}
            HAVING COUNT(*) > 1
        """,
        'referential_integrity': """
            SELECT 
              s.*,
              r.{ref_col} as ref_{ref_col}
            FROM {source} s
            LEFT JOIN {reference} r ON {join_condition}
        """,
        'completeness': """
            SELECT {columns}
            FROM {source}
        """,
        'range': """
            SELECT {column}
            FROM {source}
        """,
        'all_lookups_found': """
            SELECT 
              s.*, 
              l.{lookup_result} as lookup_{lookup_result}
            FROM {source} s
            LEFT JOIN {lookup_table} l ON {join_condition}
        """,
        'schema_match': """
            WITH source_schema AS (
              SELECT column_name, data_type, ordinal_position
              FROM information_schema.columns
              WHERE table_name = '{source}'
            ),
            reference_schema AS (
              SELECT column_name, data_type, ordinal_position
              FROM information_schema.columns
              WHERE table_name = '{reference}'
            ),
            schema_diff AS (
              SELECT 
                COALESCE(s.column_name, r.column_name) as column_name,
                s.data_type as source_type,
                r.data_type as reference_type,
                CASE 
                  WHEN s.column_name IS NULL THEN 'missing_in_source'
                  WHEN r.column_name IS NULL THEN 'extra_in_source'
                  WHEN s.data_type != r.data_type THEN 'type_mismatch'
                  ELSE 'match'
                END as status
              FROM source_schema s
              FULL OUTER JOIN reference_schema r ON s.column_name = r.column_name
            )
            SELECT * FROM schema_diff WHERE status != 'match'
        """,
    }
    
    def __init__(self, config: Dict[str, Any] = None, context: Dict[str, Any] = None):
        """Initialize TestGenerator with config and context."""
        super().__init__()
        self.config = config or {}
        self.context = context or {}
        
        # Add basic imports
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql.functions import *")
    
    def generate(self, action: Action = None, context: Dict[str, Any] = None) -> str:
        """Generate test code directly without delegation."""
        # Use instance config/context if not provided
        if action:
            # Convert Action to config dict
            self.config = action.model_dump(mode='json', exclude_none=True)
        
        if context is None:
            context = self.context
        
        # Get test type
        test_type = self.config.get('test_type', 'row_count')
        
        # Get target name
        target = self.config.get('target', f"tmp_test_{self.config.get('name')}")
        
        # Build SQL if needed
        sql = None
        if test_type in self.TEST_SQL_TEMPLATES:
            sql = self._generate_test_sql(test_type)
        elif test_type == 'custom_sql':
            sql = self.config.get('sql')
        elif test_type == 'custom_expectations':
            # For custom expectations, just pass through the source
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            sql = f"SELECT * FROM {source}"
        
        # Build expectations
        expectations = self._build_expectations(test_type)
        
        # Generate the complete code
        code_parts = []
        
        # Add imports only if not being used through orchestrator
        # (orchestrator adds imports at file level)
        if not context or 'flowgroup' not in context:
            # Standalone mode - add imports
            code_parts.append("from pyspark import pipelines as dp")
            code_parts.append("from pyspark.sql.functions import *")
            code_parts.append("")
        
        # Group expectations by violation action
        fail_expectations = {}
        warn_expectations = {}
        drop_expectations = {}
        
        for exp in expectations:
            name = exp['name']
            expression = exp['expression']
            violation = exp.get('on_violation', 'fail')
            
            if violation == 'fail':
                fail_expectations[name] = expression
            elif violation == 'warn':
                warn_expectations[name] = expression
            elif violation == 'drop':
                drop_expectations[name] = expression
        
        # Add decorators
        if fail_expectations:
            exp_str = ", ".join([f'"{k}": "{v}"' for k, v in fail_expectations.items()])
            code_parts.append(f"@dp.expect_all_or_fail({{{exp_str}}})")
        if drop_expectations:
            exp_str = ", ".join([f'"{k}": "{v}"' for k, v in drop_expectations.items()])
            code_parts.append(f"@dp.expect_all_or_drop({{{exp_str}}})")
        if warn_expectations:
            exp_str = ", ".join([f'"{k}": "{v}"' for k, v in warn_expectations.items()])
            code_parts.append(f"@dp.expect_all({{{exp_str}}})")
        
        # Add temporary table decorator
        description = self.config.get('description', f'Test: {test_type}')
        code_parts.append(f'@dp.table(name="{target}", comment="{description}", temporary=True)')
        
        # Add function definition
        code_parts.append(f"def {target}():")
        code_parts.append(f'    """{description}"""')
        
        # Add SQL execution if we have SQL
        if sql:
            # Clean up SQL formatting
            sql_lines = sql.strip().split('\n')
            sql_formatted = '\n'.join([f"        {line}" if i > 0 else line.strip() 
                                       for i, line in enumerate(sql_lines)])
            code_parts.append(f'    return spark.sql("""')
            code_parts.append(f'        {sql_formatted}')
            code_parts.append(f'    """)')
        else:
            # For expectations-only tests, just return the source
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            code_parts.append(f'    return spark.table("{source}")')
        
        return '\n'.join(code_parts)
    
    def _build_transform_config(self, test_type: str) -> Dict[str, Any]:
        """Convert test config to transform config."""
        config = {
            'name': self.config.get('name'),
            'type': 'transform',
            'transform_type': 'sql' if test_type != 'custom_expectations' else 'data_quality',
            'source': self.config.get('source'),
            'target': self.config.get('target', f"tmp_test_{self.config.get('name')}"),
        }
        
        # Add SQL based on test type
        if test_type in self.TEST_SQL_TEMPLATES:
            config['sql'] = self._generate_test_sql(test_type)
        elif test_type == 'custom_sql':
            config['sql'] = self.config.get('sql')
        
        # Add expectations
        expectations = self._build_expectations(test_type)
        if expectations:
            config['expectations'] = expectations
        
        return config
    
    def _generate_test_sql(self, test_type: str) -> str:
        """Generate SQL for specific test type."""
        template = self.TEST_SQL_TEMPLATES.get(test_type)
        if not template:
            return ""
        
        # Format the template based on test type
        if test_type == 'row_count':
            source = self.config.get('source', [])
            if len(source) >= 2:
                return template.format(source=source)
        
        elif test_type == 'uniqueness':
            columns = self.config.get('columns', [])
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            columns_str = ', '.join(columns) if columns else 'id'
            
            # Handle optional filter for uniqueness tests (e.g., Type 2 dimensions)
            filter_clause = self.config.get('filter', '') if self.config else ''
            filter_clause = filter_clause.strip() if filter_clause else ''
            if filter_clause:
                # Create SQL with WHERE clause
                sql = f"""
            SELECT {columns_str}, COUNT(*) as duplicate_count
            FROM {source}
            WHERE {filter_clause}
            GROUP BY {columns_str}
            HAVING COUNT(*) > 1
        """
                return sql.strip()
            else:
                # No filter - use original template
                return template.format(columns=columns_str, source=source)
        
        elif test_type == 'referential_integrity':
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            reference = self.config.get('reference', 'reference_table')
            source_cols = self.config.get('source_columns', ['id'])
            ref_cols = self.config.get('reference_columns', ['id'])
            
            join_conditions = []
            for s_col, r_col in zip(source_cols, ref_cols):
                join_conditions.append(f"s.{s_col} = r.{r_col}")
            
            return template.format(
                source=source,
                reference=reference,
                ref_col=ref_cols[0] if ref_cols else 'id',
                join_condition=' AND '.join(join_conditions)
            )
        
        elif test_type == 'completeness':
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            required_columns = self.config.get('required_columns', [])
            if required_columns:
                columns = ', '.join(required_columns)
            else:
                columns = '*'  # Fallback to * if no columns specified
            return template.format(source=source, columns=columns)
            
        elif test_type == 'range':
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            column = self.config.get('column', '*')  # Fallback to * if no column specified
            return template.format(source=source, column=column)
        
        elif test_type == 'all_lookups_found':
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            lookup_table = self.config.get('lookup_table', 'lookup_table')
            lookup_cols = self.config.get('lookup_columns', ['id'])
            result_cols = self.config.get('lookup_result_columns', ['result'])
            
            join_conditions = []
            for col in lookup_cols:
                join_conditions.append(f"s.{col} = l.{col}")
            
            return template.format(
                source=source,
                lookup_table=lookup_table,
                lookup_result=result_cols[0] if result_cols else 'result',
                join_condition=' AND '.join(join_conditions)
            )
        
        elif test_type == 'schema_match':
            source = self.config.get('source')
            if isinstance(source, list):
                source = source[0] if source else 'source_table'
            reference = self.config.get('reference', 'reference_table')
            return template.format(source=source, reference=reference)
        
        return template
    
    def _build_expectations(self, test_type: str) -> List[Dict[str, Any]]:
        """Build expectations based on test type."""
        on_violation = self.config.get('on_violation', 'fail')
        # Validate on_violation value, default to 'fail' if invalid
        if on_violation not in ['fail', 'warn', 'drop']:
            on_violation = 'fail'
        
        if test_type == 'row_count':
            tolerance = self.config.get('tolerance', 0)
            return [{
                'name': 'row_count_match',
                'expression': f"abs(source_count - target_count) <= {tolerance}",
                'on_violation': on_violation
            }]
        
        elif test_type == 'uniqueness':
            return [{
                'name': 'no_duplicates',
                'expression': 'duplicate_count == 0',
                'on_violation': on_violation
            }]
        
        elif test_type == 'referential_integrity':
            ref_cols = self.config.get('reference_columns', ['id'])
            ref_col = ref_cols[0] if ref_cols else 'id'
            return [{
                'name': 'referential_integrity',
                'expression': f"ref_{ref_col} IS NOT NULL",
                'on_violation': on_violation
            }]
        
        elif test_type == 'completeness':
            required_cols = self.config.get('required_columns', [])
            if required_cols:
                expressions = [f"{col} IS NOT NULL" for col in required_cols]
                return [{
                    'name': 'required_fields_complete',
                    'expression': ' AND '.join(expressions),
                    'on_violation': on_violation
                }]
        
        elif test_type == 'range':
            column = self.config.get('column', 'value')
            min_val = self.config.get('min_value')
            max_val = self.config.get('max_value')
            
            expressions = []
            if min_val is not None:
                expressions.append(f"{column} >= '{min_val}'")
            if max_val is not None:
                expressions.append(f"{column} <= '{max_val}'")
            
            if expressions:
                return [{
                    'name': 'value_in_range',
                    'expression': ' AND '.join(expressions),
                    'on_violation': on_violation
                }]
        
        elif test_type == 'all_lookups_found':
            result_cols = self.config.get('lookup_result_columns', ['result'])
            result_col = result_cols[0] if result_cols else 'result'
            return [{
                'name': 'all_lookups_found',
                'expression': f"lookup_{result_col} IS NOT NULL",
                'on_violation': on_violation
            }]
        
        elif test_type == 'schema_match':
            return [{
                'name': 'schemas_match',
                'expression': 'false',  # Fails if any row exists (schema differences)
                'on_violation': on_violation
            }]
        
        elif test_type == 'custom_expectations':
            # Pass through custom expectations
            return self.config.get('expectations', [])
        elif test_type == 'custom_sql':
            # Custom SQL can also have custom expectations
            return self.config.get('expectations', [])
        
        return []
