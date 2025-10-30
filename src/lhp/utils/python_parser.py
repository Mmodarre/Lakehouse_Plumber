"""Python parser utility for extracting table references from Python code."""

import ast
import logging
from typing import List, Set
from .sql_parser import extract_tables_from_sql


class PythonParser:
    """Parser for extracting table references from Python code that uses Spark SQL."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract_tables_from_python(self, python_code: str) -> List[str]:
        """
        Extract table references from Python code.

        Analyzes Python code for Spark SQL method calls and extracts
        table references from SQL strings and direct table references.

        Args:
            python_code: The Python source code string

        Returns:
            List of table references found in the Python code
        """
        if not python_code or not isinstance(python_code, str):
            return []

        tables = set()

        # Extract SQL queries from spark.sql() and related methods
        sql_queries = self.extract_sql_from_python(python_code)
        for sql_query in sql_queries:
            tables.update(extract_tables_from_sql(sql_query))

        # Extract direct table references from Spark methods
        direct_tables = self._extract_direct_table_references(python_code)
        tables.update(direct_tables)

        return sorted(list(tables))

    def extract_sql_from_python(self, python_code: str) -> List[str]:
        """
        Extract SQL query strings from Python code.

        Looks for calls to methods that contain SQL queries:
        - spark.sql()
        - spark.createGlobalTempView()
        - spark.createOrReplaceTempView()
        - df.createOrReplaceTempView()

        Args:
            python_code: The Python source code string

        Returns:
            List of SQL query strings found in the code
        """
        sql_queries = []

        try:
            # Normalize indentation to handle indented code blocks
            normalized_code = self._normalize_python_code(python_code)
            tree = ast.parse(normalized_code)

            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    sql_query = self._extract_sql_from_call(node)
                    if sql_query:
                        sql_queries.append(sql_query)

        except SyntaxError as e:
            self.logger.warning(f"Could not parse Python code: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting SQL from Python: {e}")

        return sql_queries

    def _extract_direct_table_references(self, python_code: str) -> Set[str]:
        """
        Extract direct table references from Spark methods.

        Looks for:
        - spark.table("table_name")
        - spark.read.table("table_name")
        - spark.catalog.* methods

        Args:
            python_code: The Python source code string

        Returns:
            Set of table references
        """
        tables = set()

        try:
            # Normalize indentation to handle indented code blocks
            normalized_code = self._normalize_python_code(python_code)
            tree = ast.parse(normalized_code)

            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    table_ref = self._extract_table_from_call(node)
                    if table_ref:
                        tables.add(table_ref)

        except Exception as e:
            self.logger.error(f"Error extracting direct table references: {e}")

        return tables

    def _extract_sql_from_call(self, node: ast.Call) -> str:
        """Extract SQL string from a function call node."""
        # Check for spark.sql() calls
        if (isinstance(node.func, ast.Attribute) and
            node.func.attr == 'sql' and
            self._is_spark_object(node.func.value)):

            return self._get_string_argument(node, 0)

        # Check for createOrReplaceTempView() calls (they might contain SQL in subqueries)
        elif (isinstance(node.func, ast.Attribute) and
              node.func.attr in ['createOrReplaceTempView', 'createGlobalTempView']):

            # These methods don't directly contain SQL, but we might want to track them
            # for completeness in future enhancements
            pass

        return None

    def _extract_table_from_call(self, node: ast.Call) -> str:
        """Extract table reference from a function call node."""
        # Check for spark.table() calls
        if (isinstance(node.func, ast.Attribute) and
            node.func.attr == 'table' and
            self._is_spark_object(node.func.value)):

            return self._get_string_argument(node, 0)

        # Check for spark.read.table() calls
        elif (isinstance(node.func, ast.Attribute) and
              node.func.attr == 'table' and
              isinstance(node.func.value, ast.Attribute) and
              node.func.value.attr == 'read' and
              self._is_spark_object(node.func.value.value)):

            return self._get_string_argument(node, 0)

        # Check for catalog methods that reference tables
        elif (isinstance(node.func, ast.Attribute) and
              isinstance(node.func.value, ast.Attribute) and
              node.func.value.attr == 'catalog' and
              self._is_spark_object(node.func.value.value) and
              node.func.attr in ['tableExists', 'dropTempView', 'listTables']):

            # These methods take table names as first argument
            if node.func.attr in ['tableExists', 'dropTempView']:
                return self._get_string_argument(node, 0)

        return None

    def _is_spark_object(self, node: ast.AST) -> bool:
        """Check if an AST node represents a spark object."""
        # Direct spark reference: spark
        if isinstance(node, ast.Name) and node.id == 'spark':
            return True

        # Attribute access: self.spark, obj.spark
        if (isinstance(node, ast.Attribute) and
            node.attr == 'spark'):
            return True

        return False

    def _get_string_argument(self, node: ast.Call, arg_index: int) -> str:
        """Extract string argument from a function call at the specified index."""
        if len(node.args) <= arg_index:
            return None

        arg = node.args[arg_index]

        # Handle string literals (ast.Constant available since Python 3.8)
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value

        # Handle f-strings (JoinedStr)
        elif isinstance(arg, ast.JoinedStr):
            return self._process_f_string(arg)

        # Handle simple variable names (we can't resolve these)
        elif isinstance(arg, ast.Name):
            self.logger.debug(f"Found variable reference '{arg.id}' in SQL call - cannot resolve")
            return None

        return None

    def _process_f_string(self, node: ast.JoinedStr) -> str:
        """
        Process an f-string node and return the template string.

        Replaces variable interpolations with placeholders to preserve
        the overall SQL structure while keeping substitution tokens intact.
        """
        parts = []

        for value in node.values:
            # Handle string constants (ast.Constant available since Python 3.8)
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                # For formatted values, try to preserve substitution tokens
                if (isinstance(value.value, ast.Name) and
                    value.value.id in ['catalog', 'schema', 'table', 'bronze_schema',
                                      'silver_schema', 'gold_schema', 'migration_schema',
                                      'old_schema']):
                    parts.append(f'{{{value.value.id}}}')
                else:
                    # For other variables, use a generic placeholder
                    parts.append('{var}')

        return ''.join(parts)

    def _normalize_python_code(self, python_code: str) -> str:
        """
        Normalize Python code by removing common indentation.

        This allows parsing of code blocks that are indented (like in test strings).

        Args:
            python_code: The Python source code string

        Returns:
            Normalized Python code with leading indentation removed
        """
        if not python_code or not isinstance(python_code, str):
            return ""

        import textwrap
        return textwrap.dedent(python_code).strip()


# Convenience functions for direct usage
def extract_tables_from_python(python_code: str) -> List[str]:
    """
    Convenience function to extract table references from Python code.

    Args:
        python_code: The Python source code string

    Returns:
        List of table references found in the Python code
    """
    parser = PythonParser()
    return parser.extract_tables_from_python(python_code)


def extract_sql_from_python(python_code: str) -> List[str]:
    """
    Convenience function to extract SQL queries from Python code.

    Args:
        python_code: The Python source code string

    Returns:
        List of SQL query strings found in the code
    """
    parser = PythonParser()
    return parser.extract_sql_from_python(python_code)