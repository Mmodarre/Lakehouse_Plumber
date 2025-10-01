"""SQL parser utility for extracting table references from SQL queries."""

import re
import logging
from typing import List, Set


class SQLParser:
    """Parser for extracting table references from SQL content."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract_tables_from_sql(self, sql_content: str) -> List[str]:
        """
        Extract table references from SQL content.

        Handles various patterns including:
        - FROM/JOIN clauses with catalog.schema.table
        - stream() function wrappers
        - CTEs (WITH clauses) - extracts tables from CTEs but excludes CTE names
        - Preserves substitution tokens like {catalog}.{schema}

        Args:
            sql_content: The SQL query string

        Returns:
            List of table references found in the SQL
        """
        if not sql_content or not isinstance(sql_content, str):
            return []

        tables = set()

        # Clean up SQL content
        cleaned_sql = self._clean_sql(sql_content)

        # First extract CTE information to know what names to exclude
        cte_tables, cte_names = self._extract_from_ctes(cleaned_sql)
        tables.update(cte_tables)

        # Extract tables from various SQL constructs, excluding CTE names
        tables.update(self._extract_from_from_clauses(cleaned_sql, cte_names))
        tables.update(self._extract_from_join_clauses(cleaned_sql, cte_names))
        tables.update(self._extract_from_function_wrappers(cleaned_sql))

        # Filter out invalid references
        valid_tables = [table for table in tables if self._is_valid_table_reference(table)]

        return sorted(valid_tables)

    def _clean_sql(self, sql_content: str) -> str:
        """Clean SQL content by removing comments and normalizing whitespace."""
        # Remove SQL comments
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

        # Normalize whitespace
        sql_content = re.sub(r'\s+', ' ', sql_content.strip())

        return sql_content

    def _extract_from_from_clauses(self, sql_content: str, cte_names: Set[str] = None) -> Set[str]:
        """Extract table references from FROM clauses, excluding CTE names and function wrappers."""
        tables = set()
        if cte_names is None:
            cte_names = set()

        # Pattern for FROM clauses
        # Matches: FROM table, FROM schema.table, FROM {catalog}.{schema}.table
        # Also handles aliases: FROM table AS alias, FROM table alias
        from_pattern = r'\bFROM\s+((?:(?:\{[^}]+\}|\w+)(?:\.(?:\{[^}]+\}|\w+))*))(?:\s+(?:AS\s+)?\w+)?'

        for match in re.finditer(from_pattern, sql_content, re.IGNORECASE):
            table_ref = match.group(1).strip()
            # Remove alias if present
            table_ref = re.sub(r'\s+(?:AS\s+)?\w+$', '', table_ref, flags=re.IGNORECASE)

            # Skip function wrappers - check if this match is followed by an opening parenthesis
            if match.end() < len(sql_content) and re.match(r'\s*\(', sql_content[match.end():]):
                continue

            # Skip if this is a CTE name
            if table_ref not in cte_names:
                tables.add(table_ref)

        # Handle comma-separated tables in FROM clause
        # e.g., FROM table1, table2, table3
        comma_pattern = r'\bFROM\s+(.*?)(?:\s+\bWHERE\b|\s+\bGROUP\b|\s+\bORDER\b|\s+\bHAVING\b|\s+\bLIMIT\b|;|\)|$)'
        comma_matches = re.findall(comma_pattern, sql_content, re.IGNORECASE | re.DOTALL)

        for match in comma_matches:
            if ',' in match:
                # Split by comma and extract table names
                parts = [part.strip() for part in match.split(',')]
                for part in parts:
                    # Skip function calls
                    if '(' in part:
                        continue

                    # Extract table reference, remove alias
                    table_match = re.match(r'((?:(?:\{[^}]+\}|\w+)(?:\.(?:\{[^}]+\}|\w+))*))(?:\s+(?:AS\s+)?\w+)?', part.strip())
                    if table_match:
                        table_ref = table_match.group(1)
                        # Skip if this is a CTE name
                        if table_ref not in cte_names:
                            tables.add(table_ref)

        return tables

    def _extract_from_join_clauses(self, sql_content: str, cte_names: Set[str] = None) -> Set[str]:
        """Extract table references from JOIN clauses, excluding CTE names."""
        tables = set()
        if cte_names is None:
            cte_names = set()

        # Pattern for all types of JOIN clauses
        join_pattern = r'\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+((?:(?:\{[^}]+\}|\w+)(?:\.(?:\{[^}]+\}|\w+))*))(?:\s+(?:AS\s+)?\w+)?'

        for match in re.finditer(join_pattern, sql_content, re.IGNORECASE):
            table_ref = match.group(1).strip()
            # Remove alias if present
            table_ref = re.sub(r'\s+(?:AS\s+)?\w+$', '', table_ref, flags=re.IGNORECASE)

            # Skip function wrappers - check if this match is followed by an opening parenthesis
            if match.end() < len(sql_content) and re.match(r'\s*\(', sql_content[match.end():]):
                continue

            # Skip if this is a CTE name
            if table_ref not in cte_names:
                tables.add(table_ref)

        return tables

    def _extract_from_ctes(self, sql_content: str) -> tuple[Set[str], Set[str]]:
        """Extract table references from CTE (WITH) clauses and return CTE names.

        Returns:
            Tuple of (table_references, cte_names)
        """
        tables = set()
        cte_names = set()

        # Extract all CTE names using multiple patterns
        # First, find the initial CTE after WITH
        first_cte_pattern = r'\bWITH\s+(\w+)\s+AS\s*\('
        first_cte_matches = re.findall(first_cte_pattern, sql_content, re.IGNORECASE)
        cte_names.update(first_cte_matches)

        # Then find subsequent CTEs after commas
        subsequent_cte_pattern = r',\s*(\w+)\s+AS\s*\('
        subsequent_cte_matches = re.findall(subsequent_cte_pattern, sql_content, re.IGNORECASE)
        cte_names.update(subsequent_cte_matches)

        # Find WITH clauses to extract table references from their definitions
        with_pattern = r'\bWITH\b.*?(?=\bSELECT\s+.*?\bFROM\s+(?!.*\bAS\s*\())'
        with_match = re.search(with_pattern, sql_content, re.IGNORECASE | re.DOTALL)

        if with_match:
            with_clause = with_match.group(0)
            # Extract FROM clauses within CTE definitions (don't pass cte_names to avoid circular filtering)
            tables.update(self._extract_from_from_clauses(with_clause))
            tables.update(self._extract_from_join_clauses(with_clause))

        return tables, cte_names

    def _extract_from_function_wrappers(self, sql_content: str) -> Set[str]:
        """Extract table references from function wrappers like stream()."""
        tables = set()

        # Pattern for function-wrapped table references
        # Matches: stream(table), live(table), snapshot(table)
        function_pattern = r'\b(?:stream|STREAM|live|LIVE|snapshot|SNAPSHOT)\s*\(\s*((?:\{[^}]+\}\.)?(?:\{[^}]+\}\.)?(?:\{[^}]+\}|\w+(?:\.\w+)*))\s*\)'

        for match in re.finditer(function_pattern, sql_content):
            table_ref = match.group(1).strip()
            tables.add(table_ref)

        return tables

    def _is_valid_table_reference(self, table_ref: str) -> bool:
        """Check if a table reference is valid and not a SQL keyword."""
        if not table_ref:
            return False

        # Filter out SQL keywords that might be mistakenly matched
        sql_keywords = {
            'AS', 'ON', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'FULL',
            'JOIN', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'USING',
            'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
            'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        }

        # Check if the reference is just a SQL keyword
        if table_ref.upper() in sql_keywords:
            return False

        # Simple validation - must contain alphanumeric characters or substitution tokens
        if not re.search(r'[\w{}]', table_ref):
            return False

        return True


# Convenience function for direct usage
def extract_tables_from_sql(sql_content: str) -> List[str]:
    """
    Convenience function to extract table references from SQL content.

    Args:
        sql_content: The SQL query string

    Returns:
        List of table references found in the SQL
    """
    parser = SQLParser()
    return parser.extract_tables_from_sql(sql_content)