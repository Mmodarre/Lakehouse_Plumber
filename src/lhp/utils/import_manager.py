"""
Unified Import Management for LakehousePlumber
==============================================

Zero-configuration import management that consolidates:
- Expression-based import detection (from operational_metadata.py)  
- File-level import extraction (adapted from streaming_table.py)
- Smart conflict resolution with established conventions
- Backward-compatible API for existing generators

Key Features:
- Reuses proven AST parsing logic
- Hardcoded rules based on Python/PySpark conventions
- Wildcard import precedence (import * wins over specific imports)
- Graceful fallback for parsing failures
- No configuration required
"""

import ast
import logging
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Union

# Import existing proven components
from .operational_metadata import ImportDetector


class ImportManager:
    """
    Unified import management with zero configuration.
    
    Consolidates all import-related functionality:
    - Manual import collection (backward compatible)
    - Expression-based detection (operational metadata)
    - File-level extraction (custom source files)
    - Smart deduplication and conflict resolution
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Import collections
        self._manual_imports: Set[str] = set()
        self._expression_imports: Set[str] = set()
        self._file_imports: Set[str] = set()
        
        # Reuse existing proven components
        self._expression_detector = ImportDetector(strategy="ast")
        
        # Hardcoded rules - no configuration needed
        self._import_order = ["standard", "third_party", "pyspark", "dlt", "custom"]
        self._wildcard_precedence = True
        
        # Standard library modules (common ones)
        self._standard_modules = {
            "ast", "sys", "os", "re", "json", "time", "datetime", "pathlib",
            "typing", "dataclasses", "functools", "itertools", "collections",
            "logging", "argparse", "subprocess", "urllib", "http"
        }
    
    def add_import(self, import_stmt: str) -> None:
        """
        Add manual import statement (backward compatible API).
        
        Args:
            import_stmt: Import statement like "from pyspark import pipelines as dp" or "from pyspark.sql import functions"
        """
        if import_stmt and import_stmt.strip():
            self._manual_imports.add(import_stmt.strip())
    
    def add_imports_from_expression(self, expression: str) -> None:
        """
        Add imports detected from PySpark expressions.
        
        Uses existing ImportDetector logic with established patterns.
        
        Args:
            expression: PySpark expression like "F.current_timestamp()"
        """
        try:
            detected = self._expression_detector.detect_imports(expression)
            self._expression_imports.update(detected)
        except Exception as e:
            self.logger.debug(f"Expression import detection failed for '{expression}': {e}")
    
    def add_imports_from_file(self, source_code: str) -> str:
        """
        Extract imports from Python source file and return cleaned source.
        
        Uses adapted streaming_table.py AST logic with PySpark support.
        
        Args:
            source_code: Full Python source code
            
        Returns:
            Source code with import statements removed
        """
        try:
            return self._extract_with_ast(source_code)
        except Exception as e:
            self.logger.warning(f"File import extraction failed: {e}")
            # Graceful fallback - return original source unchanged
            return source_code
    
    def _extract_with_ast(self, source_code: str) -> str:
        """
        Extract imports using AST parsing (adapted from streaming_table.py).
        
        Args:
            source_code: Python source code
            
        Returns:
            Source code with imports removed
        """
        try:
            tree = ast.parse(source_code)
            source_lines = source_code.split("\n")
            imports = []
            lines_to_remove = set()
            
            # Extract top-level imports (reusing streaming_table.py logic)
            for node in tree.body:
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    import_line = source_lines[node.lineno - 1].strip()
                    imports.append(import_line)
                    lines_to_remove.add(node.lineno - 1)  # Convert to 0-based
            
            # Add extracted imports (no PySpark filtering - we want them!)
            self._file_imports.update(imports)
            
            # Remove import lines from source
            cleaned_lines = []
            for i, line in enumerate(source_lines):
                if i not in lines_to_remove:
                    cleaned_lines.append(line)
                # Keep empty lines to preserve line numbers for debugging
                else:
                    cleaned_lines.append("")
            
            return "\n".join(cleaned_lines)
            
        except SyntaxError as e:
            self.logger.warning(f"AST parsing failed (invalid Python): {e}")
            return source_code
        except Exception as e:
            self.logger.warning(f"Unexpected error in import extraction: {e}")
            return source_code
    
    def get_consolidated_imports(self) -> List[str]:
        """
        Get final consolidated and deduplicated imports.
        
        Applies smart conflict resolution with hardcoded rules:
        - Wildcard imports take precedence
        - Logical grouping by type
        - Established Python/PySpark conventions
        
        Returns:
            Sorted list of deduplicated import statements
        """
        # Combine all imports
        all_imports = (
            self._manual_imports | 
            self._expression_imports | 
            self._file_imports
        )
        
        if not all_imports:
            return []
        
        # Apply conflict resolution
        resolved_imports = self._resolve_conflicts(all_imports)
        
        # Sort by established conventions
        return self._sort_imports(resolved_imports)
    
    def _resolve_conflicts(self, imports: Set[str]) -> Set[str]:
        """
        Resolve import conflicts using hardcoded rules with enhanced submodule detection.
        
        Key rules:
        1. import * takes precedence over specific imports from same module
        2. Submodule wildcard imports take precedence over parent module aliases
           (e.g., "from pyspark.sql.functions import *" beats "from pyspark.sql import functions as F")
        """
        if not imports:
            return set()
        
        # Group imports by module
        module_groups = {}
        wildcard_modules = set()
        parent_child_conflicts = {}  # Track parent-child module relationships
        
        for imp in imports:
            module = self._extract_module_name(imp)
            if module:
                if module not in module_groups:
                    module_groups[module] = []
                module_groups[module].append(imp)
                
                # Track wildcard imports
                if self._is_wildcard_import(imp):
                    wildcard_modules.add(module)
        
        # Enhanced conflict detection: Find parent-child module relationships
        self._detect_submodule_conflicts(module_groups, wildcard_modules, parent_child_conflicts)
        
        # Apply enhanced wildcard precedence rules
        resolved = set()
        excluded_modules = set()  # Modules excluded due to submodule conflicts
        
        # First pass: Handle submodule conflicts
        for child_module, parent_info in parent_child_conflicts.items():
            if child_module in wildcard_modules:
                # Child module has wildcard import - exclude parent module imports
                excluded_modules.add(parent_info['parent_module'])
                self.logger.debug(f"Submodule conflict: {child_module} wildcard excludes {parent_info['parent_module']} imports")
        
        # Second pass: Apply standard conflict resolution
        for module, module_imports in module_groups.items():
            if module in excluded_modules:
                continue  # Skip modules excluded by submodule conflicts
            
            if module in wildcard_modules:
                # Keep only wildcard import, remove specific ones
                wildcards = [imp for imp in module_imports if self._is_wildcard_import(imp)]
                resolved.update(wildcards)
            else:
                # No wildcard, keep all specific imports
                resolved.update(module_imports)
        
        return resolved
    
    def _detect_submodule_conflicts(self, module_groups: Dict[str, List[str]], 
                                   wildcard_modules: Set[str], 
                                   parent_child_conflicts: Dict[str, Dict[str, str]]) -> None:
        """
        Detect parent-child module relationships and potential conflicts.
        
        Examples:
        - "pyspark.sql.functions" is child of "pyspark.sql"
        - "from pyspark.sql.functions import *" should beat "from pyspark.sql import functions as F"
        """
        # Check each wildcard module against all other modules
        for wildcard_module in wildcard_modules:
            wildcard_parts = wildcard_module.split('.')
            
            for other_module in module_groups:
                if other_module == wildcard_module:
                    continue
                
                other_parts = other_module.split('.')
                
                # Check if wildcard_module is a child of other_module
                if (len(wildcard_parts) > len(other_parts) and 
                    wildcard_parts[:len(other_parts)] == other_parts):
                    
                    # Check if parent module imports the child as alias
                    parent_imports = module_groups[other_module]
                    child_module_name = wildcard_parts[len(other_parts)]
                    
                    for parent_import in parent_imports:
                        if self._is_parent_importing_child_as_alias(parent_import, child_module_name):
                            parent_child_conflicts[wildcard_module] = {
                                'parent_module': other_module,
                                'parent_import': parent_import,
                                'child_alias': child_module_name
                            }
                            self.logger.debug(f"Detected submodule conflict: {wildcard_module} vs {parent_import}")
                            break
    
    def _is_parent_importing_child_as_alias(self, import_stmt: str, child_name: str) -> bool:
        """
        Check if import statement is importing a child module as alias.
        
        Examples:
        - "from pyspark.sql import functions as F" imports "functions" as "F"
        - "from pyspark.sql import functions" imports "functions" directly
        """
        # Pattern: "from module import child_name as alias" or "from module import child_name"
        pattern = rf'from\s+[^\s]+\s+import\s+.*\b{re.escape(child_name)}\b(?:\s+as\s+\w+)?'
        return bool(re.search(pattern, import_stmt))
    
    def _extract_module_name(self, import_stmt: str) -> Optional[str]:
        """Extract base module name from import statement."""
        import_stmt = import_stmt.strip()
        
        # Handle "from module import ..." 
        if import_stmt.startswith("from "):
            match = re.match(r"from\s+([^\s]+)\s+import", import_stmt)
            if match:
                return match.group(1)
        
        # Handle "import module"
        elif import_stmt.startswith("import "):
            match = re.match(r"import\s+([^\s,]+)", import_stmt)
            if match:
                return match.group(1)
        
        return None
    
    def _is_wildcard_import(self, import_stmt: str) -> bool:
        """Check if import statement is a wildcard import."""
        return "import *" in import_stmt
    
    def _categorize_import(self, import_stmt: str) -> str:
        """Categorize import for sorting (hardcoded conventions)."""
        import_stmt_lower = import_stmt.lower()
        
        # Standard library
        module = self._extract_module_name(import_stmt)
        if module and module.split('.')[0] in self._standard_modules:
            return "standard"
        
        # DLT (pyspark.pipelines) - check before general pyspark check
        if "pipelines" in import_stmt_lower:
            return "dlt"
        
        # PySpark
        if "pyspark" in import_stmt_lower or "spark" in import_stmt_lower:
            return "pyspark"
        
        # Common third-party
        third_party_indicators = ["pandas", "numpy", "requests", "yaml", "json"]
        for indicator in third_party_indicators:
            if indicator in import_stmt_lower:
                return "third_party"
        
        # Default to custom
        return "custom"
    
    def _sort_imports(self, imports: Set[str]) -> List[str]:
        """Sort imports by established conventions."""
        if not imports:
            return []
        
        # Group by category
        categorized = {}
        for category in self._import_order:
            categorized[category] = []
        
        for imp in imports:
            category = self._categorize_import(imp)
            categorized[category].append(imp)
        
        # Sort within each category and combine
        sorted_imports = []
        for category in self._import_order:
            if categorized[category]:
                categorized[category].sort()
                sorted_imports.extend(categorized[category])
        
        return sorted_imports
    
    def clear(self) -> None:
        """Clear all collected imports."""
        self._manual_imports.clear()
        self._expression_imports.clear()
        self._file_imports.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about collected imports."""
        return {
            "manual_imports": len(self._manual_imports),
            "expression_imports": len(self._expression_imports), 
            "file_imports": len(self._file_imports),
            "total_unique": len(self.get_consolidated_imports())
        }
    
    def debug_info(self) -> Dict[str, any]:
        """Get detailed debug information."""
        return {
            "manual_imports": sorted(self._manual_imports),
            "expression_imports": sorted(self._expression_imports),
            "file_imports": sorted(self._file_imports),
            "consolidated": self.get_consolidated_imports(),
            "stats": self.get_stats()
        } 