"""
⚠️  TEMPORARY MODULE - DATABRICKS ASSET BUNDLE LIMITATION WORKAROUND ⚠️

This module is a TEMPORARY workaround for a Databricks Asset Bundle limitation
that requires Python files to have '# Databricks notebook source' as the first line.

🗑️  REMOVAL INSTRUCTIONS:
- This entire module should be DELETED once Databricks fixes the limitation
- Search codebase for: "temporary_databricks_headers" to find all references
- See DATABRICKS_HEADERS_REMOVAL.md for complete removal instructions

⏰ TEMPORARY NATURE:
- Added: 2024 (Bundle sync enhancement)
- Remove when: Databricks Asset Bundles no longer require notebook headers
- Issue: Databricks Asset Bundles don't recognize Python files without notebook headers

🔍 SEARCH TERMS FOR REMOVAL:
- "temporary_databricks_headers"
- "Databricks notebook source"
- "TEMPORARY.*databricks"
- "add_databricks_notebook_headers"
"""

import logging
from pathlib import Path
from typing import List

# TEMPORARY: Logger for header processing operations
logger = logging.getLogger(__name__)

# TEMPORARY: Databricks notebook header constant
DATABRICKS_HEADER = "# Databricks notebook source"


def add_databricks_notebook_headers(generated_dir: Path, substitution: str) -> int:
    """
    TEMPORARY: Add Databricks notebook headers to all Python files.
    
    This function is a TEMPORARY workaround for Databricks Asset Bundle limitation.
    It adds '# Databricks notebook source' as the first line to all Python files
    that don't already have it.
    
    Args:
        generated_dir: Path to the generated files directory
        substitution: Environment/substitution name (for logging)
        
    Returns:
        Number of files modified
        
    Note:
        This function should be REMOVED once Databricks fixes the limitation.
    """
    if not generated_dir.exists():
        logger.debug(f"TEMPORARY: Generated directory {generated_dir} does not exist, skipping header processing")
        return 0
    
    logger.info(f"🔧 TEMPORARY: Adding Databricks notebook headers for {substitution} environment...")
    
    try:
        # TEMPORARY: Find all Python files
        python_files = _find_python_files(generated_dir)
        
        if not python_files:
            logger.debug(f"TEMPORARY: No Python files found in {generated_dir}")
            return 0
            
        logger.debug(f"TEMPORARY: Found {len(python_files)} Python files to process")
        
        # TEMPORARY: Process each file
        modified_count = 0
        for file_path in python_files:
            try:
                if not _has_databricks_header(file_path):
                    if _add_databricks_header(file_path):
                        modified_count += 1
                        logger.debug(f"TEMPORARY: Added header to {file_path.relative_to(generated_dir)}")
                    else:
                        logger.warning(f"TEMPORARY: Failed to add header to {file_path}")
                else:
                    logger.debug(f"TEMPORARY: Header already exists in {file_path.relative_to(generated_dir)}")
                    
            except Exception as e:
                logger.error(f"TEMPORARY: Error processing {file_path}: {e}")
                continue
        
        if modified_count > 0:
            logger.info(f"✅ TEMPORARY: Added Databricks headers to {modified_count} Python file(s)")
        else:
            logger.info("✅ TEMPORARY: All Python files already have Databricks headers")
            
        return modified_count
        
    except Exception as e:
        logger.error(f"TEMPORARY: Error during header processing: {e}")
        return 0


def _find_python_files(directory: Path) -> List[Path]:
    """
    TEMPORARY: Find all Python files in directory tree.
    
    Args:
        directory: Root directory to search
        
    Returns:
        List of Python file paths
        
    Note:
        This function should be REMOVED with the module.
    """
    python_files = []
    
    try:
        # TEMPORARY: Recursively find all .py files
        for file_path in directory.rglob("*.py"):
            if file_path.is_file():
                python_files.append(file_path)
                
    except Exception as e:
        logger.error(f"TEMPORARY: Error finding Python files in {directory}: {e}")
        
    return python_files


def _has_databricks_header(file_path: Path) -> bool:
    """
    TEMPORARY: Check if file already has Databricks notebook header.
    
    Args:
        file_path: Path to Python file
        
    Returns:
        True if file has header, False otherwise
        
    Note:
        This function should be REMOVED with the module.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            return first_line == DATABRICKS_HEADER
            
    except Exception as e:
        logger.debug(f"TEMPORARY: Error reading {file_path}: {e}")
        return False


def _add_databricks_header(file_path: Path) -> bool:
    """
    TEMPORARY: Add Databricks notebook header as first line.
    
    Args:
        file_path: Path to Python file
        
    Returns:
        True if header added successfully, False otherwise
        
    Note:
        This function should be REMOVED with the module.
    """
    try:
        # TEMPORARY: Read existing content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # TEMPORARY: Prepend header
        new_content = f"{DATABRICKS_HEADER}\n{content}"
        
        # TEMPORARY: Write back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
            
        return True
        
    except Exception as e:
        logger.error(f"TEMPORARY: Error adding header to {file_path}: {e}")
        return False 