"""Factory interfaces for dependency injection in LakehousePlumber orchestrator."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Protocol

from ..utils.substitution import EnhancedSubstitutionManager
from ..utils.smart_file_writer import SmartFileWriter


class SubstitutionFactory(Protocol):
    """Factory interface for creating substitution managers."""
    
    def create(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        """
        Create a substitution manager for the given environment.
        
        Args:
            substitution_file: Path to substitution YAML file
            env: Environment name
            
        Returns:
            EnhancedSubstitutionManager instance
        """
        ...


class FileWriterFactory(Protocol):
    """Factory interface for creating file writers."""
    
    def create(self) -> SmartFileWriter:
        """
        Create a smart file writer instance.
        
        Returns:
            SmartFileWriter instance
        """
        ...


class DefaultSubstitutionFactory:
    """Default implementation of SubstitutionFactory."""
    
    def create(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        """Create default substitution manager."""
        return EnhancedSubstitutionManager(substitution_file, env)


class DefaultFileWriterFactory:
    """Default implementation of FileWriterFactory."""
    
    def create(self) -> SmartFileWriter:
        """Create default smart file writer."""
        return SmartFileWriter()


class OrchestrationDependencies:
    """Dependency container for orchestrator injection."""
    
    def __init__(self, 
                 substitution_factory: SubstitutionFactory = None,
                 file_writer_factory: FileWriterFactory = None):
        """
        Initialize orchestration dependencies.
        
        Args:
            substitution_factory: Factory for substitution managers
            file_writer_factory: Factory for file writers
        """
        self.substitution_factory = substitution_factory or DefaultSubstitutionFactory()
        self.file_writer_factory = file_writer_factory or DefaultFileWriterFactory()
    
    def create_substitution_manager(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        """Create substitution manager using factory."""
        return self.substitution_factory.create(substitution_file, env)
    
    def create_file_writer(self) -> SmartFileWriter:
        """Create file writer using factory."""
        return self.file_writer_factory.create()
