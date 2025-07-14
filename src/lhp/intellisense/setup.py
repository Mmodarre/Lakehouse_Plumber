"""
Core IntelliSense setup functionality for Lakehouse Plumber.

This module provides the main setup logic for configuring VS Code
IntelliSense support for Lakehouse Plumber YAML files.
"""

import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from lhp.intellisense.schema_manager import SchemaManager, SchemaManagerError
from lhp.intellisense.vscode_config import VSCodeConfigManager, VSCodeConfigError
from lhp.intellisense.extension_detector import VSCodeExtensionDetector, ExtensionDetectorError
from lhp.utils.error_formatter import LHPError, ErrorCategory


class IntelliSenseSetupError(LHPError):
    """Raised when IntelliSense setup operations fail."""
    
    def __init__(self, message: str, suggestions: list = None):
        super().__init__(
            category=ErrorCategory.CONFIG,
            code_number="004",
            title="IntelliSense Setup Error",
            details=message,
            suggestions=suggestions or ["Check prerequisites", "Verify VS Code installation", "Try running with --force"]
        )


class IntelliSenseSetup:
    """Main class for setting up IntelliSense support."""
    
    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = project_root or Path.cwd()
        self.schema_manager = SchemaManager()
        self.vscode_config = VSCodeConfigManager()
        self.extension_detector = VSCodeExtensionDetector()
        
        # Setup directories
        self.lhp_config_dir = Path.home() / ".lhp"
        self.schema_cache_dir = self.lhp_config_dir / "schemas"
        self.setup_log_file = self.lhp_config_dir / "setup.log"
        
        # Ensure directories exist
        self.lhp_config_dir.mkdir(exist_ok=True)
        self.schema_cache_dir.mkdir(exist_ok=True)
    
    def _log_setup_step(self, message: str) -> None:
        """Log a setup step to the setup log file."""
        try:
            with open(self.setup_log_file, "a", encoding="utf-8") as f:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] {message}\n")
        except Exception:
            pass  # Ignore logging errors
    
    def check_prerequisites(self) -> Dict[str, Any]:
        """
        Check if all prerequisites for IntelliSense setup are met.
        
        Returns:
            Dictionary containing prerequisite check results
        """
        results = {
            "vscode_installed": False,
            "vscode_accessible": False,
            "schemas_available": False,
            "settings_writable": False,
            "conflicts_detected": False,
            "conflict_details": [],
            "missing_requirements": [],
            "warnings": []
        }
        
        # Check if VS Code is installed
        try:
            results["vscode_installed"] = self.extension_detector.is_vscode_installed()
            if results["vscode_installed"]:
                results["vscode_accessible"] = True
        except ExtensionDetectorError:
            results["vscode_installed"] = False
            results["missing_requirements"].append("VS Code is not installed or not accessible")
        
        # Check if schemas are available
        try:
            schema_availability = self.schema_manager.validate_schema_availability()
            results["schemas_available"] = all(schema_availability.values())
            if not results["schemas_available"]:
                missing_schemas = [name for name, available in schema_availability.items() if not available]
                results["missing_requirements"].append(f"Missing schemas: {', '.join(missing_schemas)}")
        except SchemaManagerError as e:
            results["schemas_available"] = False
            results["missing_requirements"].append(f"Schema validation failed: {str(e)}")
        
        # Check if VS Code settings are writable
        try:
            settings_path = self.vscode_config.get_settings_path()
            parent_dir = settings_path.parent
            
            # Check if parent directory exists or can be created
            if not parent_dir.exists():
                try:
                    parent_dir.mkdir(parents=True, exist_ok=True)
                    results["settings_writable"] = True
                except Exception:
                    results["settings_writable"] = False
                    results["missing_requirements"].append("Cannot create VS Code settings directory")
            else:
                # Check if we can write to the settings file
                try:
                    if settings_path.exists():
                        # Check if file is writable
                        results["settings_writable"] = os.access(settings_path, os.W_OK)
                    else:
                        # Check if we can create the file
                        results["settings_writable"] = os.access(parent_dir, os.W_OK)
                    
                    if not results["settings_writable"]:
                        results["missing_requirements"].append("VS Code settings file is not writable")
                except Exception:
                    results["settings_writable"] = False
                    results["missing_requirements"].append("Cannot access VS Code settings file")
        except Exception:
            results["settings_writable"] = False
            results["missing_requirements"].append("Error checking VS Code settings access")
        
        # Check for conflicting extensions
        try:
            conflict_analysis = self.extension_detector.analyze_extension_conflicts()
            results["conflicts_detected"] = conflict_analysis["conflicting_extensions"] > 0
            results["conflict_details"] = conflict_analysis["conflicts"]
            
            if results["conflicts_detected"]:
                high_severity = conflict_analysis["severity_summary"]["high"]
                if high_severity > 0:
                    results["warnings"].append(f"Found {high_severity} high-severity extension conflicts")
        except ExtensionDetectorError:
            results["warnings"].append("Could not check for extension conflicts")
        
        return results
    
    def copy_schemas_to_cache(self) -> Dict[str, Path]:
        """
        Copy all schemas to the local cache directory.
        
        Returns:
            Dictionary mapping schema names to their cached file paths
            
        Raises:
            IntelliSenseSetupError: If schemas cannot be copied
        """
        try:
            self._log_setup_step("Copying schemas to cache directory")
            
            # Clear existing cache
            if self.schema_cache_dir.exists():
                shutil.rmtree(self.schema_cache_dir)
            self.schema_cache_dir.mkdir(parents=True)
            
            # Copy all schemas
            copied_schemas = self.schema_manager.copy_all_schemas_to_directory(self.schema_cache_dir)
            
            self._log_setup_step(f"Successfully copied {len(copied_schemas)} schemas to cache")
            return copied_schemas
            
        except SchemaManagerError as e:
            raise IntelliSenseSetupError(f"Failed to copy schemas to cache: {str(e)}")
    
    def setup_vscode_schema_associations(self) -> Dict[str, str]:
        """
        Set up VS Code schema associations.
        
        Returns:
            Dictionary of schema associations that were set up
            
        Raises:
            IntelliSenseSetupError: If schema associations cannot be set up
        """
        try:
            self._log_setup_step("Setting up VS Code schema associations")
            
            # Get schema mapping for VS Code
            schema_mapping = self.schema_manager.get_schema_mapping_for_vscode(self.schema_cache_dir)
            
            # Create backup of existing settings
            backup_path = self.vscode_config.backup_settings()
            if backup_path:
                self._log_setup_step(f"Created backup of VS Code settings: {backup_path}")
            
            # Get current settings
            current_settings = self.vscode_config.read_settings()
            
            # Update YAML schemas
            if "yaml.schemas" not in current_settings:
                current_settings["yaml.schemas"] = {}
            
            # Add LHP schema associations
            for schema_path, pattern in schema_mapping.items():
                current_settings["yaml.schemas"][schema_path] = pattern
            
            # Ensure YAML file associations and validation are enabled
            self.vscode_config.ensure_yaml_file_associations()
            self.vscode_config.ensure_yaml_validation_enabled()
            
            # Write updated settings
            self.vscode_config.write_settings(current_settings)
            
            self._log_setup_step("Successfully set up VS Code schema associations")
            return schema_mapping
            
        except (VSCodeConfigError, SchemaManagerError) as e:
            raise IntelliSenseSetupError(f"Failed to set up VS Code schema associations: {str(e)}")
    
    def run_full_setup(self, force: bool = False) -> Dict[str, Any]:
        """
        Run the complete IntelliSense setup process.
        
        Args:
            force: If True, skip prerequisite checks and force setup
            
        Returns:
            Dictionary containing setup results
            
        Raises:
            IntelliSenseSetupError: If setup fails
        """
        self._log_setup_step("Starting IntelliSense setup")
        
        setup_results = {
            "success": False,
            "prerequisites_met": False,
            "schemas_copied": 0,
            "associations_created": 0,
            "backup_created": False,
            "conflicts_detected": False,
            "warnings": [],
            "errors": []
        }
        
        try:
            # Check prerequisites
            if not force:
                self._log_setup_step("Checking prerequisites")
                prereq_results = self.check_prerequisites()
                setup_results["prerequisites_met"] = len(prereq_results["missing_requirements"]) == 0
                setup_results["conflicts_detected"] = prereq_results["conflicts_detected"]
                setup_results["warnings"].extend(prereq_results["warnings"])
                
                if not setup_results["prerequisites_met"]:
                    setup_results["errors"].extend(prereq_results["missing_requirements"])
                    raise IntelliSenseSetupError(f"Prerequisites not met: {', '.join(prereq_results['missing_requirements'])}")
            else:
                setup_results["prerequisites_met"] = True
                self._log_setup_step("Skipping prerequisite checks (forced setup)")
            
            # Copy schemas to cache
            copied_schemas = self.copy_schemas_to_cache()
            setup_results["schemas_copied"] = len(copied_schemas)
            
            # Set up VS Code schema associations
            schema_associations = self.setup_vscode_schema_associations()
            setup_results["associations_created"] = len(schema_associations)
            setup_results["backup_created"] = True
            
            # Check for conflicts after setup
            try:
                conflict_analysis = self.extension_detector.analyze_extension_conflicts()
                if conflict_analysis["conflicting_extensions"] > 0:
                    setup_results["conflicts_detected"] = True
                    setup_results["warnings"].append(
                        f"Detected {conflict_analysis['conflicting_extensions']} potentially conflicting extensions"
                    )
            except ExtensionDetectorError:
                setup_results["warnings"].append("Could not check for extension conflicts after setup")
            
            setup_results["success"] = True
            self._log_setup_step("IntelliSense setup completed successfully")
            
            return setup_results
            
        except Exception as e:
            setup_results["errors"].append(str(e))
            self._log_setup_step(f"IntelliSense setup failed: {str(e)}")
            raise IntelliSenseSetupError(f"Setup failed: {str(e)}")
    
    def verify_setup(self) -> Dict[str, Any]:
        """
        Verify that IntelliSense setup is working correctly.
        
        Returns:
            Dictionary containing verification results
        """
        verification_results = {
            "schemas_cached": False,
            "vscode_configured": False,
            "associations_active": False,
            "validation_enabled": False,
            "issues": []
        }
        
        try:
            # Check if schemas are cached
            if self.schema_cache_dir.exists():
                cached_files = list(self.schema_cache_dir.glob("*.json"))
                verification_results["schemas_cached"] = len(cached_files) > 0
            
            if not verification_results["schemas_cached"]:
                verification_results["issues"].append("Schemas not found in cache directory")
            
            # Check VS Code configuration
            try:
                settings = self.vscode_config.read_settings()
                verification_results["vscode_configured"] = "yaml.schemas" in settings
                
                if verification_results["vscode_configured"]:
                    # Check if LHP schemas are in the configuration
                    yaml_schemas = settings["yaml.schemas"]
                    lhp_schemas = [path for path in yaml_schemas.keys() if ".lhp" in path or "lakehouse-plumber" in path]
                    verification_results["associations_active"] = len(lhp_schemas) > 0
                    
                    if not verification_results["associations_active"]:
                        verification_results["issues"].append("LHP schema associations not found in VS Code settings")
                
                # Check if validation is enabled
                verification_results["validation_enabled"] = settings.get("yaml.validate", False)
                
                if not verification_results["validation_enabled"]:
                    verification_results["issues"].append("YAML validation not enabled in VS Code")
                
            except VSCodeConfigError as e:
                verification_results["issues"].append(f"Could not read VS Code settings: {str(e)}")
            
        except Exception as e:
            verification_results["issues"].append(f"Verification failed: {str(e)}")
        
        return verification_results
    
    def cleanup_setup(self) -> Dict[str, Any]:
        """
        Clean up IntelliSense setup (remove schema associations and cached files).
        
        Returns:
            Dictionary containing cleanup results
        """
        cleanup_results = {
            "success": False,
            "schemas_removed": 0,
            "associations_removed": 0,
            "cache_cleared": False,
            "errors": []
        }
        
        try:
            self._log_setup_step("Starting IntelliSense cleanup")
            
            # Remove schema associations from VS Code
            try:
                removed_count = self.vscode_config.clear_lhp_schemas()
                cleanup_results["associations_removed"] = removed_count
                self._log_setup_step(f"Removed {removed_count} schema associations from VS Code")
            except VSCodeConfigError as e:
                cleanup_results["errors"].append(f"Failed to remove schema associations: {str(e)}")
            
            # Clear schema cache
            try:
                if self.schema_cache_dir.exists():
                    cached_files = list(self.schema_cache_dir.glob("*.json"))
                    cleanup_results["schemas_removed"] = len(cached_files)
                    shutil.rmtree(self.schema_cache_dir)
                    self.schema_cache_dir.mkdir()
                    cleanup_results["cache_cleared"] = True
                    self._log_setup_step("Cleared schema cache")
            except Exception as e:
                cleanup_results["errors"].append(f"Failed to clear schema cache: {str(e)}")
            
            cleanup_results["success"] = len(cleanup_results["errors"]) == 0
            self._log_setup_step("IntelliSense cleanup completed")
            
            return cleanup_results
            
        except Exception as e:
            cleanup_results["errors"].append(str(e))
            self._log_setup_step(f"IntelliSense cleanup failed: {str(e)}")
            return cleanup_results
    
    def get_setup_status(self) -> Dict[str, Any]:
        """
        Get the current status of IntelliSense setup.
        
        Returns:
            Dictionary containing setup status information
        """
        status = {
            "setup_detected": False,
            "schemas_available": False,
            "vscode_configured": False,
            "last_setup_time": None,
            "schema_count": 0,
            "association_count": 0,
            "issues": []
        }
        
        try:
            # Check if setup has been run before
            if self.setup_log_file.exists():
                status["setup_detected"] = True
                # Get last setup time from log file
                try:
                    with open(self.setup_log_file, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        if lines:
                            last_line = lines[-1].strip()
                            if last_line:
                                # Extract timestamp from log line
                                import re
                                match = re.match(r'\[([^\]]+)\]', last_line)
                                if match:
                                    status["last_setup_time"] = match.group(1)
                except Exception:
                    pass
            
            # Check schema availability
            if self.schema_cache_dir.exists():
                cached_schemas = list(self.schema_cache_dir.glob("*.json"))
                status["schema_count"] = len(cached_schemas)
                status["schemas_available"] = status["schema_count"] > 0
            
            # Check VS Code configuration
            try:
                settings = self.vscode_config.read_settings()
                if "yaml.schemas" in settings:
                    yaml_schemas = settings["yaml.schemas"]
                    lhp_associations = [path for path in yaml_schemas.keys() if ".lhp" in path or "lakehouse-plumber" in path]
                    status["association_count"] = len(lhp_associations)
                    status["vscode_configured"] = status["association_count"] > 0
            except VSCodeConfigError:
                status["issues"].append("Could not read VS Code configuration")
            
            # Run verification if setup is detected
            if status["setup_detected"]:
                verification = self.verify_setup()
                status["issues"].extend(verification["issues"])
            
        except Exception as e:
            status["issues"].append(f"Error checking setup status: {str(e)}")
        
        return status
    
    def get_conflict_report(self) -> str:
        """
        Get a formatted report of extension conflicts.
        
        Returns:
            Formatted conflict report as a string
        """
        try:
            return self.extension_detector.generate_conflict_report()
        except ExtensionDetectorError as e:
            return f"Error generating conflict report: {str(e)}"
    
    def get_setup_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of the current setup state.
        
        Returns:
            Dictionary containing setup summary information
        """
        summary = {
            "status": self.get_setup_status(),
            "prerequisites": self.check_prerequisites(),
            "verification": self.verify_setup() if self.get_setup_status()["setup_detected"] else None,
            "conflicts": None
        }
        
        try:
            summary["conflicts"] = self.extension_detector.analyze_extension_conflicts()
        except ExtensionDetectorError:
            summary["conflicts"] = {"error": "Could not analyze extension conflicts"}
        
        return summary 