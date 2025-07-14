"""
VS Code extension detection for IntelliSense setup.

This module provides utilities to detect installed VS Code extensions
that might conflict with YAML schema associations.
"""

import json
import platform
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from lhp.utils.error_formatter import LHPError, ErrorCategory


class ExtensionDetectorError(LHPError):
    """Raised when extension detection operations fail."""
    
    def __init__(self, message: str, suggestions: list = None):
        super().__init__(
            category=ErrorCategory.GENERAL,
            code_number="003",
            title="Extension Detection Error",
            details=message,
            suggestions=suggestions or ["Check VS Code installation", "Verify VS Code CLI access"]
        )


class ExtensionInfo:
    """Information about a VS Code extension."""
    
    def __init__(self, identifier: str, display_name: str, version: str, 
                 is_active: bool, description: str = ""):
        self.identifier = identifier
        self.display_name = display_name
        self.version = version
        self.is_active = is_active
        self.description = description
    
    def __repr__(self):
        return f"ExtensionInfo(id={self.identifier}, name={self.display_name}, version={self.version}, active={self.is_active})"


class VSCodeExtensionDetector:
    """Detects VS Code extensions that might conflict with YAML schema associations."""
    
    def __init__(self):
        self._extensions_directory = self._get_extensions_directory()
        self._known_yaml_extensions = {
            "redhat.vscode-yaml": {
                "name": "YAML",
                "description": "YAML Language Support by Red Hat",
                "potential_conflicts": ["yaml.schemas", "yaml.validate", "yaml.completion"],
                "severity": "high"
            },
            "ms-kubernetes-tools.vscode-kubernetes-tools": {
                "name": "Kubernetes",
                "description": "Kubernetes extension for VS Code",
                "potential_conflicts": ["yaml.schemas"],
                "severity": "medium"
            },
            "ms-vscode.vscode-yaml-sort": {
                "name": "YAML Sort",
                "description": "Sort YAML files",
                "potential_conflicts": ["yaml.format"],
                "severity": "low"
            },
            "ms-vscode.vscode-yaml": {
                "name": "YAML Support",
                "description": "YAML support for VS Code",
                "potential_conflicts": ["yaml.schemas", "yaml.validate"],
                "severity": "high"
            },
            "docsmsft.docs-yaml": {
                "name": "Docs Authoring Pack",
                "description": "Microsoft Docs YAML support",
                "potential_conflicts": ["yaml.schemas"],
                "severity": "medium"
            },
            "ansible.ansible-language-server": {
                "name": "Ansible",
                "description": "Ansible language server",
                "potential_conflicts": ["yaml.schemas"],
                "severity": "medium"
            },
            "hashicorp.terraform": {
                "name": "Terraform",
                "description": "Terraform language support",
                "potential_conflicts": ["yaml.schemas"],
                "severity": "low"
            }
        }
    
    def _get_extensions_directory(self) -> Path:
        """Get the platform-specific VS Code extensions directory."""
        system = platform.system()
        
        if system == "Windows":
            return Path.home() / ".vscode" / "extensions"
        elif system == "Darwin":
            return Path.home() / ".vscode" / "extensions"
        else:  # Linux and others
            return Path.home() / ".vscode" / "extensions"
    
    def _get_vscode_executable(self) -> str:
        """Get the VS Code executable name for the current platform."""
        system = platform.system()
        
        if system == "Windows":
            return "code.cmd"
        else:
            return "code"
    
    def is_vscode_installed(self) -> bool:
        """Check if VS Code is installed and accessible."""
        try:
            code_executable = self._get_vscode_executable()
            result = subprocess.run(
                [code_executable, "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            return False
    
    def get_installed_extensions_via_cli(self) -> List[ExtensionInfo]:
        """
        Get list of installed extensions using VS Code CLI.
        
        Returns:
            List of installed extension information
            
        Raises:
            ExtensionDetectorError: If extensions cannot be retrieved
        """
        if not self.is_vscode_installed():
            raise ExtensionDetectorError("VS Code is not installed or not accessible")
        
        try:
            code_executable = self._get_vscode_executable()
            result = subprocess.run(
                [code_executable, "--list-extensions", "--show-versions"],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                raise ExtensionDetectorError(f"Failed to list extensions: {result.stderr}")
            
            extensions = []
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    if '@' in line:
                        identifier, version = line.rsplit('@', 1)
                        extensions.append(ExtensionInfo(
                            identifier=identifier,
                            display_name=identifier.split('.')[-1],
                            version=version,
                            is_active=True  # CLI only shows active extensions
                        ))
            
            return extensions
            
        except subprocess.TimeoutExpired:
            raise ExtensionDetectorError("Timeout while retrieving extensions")
        except Exception as e:
            raise ExtensionDetectorError(f"Failed to retrieve extensions: {str(e)}")
    
    def get_installed_extensions_via_filesystem(self) -> List[ExtensionInfo]:
        """
        Get list of installed extensions by scanning the filesystem.
        
        Returns:
            List of installed extension information
            
        Raises:
            ExtensionDetectorError: If extensions cannot be retrieved
        """
        if not self._extensions_directory.exists():
            return []
        
        try:
            extensions = []
            
            for ext_dir in self._extensions_directory.iterdir():
                if ext_dir.is_dir():
                    # Parse extension directory name (usually format: publisher.name-version)
                    dir_name = ext_dir.name
                    
                    # Skip system directories
                    if dir_name.startswith('.'):
                        continue
                    
                    # Try to extract identifier and version
                    if '-' in dir_name:
                        identifier_part, version_part = dir_name.rsplit('-', 1)
                        
                        # Check if this looks like a version (contains digits)
                        if any(c.isdigit() for c in version_part):
                            identifier = identifier_part
                            version = version_part
                        else:
                            identifier = dir_name
                            version = "unknown"
                    else:
                        identifier = dir_name
                        version = "unknown"
                    
                    # Try to read package.json for more details
                    package_json_path = ext_dir / "package.json"
                    display_name = identifier.split('.')[-1] if '.' in identifier else identifier
                    description = ""
                    
                    if package_json_path.exists():
                        try:
                            with open(package_json_path, 'r', encoding='utf-8') as f:
                                package_data = json.load(f)
                                display_name = package_data.get('displayName', display_name)
                                description = package_data.get('description', '')
                                if 'version' in package_data:
                                    version = package_data['version']
                        except (json.JSONDecodeError, Exception):
                            pass
                    
                    extensions.append(ExtensionInfo(
                        identifier=identifier,
                        display_name=display_name,
                        version=version,
                        is_active=True,  # Assume active if installed
                        description=description
                    ))
            
            return extensions
            
        except Exception as e:
            raise ExtensionDetectorError(f"Failed to scan extensions directory: {str(e)}")
    
    def get_installed_extensions(self) -> List[ExtensionInfo]:
        """
        Get list of installed extensions using the best available method.
        
        Returns:
            List of installed extension information
        """
        # Try CLI first (more reliable), fall back to filesystem
        try:
            return self.get_installed_extensions_via_cli()
        except ExtensionDetectorError:
            try:
                return self.get_installed_extensions_via_filesystem()
            except ExtensionDetectorError:
                return []
    
    def get_yaml_extensions(self) -> List[ExtensionInfo]:
        """
        Get list of installed YAML-related extensions.
        
        Returns:
            List of YAML-related extension information
        """
        all_extensions = self.get_installed_extensions()
        yaml_extensions = []
        
        for ext in all_extensions:
            # Check if extension ID matches known YAML extensions
            if ext.identifier in self._known_yaml_extensions:
                yaml_extensions.append(ext)
            # Check if extension seems YAML-related based on name/description
            elif any(keyword in ext.display_name.lower() for keyword in ['yaml', 'yml']):
                yaml_extensions.append(ext)
            elif any(keyword in ext.description.lower() for keyword in ['yaml', 'yml']):
                yaml_extensions.append(ext)
        
        return yaml_extensions
    
    def get_conflicting_extensions(self) -> List[Tuple[ExtensionInfo, Dict[str, Any]]]:
        """
        Get list of extensions that might conflict with LHP schema associations.
        
        Returns:
            List of tuples containing extension info and conflict details
        """
        yaml_extensions = self.get_yaml_extensions()
        conflicts = []
        
        for ext in yaml_extensions:
            if ext.identifier in self._known_yaml_extensions:
                conflict_info = self._known_yaml_extensions[ext.identifier]
                conflicts.append((ext, conflict_info))
        
        return conflicts
    
    def analyze_extension_conflicts(self) -> Dict[str, Any]:
        """
        Analyze potential conflicts with installed extensions.
        
        Returns:
            Dictionary containing conflict analysis results
        """
        conflicts = self.get_conflicting_extensions()
        
        analysis = {
            "total_extensions": len(self.get_installed_extensions()),
            "yaml_extensions": len(self.get_yaml_extensions()),
            "conflicting_extensions": len(conflicts),
            "conflicts": [],
            "severity_summary": {"high": 0, "medium": 0, "low": 0},
            "recommendations": []
        }
        
        for ext_info, conflict_details in conflicts:
            conflict_entry = {
                "extension": {
                    "id": ext_info.identifier,
                    "name": ext_info.display_name,
                    "version": ext_info.version,
                    "description": ext_info.description
                },
                "conflict_details": conflict_details
            }
            analysis["conflicts"].append(conflict_entry)
            
            # Update severity summary
            severity = conflict_details.get("severity", "low")
            analysis["severity_summary"][severity] += 1
        
        # Generate recommendations
        if analysis["conflicting_extensions"] > 0:
            analysis["recommendations"].append(
                "Consider reviewing your YAML extension configuration to avoid conflicts"
            )
            
            high_severity_count = analysis["severity_summary"]["high"]
            if high_severity_count > 0:
                analysis["recommendations"].append(
                    f"Found {high_severity_count} high-severity conflict(s) that may override LHP schema associations"
                )
        else:
            analysis["recommendations"].append(
                "No conflicting YAML extensions detected"
            )
        
        return analysis
    
    def get_recommended_actions(self) -> List[Dict[str, str]]:
        """
        Get recommended actions to resolve extension conflicts.
        
        Returns:
            List of recommended actions
        """
        conflicts = self.get_conflicting_extensions()
        actions = []
        
        for ext_info, conflict_details in conflicts:
            severity = conflict_details.get("severity", "low")
            
            if severity == "high":
                actions.append({
                    "extension": ext_info.identifier,
                    "action": "review_settings",
                    "description": f"Review {ext_info.display_name} settings to ensure compatibility with LHP schemas",
                    "priority": "high"
                })
            elif severity == "medium":
                actions.append({
                    "extension": ext_info.identifier,
                    "action": "monitor",
                    "description": f"Monitor {ext_info.display_name} for potential conflicts",
                    "priority": "medium"
                })
        
        return actions
    
    def generate_conflict_report(self) -> str:
        """
        Generate a human-readable conflict report.
        
        Returns:
            Formatted conflict report as a string
        """
        analysis = self.analyze_extension_conflicts()
        
        report = []
        report.append("VS Code Extension Conflict Analysis")
        report.append("=" * 40)
        report.append(f"Total Extensions: {analysis['total_extensions']}")
        report.append(f"YAML Extensions: {analysis['yaml_extensions']}")
        report.append(f"Conflicting Extensions: {analysis['conflicting_extensions']}")
        report.append("")
        
        if analysis["conflicting_extensions"] > 0:
            report.append("Potential Conflicts:")
            report.append("-" * 20)
            
            for conflict in analysis["conflicts"]:
                ext = conflict["extension"]
                details = conflict["conflict_details"]
                
                report.append(f"• {ext['name']} ({ext['id']})")
                report.append(f"  Version: {ext['version']}")
                report.append(f"  Severity: {details['severity'].upper()}")
                report.append(f"  Potential Issues: {', '.join(details['potential_conflicts'])}")
                report.append("")
        
        if analysis["recommendations"]:
            report.append("Recommendations:")
            report.append("-" * 15)
            for rec in analysis["recommendations"]:
                report.append(f"• {rec}")
        
        return "\n".join(report)
    
    def check_extension_compatibility(self, extension_id: str) -> Dict[str, Any]:
        """
        Check compatibility of a specific extension with LHP schemas.
        
        Args:
            extension_id: Extension identifier to check
            
        Returns:
            Dictionary containing compatibility information
        """
        installed_extensions = self.get_installed_extensions()
        
        # Find the extension
        target_extension = None
        for ext in installed_extensions:
            if ext.identifier == extension_id:
                target_extension = ext
                break
        
        if not target_extension:
            return {
                "extension_id": extension_id,
                "installed": False,
                "compatible": True,
                "message": "Extension not installed"
            }
        
        # Check if it's a known problematic extension
        if extension_id in self._known_yaml_extensions:
            conflict_info = self._known_yaml_extensions[extension_id]
            return {
                "extension_id": extension_id,
                "installed": True,
                "compatible": conflict_info["severity"] == "low",
                "severity": conflict_info["severity"],
                "potential_conflicts": conflict_info["potential_conflicts"],
                "message": f"Extension may conflict with LHP schema associations"
            }
        
        return {
            "extension_id": extension_id,
            "installed": True,
            "compatible": True,
            "message": "No known conflicts"
        } 