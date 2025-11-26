import yaml
from pathlib import Path
from typing import Dict, Any, List
from ..models.config import FlowGroup, Template, Preset
from ..utils.error_formatter import LHPError


class YAMLParser:
    """Parse and validate YAML configuration files."""

    def __init__(self):
        pass

    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse a single YAML file."""
        # Import here to avoid circular imports
        try:
            from ..utils.error_formatter import LHPError
        except ImportError:
            LHPError = None
            
        from ..utils.yaml_loader import load_yaml_file
        try:
            content = load_yaml_file(file_path, error_context=f"YAML file {file_path}")
            return content or {}
        except Exception as e:
            # Check if it's an LHPError that should be re-raised
            if LHPError and isinstance(e, LHPError):
                raise  # Re-raise LHPError as-is
            elif isinstance(e, ValueError):
                # For backward compatibility, convert back to generic error for non-LHPErrors
                if "File not found" in str(e):
                    raise ValueError(f"Error reading {file_path}: {e}")
                raise  # Re-raise ValueError as-is for YAML errors
            else:
                raise ValueError(f"Error reading {file_path}: {e}")

    def parse_flowgroups_from_file(self, file_path: Path) -> List[FlowGroup]:
        """Parse one or more FlowGroups from a YAML file.
        
        Supports both multi-document syntax (---) and flowgroups array syntax.
        
        Args:
            file_path: Path to YAML file containing one or more flowgroups
            
        Returns:
            List of FlowGroup objects
            
        Raises:
            ValueError: For duplicate flowgroup names, mixed syntax, or parsing errors
        """
        from ..utils.yaml_loader import load_yaml_documents_all
        
        # Load all documents from file
        try:
            documents = load_yaml_documents_all(file_path, error_context=f"flowgroup file {file_path}")
        except ValueError:
            # Re-raise with better context
            raise
        
        if not documents:
            raise ValueError(f"No content found in {file_path}")
        
        flowgroups = []
        seen_flowgroup_names = set()
        uses_array_syntax = False
        uses_regular_syntax = False
        
        # Process each document
        for doc_index, doc in enumerate(documents, start=1):
            # Check if this document uses array syntax
            if 'flowgroups' in doc:
                uses_array_syntax = True
                
                # Extract document-level shared fields
                shared_fields = {k: v for k, v in doc.items() if k != 'flowgroups'}
                
                # Process each flowgroup in the array
                for fg_config in doc['flowgroups']:
                    # Apply inheritance: only inherit if key not present in fg_config
                    inheritable_fields = ['pipeline', 'use_template', 'presets', 'operational_metadata', 'job_name']
                    for field in inheritable_fields:
                        if field not in fg_config and field in shared_fields:
                            fg_config[field] = shared_fields[field]
                    
                    # Check for duplicate flowgroup name
                    fg_name = fg_config.get('flowgroup')
                    if fg_name in seen_flowgroup_names:
                        raise ValueError(
                            f"Duplicate flowgroup name '{fg_name}' in file {file_path}"
                        )
                    if fg_name:
                        seen_flowgroup_names.add(fg_name)
                    
                    # Parse flowgroup
                    try:
                        flowgroups.append(FlowGroup(**fg_config))
                    except Exception as e:
                        raise ValueError(
                            f"Error parsing flowgroup in document {doc_index} of {file_path}: {e}"
                        )
            else:
                # Regular syntax (one flowgroup per document)
                uses_regular_syntax = True
                
                # Check for duplicate flowgroup name
                fg_name = doc.get('flowgroup')
                if fg_name in seen_flowgroup_names:
                    raise ValueError(
                        f"Duplicate flowgroup name '{fg_name}' in file {file_path}"
                    )
                if fg_name:
                    seen_flowgroup_names.add(fg_name)
                
                # Parse flowgroup
                try:
                    flowgroups.append(FlowGroup(**doc))
                except Exception as e:
                    raise ValueError(
                        f"Error parsing flowgroup in document {doc_index} of {file_path}: {e}"
                    )
        
        # Check for mixed syntax
        if uses_array_syntax and uses_regular_syntax:
            raise ValueError(
                f"Mixed syntax detected in {file_path}: cannot use both multi-document (---) "
                "and flowgroups array syntax in the same file"
            )
        
        return flowgroups

    def parse_flowgroup(self, file_path: Path) -> FlowGroup:
        """Parse a FlowGroup YAML file.
        
        Note: This method only supports single-flowgroup files. If the file contains
        multiple flowgroups (via --- separator or flowgroups array), use 
        parse_flowgroups_from_file() instead.
        """
        from ..utils.yaml_loader import load_yaml_documents_all
        
        # Check if file contains multiple flowgroups
        try:
            documents = load_yaml_documents_all(file_path)
        except ValueError:
            # If we can't even load it, fall back to original behavior
            content = self.parse_file(file_path)
            return FlowGroup(**content)
        
        # Check for multiple documents
        if len(documents) > 1:
            raise ValueError(
                f"File {file_path} contains multiple flowgroups (multiple documents). "
                "Use parse_flowgroups_from_file() instead."
            )
        
        # Check for array syntax
        if documents and 'flowgroups' in documents[0]:
            raise ValueError(
                f"File {file_path} contains multiple flowgroups (array syntax). "
                "Use parse_flowgroups_from_file() instead."
            )
        
        # Single flowgroup - use original parsing
        content = self.parse_file(file_path)
        return FlowGroup(**content)

    def parse_template(self, file_path: Path) -> Template:
        """Parse a Template YAML file.
        
        .. deprecated:: 0.6.3
            Use :func:`parse_template_raw` instead. This method will be removed in v0.7.0.
        """
        import warnings
        warnings.warn(
            "parse_template() is deprecated and will be removed in v0.7.0. "
            "Use parse_template_raw() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        content = self.parse_file(file_path)
        return Template(**content)
    
    def parse_template_raw(self, file_path: Path) -> Template:
        """Parse a Template YAML file with raw actions (no Action object creation).
        
        This is used during template loading to avoid validation of template syntax
        like {{ table_properties }}. Actions will be validated later during rendering
        when actual parameter values are available.
        """
        content = self.parse_file(file_path)
        
        # Create template with raw actions
        raw_actions = content.pop('actions', [])
        template = Template(**content, actions=raw_actions)
        template._raw_actions = True  # Set flag after creation
        return template

    def parse_preset(self, file_path: Path) -> Preset:
        """Parse a Preset YAML file."""
        content = self.parse_file(file_path)
        return Preset(**content)

    def discover_templates(self, templates_dir: Path) -> List[Template]:
        """Discover all Template files.
        
        .. deprecated:: 0.6.3
            This method is unused and will be removed in v0.7.0.
        """
        import warnings
        warnings.warn(
            "discover_templates() is deprecated and will be removed in v0.7.0.",
            DeprecationWarning,
            stacklevel=2
        )
        templates = []
        for yaml_file in templates_dir.glob("*.yaml"):
            if yaml_file.is_file():
                try:
                    template = self.parse_template(yaml_file)
                    templates.append(template)
                except Exception as e:
                    print(f"Warning: Could not parse template {yaml_file}: {e}")
        return templates

    def discover_presets(self, presets_dir: Path) -> List[Preset]:
        """Discover all Preset files."""
        presets = []
        for yaml_file in presets_dir.glob("*.yaml"):
            if yaml_file.is_file():
                try:
                    preset = self.parse_preset(yaml_file)
                    presets.append(preset)
                except Exception as e:
                    print(f"Warning: Could not parse preset {yaml_file}: {e}")
        return presets
