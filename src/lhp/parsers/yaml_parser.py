import logging
import threading
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from ..models.config import FlowGroup, Template, Preset
from ..utils.error_formatter import LHPError
from ..utils.yaml_loader import load_yaml_file


class YAMLParser:
    """Parse and validate YAML configuration files."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)


    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse a single YAML file."""

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



    def discover_presets(self, presets_dir: Path) -> List[Preset]:
        """Discover all Preset files."""
        presets = []
        for yaml_file in presets_dir.glob("*.yaml"):
            if yaml_file.is_file():
                try:
                    preset = self.parse_preset(yaml_file)
                    presets.append(preset)
                except Exception as e:
                    self.logger.warning(f"Could not parse preset {yaml_file}: {e}")
        return presets


class CachingYAMLParser:
    """Thread-safe caching wrapper for YAMLParser.
    
    Uses file path + modification time as cache key to automatically
    invalidate cache when files change.
    """
    
    def __init__(self, base_parser: Optional['YAMLParser'] = None, 
                 max_cache_size: int = 500) -> None:
        """Initialize caching parser.
        
        Args:
            base_parser: Underlying YAMLParser instance (creates new if None)
            max_cache_size: Maximum number of cached entries
        """
        self._parser: YAMLParser = base_parser or YAMLParser()
        self._cache: Dict[Tuple[str, float], List[FlowGroup]] = {}
        self._max_cache_size: int = max_cache_size
        self._lock: threading.RLock = threading.RLock()
        self._hits: int = 0
        self._misses: int = 0

    def parse_flowgroups_from_file(self, path: Path) -> List[FlowGroup]:
        """Parse flowgroups with caching based on file mtime.
        
        Args:
            path: Path to YAML file
            
        Returns:
            List of FlowGroup objects
        """
        resolved_path: Path = path.resolve()
        try:
            mtime: float = resolved_path.stat().st_mtime
        except OSError:
            # File doesn't exist or can't be accessed - don't cache
            return self._parser.parse_flowgroups_from_file(path)

        cache_key: Tuple[str, float] = (str(resolved_path), mtime)

        with self._lock:
            if cache_key in self._cache:
                self._hits += 1
                return self._cache[cache_key]

            self._misses += 1

            # Evict oldest entries if cache is full
            if len(self._cache) >= self._max_cache_size:
                # Remove ~10% of entries (FIFO approximation)
                keys_to_remove = list(self._cache.keys())[:self._max_cache_size // 10]
                for key in keys_to_remove:
                    del self._cache[key]

            # Parse and cache
            result: List[FlowGroup] = self._parser.parse_flowgroups_from_file(path)
            self._cache[cache_key] = result
            return result

    def clear_cache(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics.
        
        Returns:
            Dictionary with cache hits, misses, hit rate, and size
        """
        with self._lock:
            total: int = self._hits + self._misses
            hit_rate: float = (self._hits / total * 100) if total > 0 else 0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "total": total,
                "hit_rate_percent": round(hit_rate, 1),
                "cache_size": len(self._cache)
            }

    def __getattr__(self, name: str) -> Any:
        """Delegate other methods to base parser.
        
        Args:
            name: Attribute name
            
        Returns:
            Attribute from base parser
        """
        return getattr(self._parser, name)
