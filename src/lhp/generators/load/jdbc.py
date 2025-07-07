"""JDBC load generator with secret support."""

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action

class JDBCLoadGenerator(BaseActionGenerator):
    """Generate JDBC load actions with secret support."""
    
    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
    
    def generate(self, action: Action, context: dict) -> str:
        """Generate JDBC load code with secret substitution."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ValueError("JDBC source must be a configuration object")
        
        # Process source config through substitution manager first if available
        if 'substitution_manager' in context:
            source_config = context['substitution_manager'].substitute_yaml(source_config)
        
        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "jdbc_url": source_config.get("url"),
            "jdbc_user": source_config.get("user"),
            "jdbc_password": source_config.get("password"),
            "jdbc_driver": source_config.get("driver"),
            "jdbc_query": source_config.get("query"),
            "jdbc_table": source_config.get("table"),
            "description": action.description or f"JDBC source: {action.name}"
        }
        
        code = self.render_template("load/jdbc.py.j2", template_context)
        
        # Process secret substitutions
        if 'substitution_manager' in context:
            code = context['substitution_manager'].replace_secret_placeholders(code)
        
        return code 