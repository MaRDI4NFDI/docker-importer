from typing import Dict, Type, Tuple
import os

class SourceRegistry:
    """Central registry for all data sources."""
    _sources: Dict[str, Type['ADataSource']] = {}
    _credentials: Dict[str, Tuple[str, str]] = {}
    
    @classmethod
    def register(cls, name: str, source_class: Type['ADataSource'], 
                 user_env_var: str = None, password_env_var: str = None):
        """Register a data source with its credentials.
        
        Args:
            name: Source name
            source_class: Source class
            user_env_var: Environment variable name for username (defaults to {NAME}_USER)
            password_env_var: Environment variable name for password (defaults to {NAME}_PASS)
        """
        cls._sources[name] = source_class
        
        # Default environment variable names
        if user_env_var is None:
            user_env_var = f"{name.upper()}_USER"
        if password_env_var is None:
            password_env_var = f"{name.upper()}_PASS"
            
        cls._credentials[name] = (user_env_var, password_env_var)
    
    @classmethod
    def create_source(cls, name: str) -> 'ADataSource':
        """Create a source instance with appropriate credentials."""
        if name not in cls._sources:
            raise ValueError(f"Unknown source: {name}")
        
        user_env_var, password_env_var = cls._credentials[name]
        user = os.environ.get(user_env_var)
        password = os.environ.get(password_env_var)
        
        if not user or not password:
            missing = []
            if not user:
                missing.append(user_env_var)
            if not password:
                missing.append(password_env_var)
            raise ValueError(f"Missing required environment variables for {name}: {', '.join(missing)}")
        
        return cls._sources[name](user=user, password=password)