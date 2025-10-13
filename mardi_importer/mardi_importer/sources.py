from typing import Dict, Any, Optional
from .registry import SourceRegistry
from mardi_importer.base import Importer

def import_source(
    source_name: str,
    pull: bool = True,
    push: bool = True
) -> Dict[str, Any]:
    """
    Universal import function using the registry.
    
    Args:
        source_name: Registered name of the source
        pull: Whether to pull data (default: True)
        push: Whether to push data (default: True)
    """
    source = SourceRegistry.create_source(source_name)
    importer = Importer(source)
    return importer.import_all(pull=pull, push=push)
