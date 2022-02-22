# -*- coding: utf-8 -*-
from importer.Importer import AEntityCreator

class DummyEntityCreator(AEntityCreator):
    """
    A dummy for unit testing
    """
    
    def read_entity_list(path):
        """Overrides abstract method."""
        pass
