# -*- coding: utf-8 -*-
from importer.Importer import ADataSource

class DummyDataSource(ADataSource):
    """
    A dummy for unit testing
    """
    
    def pull():
        """Overrides abstract method."""
        pass
