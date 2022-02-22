#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:50:55 2022

@author: alvaro
"""
from importer.Importer import AEntityCreator
import pandas as pd

class EntityCreator(AEntityCreator):
    """
    Creates entities in a local Wikibase by copying them from Wikidata.
    """
    entity_df = None # data frame of entities to import
    
    def read_entity_list(self, path):
        """Overrides abstract method."""
        self.entity_df = pd.read_csv(path)
    
    def create_entities(self):
        """Overrides abstract method."""
        pass
    