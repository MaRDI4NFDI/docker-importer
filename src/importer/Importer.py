#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:53:53 2022

@author: alvaro
"""
class Importer:
    """Controller class for importing data from an external source to the local Wikibase."""
    def __init__(self, entityCreator):
        """
        Construct.
        """
        self.entityCreator = entityCreator
    
    def import_all(self):
        """
        Manages the import process.
        """
        pass

class AEntityCreator():
    """Abstract base class for creating entities in a local Wikibase."""

    def read_entity_list(self, path):
        """
        Reads a list of entities to import.
        
        @param path: The list is a plain-text file with one entity (Px or Qx) per line.
        @returns: pandas data frame
        """
        raise NotImplementedError
    
    def create_entities(self):
        """
        Creates all necessary entities in the local Wikibase.
        @returns panas data frame of mappings between local Wikibase and Wikidata (or external Wikibase)
        """
        raise NotImplementedError
        
    
class ImporterException(Exception):
    """Failed importer operation."""
    pass
