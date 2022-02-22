#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:54:34 2022

@author: alvaro
"""

import unittest
from wikidata.EntityCreator import EntityCreator

class test_EntityCreator(unittest.TestCase):

    def test_01(self):
        """Tests that a Wikidata entity creator can be instantiated at all."""
        creator = EntityCreator()
        self.assertTrue(creator, 'Wikidata entity creator could not be instantiated')
        
    def test_02(self):
        """Tests that a list of entitis to import can be read."""
        entity_list = "/config/Properties_to_import_from_WD.txt"
        creator = EntityCreator()
        creator.read_entity_list(entity_list)
        self.assertTrue(len(creator.entity_df) > 0, "Entity data frame could not be read.")
        
    def test_03(self):
        """Tests that entities can be imported from Wikidata into the local Wikibase."""
        entity_list = "/config/Properties_to_import_from_WD.txt"
        creator = EntityCreator()
        creator.read_entity_list(entity_list)
        creator.create_entities()
