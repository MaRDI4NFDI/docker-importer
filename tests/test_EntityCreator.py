#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:54:34 2022

@author: alvaro
"""

import unittest
from wikidata.EntityCreator import EntityCreator

class test_EntityCreator(unittest.TestCase):
    entity_list = "/tests/data/test_properties_to_import_from_WD.txt"

    def test_01(self):
        """Tests that a Wikidata entity creator can be instantiated at all."""
        creator = EntityCreator(test_EntityCreator.entity_list)
        self.assertTrue(creator, 'Wikidata entity creator could not be instantiated')
        
    def test_02(self):
        """Tests that entities can be imported from Wikidata into the local Wikibase."""
        creator = EntityCreator(test_EntityCreator.entity_list)
        resp_df = creator.create_entities()
        self.assertTrue(len(resp_df)==1, "Failed importing entities from Wikibase.")        
        # P31 is imported from Wikidata and mapped to P1 in local wikibase
        self.assertTrue(resp_df.loc[0, 'wbs_local_id'] == 'P1', "Failed importing entities from Wikibase.")        
        self.assertTrue(resp_df.loc[0, 'wbs_original_id'] == 'P31', "Failed importing entities from Wikibase.")        
