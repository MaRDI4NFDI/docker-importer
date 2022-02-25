#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:01:56 2022

@author: alvaro
"""

import unittest
from importer.Importer import Importer
from DummyEntityCreator import DummyEntityCreator as EntityCreator
from DummyDataSource import DummyDataSource as DataSource

class test_Importer(unittest.TestCase):

    def test_01(self):
        """Tests that an importer can be instantiated at all."""
        creator = EntityCreator()
        data_source = DataSource()
        importer = Importer(creator, data_source)
        self.assertTrue(importer, 'Importer could not be instantiated')