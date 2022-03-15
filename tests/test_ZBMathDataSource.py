#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 15:52:28 2022

@author: alvaro
"""

import unittest
from zbmath.ZBMathSource import ZBMathSource

class test_ZBMathSource(unittest.TestCase):
    sw_list = "/tests/data/test_swMATH-software-list.csv"

    def test_01(self):
        """Tests that a zbMath data source can be instantiated at all."""
        data_source = ZBMathSource(test_ZBMathSource.sw_list)
        self.assertTrue(data_source, 'ZbMathDataSource could not be instantiated')

    def test_01(self):
        """Tests that a list of software can be read."""
        data_source = ZBMathSource(test_ZBMathSource.sw_list)
        self.assertTrue(len(data_source.software_list)>0, 'Software list could not be read')
