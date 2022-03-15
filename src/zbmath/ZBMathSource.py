#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 18:53:53 2022

@author: alvaro
"""
from importer.Importer import ADataSource
import pandas as pd

class ZBMathSource(ADataSource):
    """Reads data from zb math API. Will only read bibliographical data related to specific software."""

    def __init__(self, path):
        """
        @param path: string path to a csv file with one software name per row and 2 columns 'swMATH work ID' and 'Software'
        """
        # load the list of swMath software
        software_df = pd.read_csv(path)
        self.software_list = software_df['Software'].tolist()

    def pull(self):
        """
        Overrides abstract method.
        This method uses the zbmath API to:
            * filter paper references related to the softwares in self.software_list.
        
        """
        pass