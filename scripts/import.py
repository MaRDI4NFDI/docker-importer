#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:40:58 2022
@author: alvaro
"""
import sys
from optparse import OptionParser
from importer.Importer import Importer, ImporterException
from wikidata.EntityCreator import EntityCreator
from zbmath.ZBMathSource import ZBMathSource
from cran.CRANSource import CRANSource

try:
    # Parse command-line options
    optionsParser = OptionParser()
    (options, args) = optionsParser.parse_args()

    # an object to create entities copied from Wikidata    
    entity_list = "/config/Properties_to_import_from_WD.txt"
    entityCreator = EntityCreator(entity_list)

    # an object to import paper references related to certain softwares from zbMath
    # software_list = "/config/swMATH-software-list.csv"
    # data_source = ZBMathSource(software_list)

    # an object to import metadata related to R packages from CRAN
    data_source = CRANSource()
    
    # A wrapper for the import process
    importer = Importer(entityCreator, data_source)
    importer.import_all()

except ImporterException as e:
     print(e)
     optionsParser.print_help()
     sys.exit(1)