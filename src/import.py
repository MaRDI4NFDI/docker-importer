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

try:
    # Parse command-line options
    optionsParser = OptionParser()
    (options, args) = optionsParser.parse_args()
    print(options)
    
    # A wrapper for the import process
    entity_list = "/config/Properties_to_import_from_WD.txt"
    entityCreator = EntityCreator()
    entityCreator.read_entity_list(entity_list)
    importer = Importer(entityCreator)
    importer.import_all()

except ImporterException as e:
     print(e)
     optionsParser.print_help()
     sys.exit(1)
