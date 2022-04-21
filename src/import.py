#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:40:58 2022

@author: alvaro
"""
import sys
from argparse import ArgumentParser

# from importer.Importer import Importer, ImporterException
# from wikidata.EntityCreator import EntityCreator
from zbmath.ZBMathSource import ZBMathSource


tags = [
    "author",
    "author_ids",
    "document_title",
    "source",
    "classifications",
    "language",
    "links",
    "keywords",
    "doi",
    "publication_year",
    "serial",
]

# Parse command-line options
parser = ArgumentParser()
parser.add_argument(
    "--from_date", type=str, required=False, help="required date format: 2012-12-12"
)
parser.add_argument(
    "--until_date", type=str, required=False, help="required date format: 2012-12-12"
)
parser.add_argument("--out_dir", type=str, required=True)
parser.add_argument("--tags", nargs="+", required=False, default=tags)
parser.add_argument("--raw_dump_path", required=False, default=None)

args = parser.parse_args()

data_source = ZBMathSource(
    out_dir=args.out_dir, tags=args.tags, raw_dump_path=args.raw_dump_path
)
# data_source.write_data_dump()
data_source.process_data()
print("Finished!!")
print(data_source.unknown_doi_agency_dict)

# an object to create entities copied from Wikidata
# entity_list = "/config/Properties_to_import_from_WD.txt"
# entityCreator = EntityCreator(entity_list)

# an object to import paper references related to certain softwares from zbMath
# software_list = "/config/swMATH-software-list.csv"
# data_source = ZBMathSource(software_list)

# A wrapper for the import process
# importer = Importer(entityCreator, data_source)
# importer.import_all()
