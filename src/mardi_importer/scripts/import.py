#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:40:58 2022
@author: alvaro
"""
import sys
from argparse import ArgumentParser
from mardi_importer.importer.Importer import Importer, ImporterException
from mardi_importer.wikidata.EntityCreator import EntityCreator
from mardi_importer.zbmath.ZBMathSource import ZBMathSource
from mardi_importer.zbmath.ZBMathConfigParser import ZBMathConfigParser
from mardi_importer.cran.CRANSource import CRANSource


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, choices=["ZBMath", "CRAN"])
    parser.add_argument("--conf_path", required=False)
    return parser


def main():
    # Parse command-line arguments
    args = get_parser().parse_args()

    if args.mode == "ZBMath":

        if args.conf_path is None:
            sys.exit("--conf_path is required for --mode ZBMath")
        conf_parser = ZBMathConfigParser(args.conf_path)
        conf = conf_parser.parse_config()

        data_source = ZBMathSource(
            out_dir=conf["out_dir"],
            tags=conf["tags"],
            from_date=conf["from_date"],
            until_date=conf["until_date"],
            raw_dump_path=conf["raw_dump_path"],
            split_id=conf["split_id"],
            processed_dump_path=conf["processed_dump_path"],
        )
        data_source.write_data_dump()
        data_source.process_data()
        data_source.write_error_ids()

    elif args.mode == "CRAN":
        # an object to create entities copied from Wikidata
        entity_list = "/config/Properties_to_import_from_WD.txt"
        entityCreator = EntityCreator(entity_list)

        # an object to import metadata related to R packages from CRAN
        data_source = CRANSource()

        # A wrapper for the import process
        importer = Importer(entityCreator, data_source)
        importer.import_all()


if __name__ == "__main__":
    main()
