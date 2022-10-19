#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:40:58 2022
@author: alvaro
"""
import sys
import logging
import logging.config
from argparse import ArgumentParser
from mardi_importer.importer.Importer import Importer, ImporterException
from mardi_importer.wikidata.EntityCreator import EntityCreator
from mardi_importer.zbmath.ZBMathSource import ZBMathSource
from mardi_importer.zbmath.ZBMathConfigParser import ZBMathConfigParser
from mardi_importer.cran.CRANSource import CRANSource
from mardi_importer.cran.CRANEntityCreator import CRANEntityCreator


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, choices=["ZBMath", "CRAN"])
    parser.add_argument("--conf_path", required=False)
    return parser


def main():
    logging.config.fileConfig("logging_config.ini", disable_existing_loggers=False)
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
        # data_source.write_data_dump()
        # data_source.process_data()
        # data_source.write_error_ids()

        from mardi_importer.integrator.Integrator import Integrator

        i = Integrator(conf_path=args.conf_path)
        i.check_or_create_db_table()
        i.create_units(
            id_list=["Q177", "Q192783"], languages=["en", "de"], recurse=True
        )
        i.import_items()
        i.engine.dispose()

    elif args.mode == "CRAN":
        # an object to create entities copied from Wikidata
        entity_list = "/config/Properties_to_import_from_WD.txt"
        entityCreator = CRANEntityCreator(entity_list)

        # an object to import metadata related to R packages from CRAN
        data_source = CRANSource()

        # A wrapper for the import process
        importer = Importer(entityCreator, data_source)
        importer.import_all()


if __name__ == "__main__":
    main()

# references = relation["references"][0]["snaks"]
# for ref_id in references:
#     # add property name of reference
#     self.add_secondary_units(unit_id=ref_id, languages=languages)
#     # for each target of this property in references, add item if it is an item
#     for ref_snak in references["ref_id"]:
#         if "id" in ref_snak["datavalue"]["value"]:
#             self.add_secondary_units(unit_id=ref_snak["datavalue"]["value"]["id"])


# target = self.get_target(data_value)
# target["references"] = references
# claims.append(target)
