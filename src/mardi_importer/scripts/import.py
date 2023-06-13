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
from mardi_importer.importer.Importer import Importer
from mardi_importer.zbmath.ZBMathSource import ZBMathSource
from mardi_importer.zbmath.ZBMathConfigParser import ZBMathConfigParser
from mardi_importer.cran.CRANSource import CRANSource
from mardi_importer.polydb.PolyDBSource import PolyDBSource
from mardi_importer.integrator.MardiIntegrator import MardiIntegrator


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument(
        "--mode", type=str, required=True, choices=["ZBMath", "CRAN", "polydb"]
    )
    parser.add_argument("--conf_path", required=False)
    parser.add_argument("--wikidata_id_file_path", required=False)
    return parser


def main(**args):
    # logging.config.fileConfig("logging_config.ini", disable_existing_loggers=False)
    # Parse command-line arguments

    if args["mode"] == "ZBMath":

        if args["conf_path"] is None:
            sys.exit("--conf_path is required for --mode ZBMath")
        conf_parser = ZBMathConfigParser(args["conf_path"])
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
        importer = Importer(data_source)
        importer.import_all(pull=False)

    elif args["mode"] == "CRAN":

        # an object to import metadata related to R packages from CRAN
        data_source = CRANSource()

        # A wrapper for the import process
        importer = Importer(data_source)
        importer.import_all()

    elif args["mode"] == "polydb":
        data_source = PolyDBSource()
        importer = Importer(data_source)
        importer.import_all()


if __name__ == "__main__":
    args = get_parser().parse_args()
    main(**vars(args))
