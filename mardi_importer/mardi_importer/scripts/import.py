import sys
import logging
import logging.config
from argparse import ArgumentParser

from mardi_importer.zbmath import ZBMathSource, ZBMathConfigParser
#from mardi_importer.openml import OpenMLSource
from mardi_importer.importer import Importer
from mardi_importer.cran import CRANSource
from mardi_importer.polydb import PolyDBSource
from mardi_importer.zenodo import ZenodoSource


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument(
        "--mode", type=str, required=True, choices=["ZBMath", "CRAN", "polydb","OpenML", "zenodo"]
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
        importer.import_all(pull=False, push=True)

    elif args["mode"] == "OpenML":
        # if args["conf_path"] is None:
        #     sys.exit("--conf_path is required for --mode OpenML")
        #conf_parser = OpenMLConfigParser(args["conf_path"])
        #conf = conf_parser.parse_config()

        #data_source = OpenMLSource()
        #importer = Importer(data_source)
        #importer.import_all(pull=False, push=True)
        print('Deactivate due to error in openml package')

    elif args["mode"] == "CRAN":
        data_source = CRANSource()
        importer = Importer(data_source)
        importer.import_all()

    elif args["mode"] == "polydb":
        data_source = PolyDBSource()
        importer = Importer(data_source)
        importer.import_all()

    elif args["mode"] == "zenodo":
        data_source = ZenodoSource(resourceTypes = ["dataset"], orcid_id_file = "/mardi_importer/mardi_importer/zenodo/orcids-all.csv")
        importer = Importer(data_source)
        importer.import_all()


if __name__ == "__main__":
    args = get_parser().parse_args()
    main(**vars(args))
