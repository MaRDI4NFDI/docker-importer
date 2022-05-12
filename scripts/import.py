#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:40:58 2022

@author: alvaro
"""
from argparse import ArgumentParser

# from importer.Importer import Importer, ImporterException
# from wikidata.EntityCreator import EntityCreator
from mardi_importer.zbmath.ZBMathSource import ZBMathSource


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


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument(
        "--from_date",
        type=str,
        default=None,
        required=False,
        help="required date format: 2012-12-12",
    )
    parser.add_argument(
        "--until_date",
        type=str,
        required=False,
        default=None,
        help="required date format: 2012-12-12",
    )
    parser.add_argument("--out_dir", type=str, required=True)
    parser.add_argument("--tags", nargs="+", required=False, default=tags)
    parser.add_argument("--raw_dump_path", required=False, default=None)
    parser.add_argument("--split_id", required=False, default=None)
    parser.add_argument("--processed_dump_path", required=False, default=None)
    return parser


def main():
    """importer main"""
    # Parse command-line options
    args = get_parser().parse_args()
    data_source = ZBMathSource(
        out_dir=args.out_dir,
        tags=args.tags,
        from_date=args.from_date,
        until_date=args.until_date,
        raw_dump_path=args.raw_dump_path,
        split_id=args.split_id,
        processed_dump_path=args.processed_dump_path,
    )
    # data_source.write_data_dump()
    # data_source.process_data()
    data_source.get_invalid_dois()
    data_source.write_error_ids()
    print("Finished")
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


if __name__ == "__main__":
    main()