#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from mardi_importer.importer import Importer


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument(
        "--mode", type=str, required=True, choices=["zbmath", "cran", "polydb","openml", "zenodo", "miplib"]
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Import at most this many records. Useful for a trial run."
    )
    parser.add_argument(
        "--instances", nargs="*", default=None,
        help="Import only these named records, skipping the source listing."
    )
    parser.add_argument(
        "--no-push", dest="push", action="store_false",
        help="Fetch and parse, but write nothing to the Wikibase."
    )
    return parser

def main(**args):

    pull = True
    push = args.get("push", True)

    if args["mode"] == "zbmath":
        pull = False

    source = Importer.create_source(args["mode"])

    if args.get("limit") is not None:
        source.instance_limit = args["limit"]
    if args.get("instances"):
        source.instance_names = args["instances"]

    if pull:
        source.pull()
    if push:
        source.push()

if __name__ == "__main__":
    args = get_parser().parse_args()
    main(**vars(args))
