#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from mardi_importer.importer import Importer


def get_parser():
    """Get arguments parser"""
    parser = ArgumentParser()
    parser.add_argument(
        "--mode", type=str, required=True, choices=["zbmath", "cran", "polydb","openml", "zenodo"]
    )
    return parser

def main(**args): 

    pull = True
    push = True

    if args["mode"] == "zbmath":
        pull = False

    source = Importer.create_source(args["mode"])
    if pull:
        source.pull()
    if push:
        source.push()

if __name__ == "__main__":
    args = get_parser().parse_args()
    main(**vars(args))
