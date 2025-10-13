#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from mardi_importer.sources import import_source


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

    if args["mode"] == "ZBMath":
        pull = False

    import_source(args["mode"], pull=pull, push=push)

if __name__ == "__main__":
    args = get_parser().parse_args()
    main(**vars(args))
