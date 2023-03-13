#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:53:53 2022

@author: alvaro
"""


class Importer:
    """Controller class for importing data from an external source to the local Wikibase."""

    def __init__(self, dataSource):
        """
        Construct.
        Args:
            entityCreator: object implementing AEntityCreator
            dataSource: object implementig ADataSource
        """
        self.dataSource = dataSource

    def import_all(self):
        """
        Manages the import process.
        """
        self.dataSource.setup()
        #self.dataSource.pull()
        #self.dataSource.push()


class ADataSource:
    """Abstract base class for reading data from external sources."""

    def write_data_dump(self):
        """
        Write data dump from API.
        """
        raise NotImplementedError

    def process_data(self):
        """
        Process data dump.
        """
        raise NotImplementedError

    def pull(self):
        """
        Pull data from DataSource
        """
        raise NotImplementedError

    def push(self):
        """
        Push data into the MaRDI knowledge graph.
        """
        raise NotImplementedError

class AConfigParser:
    """ Abstract base class for parsing config files """

    def parse_config(self):
        """
        Parse config file.
        Returns:
            Dictionary: Dictionary containing config values
        """


class ImporterException(Exception):
    """Failed importer operation."""

    pass
