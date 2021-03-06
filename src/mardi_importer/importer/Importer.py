#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 18:53:53 2022

@author: alvaro
"""


class Importer:
    """Controller class for importing data from an external source to the local Wikibase."""

    def __init__(self, entityCreator, dataSource):
        """
        Construct.
        Args:
            entityCreator: object implementing AEntityCreator
            dataSource: object implementig ADataSource
        """
        self.entityCreator = entityCreator
        self.dataSource = dataSource

    def import_all(self):
        """
        Manages the import process.
        """
        self.entityCreator.import_entities()
        self.entityCreator.create_entities()

        self.dataSource.pull()
        self.dataSource.push()


class AEntityCreator:
    """Abstract base class for creating entities in a local Wikibase."""

    def import_entities(self):
        """
        Imports all necessary entities from Wikidata in the local Wikibase.

        Returns:
            Pandas dataframe: Mapping between local Wikibase and Wikidata.
        """
        raise NotImplementedError
    
    def create_entities(self):
        """
        Creates all necessary additional entities in the local Wikibase.
        """
        raise NotImplementedError


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
