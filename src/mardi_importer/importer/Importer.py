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
        self.entityCreator.create_entities()
        self.dataSource.pull()


class AEntityCreator:
    """Abstract base class for creating entities in a local Wikibase."""

    def create_entities(self):
        """
        Creates all necessary entities in the local Wikibase.

        Returns:
            pandas data frame of mappings between local Wikibase and Wikidata (or external Wikibase)
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


class ImporterException(Exception):
    """Failed importer operation."""

    pass