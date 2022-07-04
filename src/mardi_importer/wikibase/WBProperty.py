#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIException
from mardi_importer.wikibase.WBEntity import WBEntity
from mardi_importer.wikibase.WBMapping import wb_SQL_query

class WBProperty(WBEntity):
    """Class to manage local properties in the wikibase instance.

    Attributes:
        datatype (String): Data type of the property (e.g. time, 
            wikibase-entity, etc.)
    """
    def __init__(self, label):
        self.datatype = ""
        super(WBProperty, self).__init__(label)

    def create(self):
        """Creates a new wikibase property with the instantiated **label**,
        **description**, **datatype** and **statements**.
        """
        data = {"labels": {"en": {"language": "en", "value": self.label}}}
        if len(self.description) > 0:
            data["descriptions"] = {"en": {"language": "en", "value": self.description}}
        data["datatype"] = self.datatype
        data["claims"] = self.claims
        try:
            return self.wb_connection.create_entity("property", data)
        except WBAPIException as e:
            log.error(f"Property could not be created through the WB API: {str(e)}")

    def exists(self):
        """Checks if a WB property with the given label already exists.

        Searches for a WB property with the instantiated label and returns **True**
        if a matching result is found.

        Returns: 
          String: ID of the corresponding property, if found. **None** otherwise.
        """
        return self.wb_connection.read_entity_by_title("property", self.label)

    def SQL_exists(self):
        """Checks if a WB property with the given label already exists in the 
        Wikibase SQL tables.

        Queries the Wikibase SQL tables to search for an WB property with the 
        instantiated label and returns its ID if a matching result is found.

        Returns: 
          String: ID of the corresponding property, if found. **None** otherwise.
        """
        entity_number = wb_SQL_query(self.label, "property")
        if entity_number:
            return "P" + str(entity_number) 
        return None

    def add_datatype(self, datatype):
        """Adds the data type to the instantiated property.

        Args:
            datatype (String): Data type (e.g. string, time, etc.).

        Returns: 
          WBProperty: Property
        """
        self.datatype = datatype
        return self
