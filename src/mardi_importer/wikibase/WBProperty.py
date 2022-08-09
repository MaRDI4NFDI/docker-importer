#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIException
from mardi_importer.wikibase.WBEntity import WBEntity
from mardi_importer.wikibase.WBMapping import wb_SQL_query
import logging
log = logging.getLogger('CRANlogger')

class WBProperty(WBEntity):
    """Class to manage local properties in the wikibase instance.

    Attributes:
        datatype (String): Data type of the property (e.g. time, 
            wikibase-entity, etc.)
    """
    def __init__(self, label=None, *, ID=None):
        super(WBProperty, self).__init__(label)
        self.datatype = ""
        self.assign_ID(ID)

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
            self.ID = self.wb_connection.create_entity("property", data)
            return self.ID
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

    def label_exists(self):
        """Checks if a WB property with the given label already exists in the 
        Wikibase SQL tables.

        Queries the Wikibase SQL tables to search for an WB property with the 
        instantiated label and returns its ID if a matching result is found.

        Returns: 
          String: ID of the corresponding property, if found. **None** otherwise.
        """
        entity_number = wb_SQL_query(self.label, "property")
        if entity_number:
            return entity_number[0]
        return None

    def instance_exists(self, instance):
        """Checks if a WB Property with the given label and which is an instance
        of *instance* already exists in the Wikibase SQL tables.

        Queries the Wikibase SQL tables to search for an WB property with the 
        instantiated label. For each found property, it checks whether it has
        the statement instance of *instance* and returns its ID if a matching 
        result is found.

        Returns: 
          String: ID of the corresponding property, if found. **None** otherwise.
        """
        if instance[0:3] == "WD_":
            instance = get_wbs_local_id(instance[3:])
        entity_number = wb_SQL_query(self.label, "property")
        for ID in entity_number:
            property = WBProperty(ID=ID)
            property_array = property.get_value('WD_P31')
            for instance_property in property_array:
                if instance_property == instance:
                    return ID
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

    def assign_ID(self, ID):
        """Returns the ID of the instantiated property through an SQL query to the 
        Wikibase tables.

        The method is called during the instantiation of a property. If the property
        is declared using a label, the method returns the ID for that property, if
        it already exists. If the property is declared already with an ID, the label
        corresponding to that ID is assigned to the corresponding attribute of the
        instantiated property.

        Args:
            id (String): Optional

        Returns:
            String: Property ID
        """
        if ID:
            self.ID = ID
            self.label = self.get_label_by_ID()
        else:
            self.ID = self.label_exists()
            