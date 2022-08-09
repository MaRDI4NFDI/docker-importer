#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIException
from mardi_importer.wikibase.WBEntity import WBEntity
from mardi_importer.wikibase.WBMapping import wb_SQL_query, get_wbs_local_id
import logging
log = logging.getLogger('CRANlogger')


class WBItem(WBEntity):
    """Class to manage local items in the wikibase instance.
    """
    def create(self):
        """Creates a new wikibase item with the instantiated **label**,
        **description** and **statements**.
        """
        data = {"labels": {"en": {"language": "en", "value": self.label}}}
        if len(self.description) > 0:
            data["descriptions"] = {"en": {"language": "en", "value": self.description}}
        data["claims"] = self.claims
        try:
            self.ID = self.wb_connection.create_entity("item", data)
            print(self.ID)
            return self.ID
        except WBAPIException as e:
            log.error(f"Item could not be created through the WB API: {str(e)}")

    def exists(self):
        """Checks if a WB item with the given label already exists.

        Searches for a WB item using the Wikibase API with the instantiated 
        label and returns its ID if a matching result is found.

        Returns: 
          String: ID of the corresponding item, if found. **None** otherwise.
        """
        return self.wb_connection.read_entity_by_title("item", self.label)

    def label_exists(self):
        """Checks if a WB item with the given label already exists in the 
        Wikibase SQL tables.

        Queries the Wikibase SQL tables to search for an WB item with the 
        instantiated label and returns its ID if a matching result is found.

        Returns: 
          String: ID of the corresponding item, if found. **None** otherwise.
        """
        entity_number = wb_SQL_query(self.label, "item")
        if entity_number:
            return entity_number[0]
        return None

    def instance_exists(self, instance):
        """Checks if a WB item with the given label and which is an instance
        of *instance* already exists in the Wikibase SQL tables.

        Queries the Wikibase SQL tables to search for an WB item with the 
        instantiated label. For each found item, it checks whether it has
        the statement instance of *instance* and returns its ID if a matching 
        result is found.

        Returns: 
          String: ID of the corresponding item, if found. **None** otherwise.
        """
        if instance[0:3] == "WD_":
            instance = get_wbs_local_id(instance[3:])
        entity_number = wb_SQL_query(self.label, "item")
        for ID in entity_number:
            item = WBItem(ID=ID)
            item_array = item.get_value('WD_P31')
            for instance_item in item_array:
                if instance_item == instance:
                    return ID
        return None

    def instance_property_exists(self, instance, property, value):
        """Abstract method for checking the existence of an entity
        with the same label and that is an instance of *instance* 
        and with given *property* and *value*.

        Specific methods are defined for *Items* and *Properties*.
        """
        if instance[0:3] == "WD_":
            instance = get_wbs_local_id(instance[3:])
        entity_number = wb_SQL_query(self.label, "item")
        for ID in entity_number:
            item = WBItem(ID=ID)
            item_array = item.get_value('WD_P31')
            for instance_item in item_array:
                if instance_item == instance:
                    value_array = item.get_value(property)
                    for value_item in value_array:
                        if value_item == value:
                            return ID
        return None
