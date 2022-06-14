#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBEntity import WBEntity
from mardi_importer.wikibase.WBMapping import wb_SQL_query


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
        return self.wb_connection.create_entity("item", data)

    def exists(self):
        """Checks if a WB item with the given label already exists.

        Searches for a WB item with the instantiated label and returns its ID
        if a matching result is found.

        Returns: 
          String: ID of the corresponding item, if found. **None** otherwise.
        """
        return self.wb_connection.read_entity_by_title("item", self.label)

    def SQL_exists(self):
        entity_number = wb_SQL_query(self.label, "item")
        if entity_number:
            return "Q" + str(entity_number) 
        return None
