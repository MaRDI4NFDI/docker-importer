#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBEntity import WBEntity


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

        Searches for a WB item with the instantiated label and returns **True**
        if a matching result is found.

        Returns: 
          Boolean: **True** if item exists, **False** otherwise.
        """
        return self.wb_connection.read_entity_by_title("item", self.label)
