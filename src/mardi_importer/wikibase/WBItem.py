#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBEntity import WBEntity


class WBItem(WBEntity):
    def create(self):
        data = {"labels": {"en": {"language": "en", "value": self.label}}}
        if len(self.description) > 0:
            data["descriptions"] = {"en": {"language": "en", "value": self.description}}
        data["claims"] = self.claims
        return self.wb_connection.create_entity("item", data)

    def exists(self):
        return self.wb_connection.read_entity_by_title("item", self.label)
