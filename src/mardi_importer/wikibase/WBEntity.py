#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIConnection
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
import configparser


class WBEntity:
    def __init__(self, label):
        self.label = label
        self.description = ""
        self.claims = []

        config = configparser.ConfigParser()
        config.sections()
        config.read("/config/credentials.ini")
        username = config["default"]["username"]
        botpwd = config["default"]["password"]
        WIKIBASE_API = config["default"]["WIKIBASE_API"]
        self.wb_connection = WBAPIConnection(username, botpwd, WIKIBASE_API)

    def add_description(self, description):
        self.description = description
        return self

    def add_statement(self, property, value, **qualifiers):
        if qualifiers:
            qualifiers = self.process_qualifiers(qualifiers)
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        data_type = self.get_datatype(property)
        if data_type == "string" or data_type == "url":
            statement = {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {"type": "string", "value": value},
                },
                "type": "statement",
                "rank": "normal",
            }
        elif data_type == "wikibase-item":
            if value[0:3] == "WD_":
                value = get_wbs_local_id(value[3:])
            statement = {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {
                        "type": "wikibase-entityid",
                        "value": {"entity-type": "item", "id": value},
                    },
                },
                "type": "statement",
                "rank": "normal",
            }
        elif data_type == "time":
            statement = {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {
                        "type": "time",
                        "value": {
                            "time": value,
                            "precision": 11,
                            "timezone": 0,
                            "before": 0,
                            "after": 0,
                            "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                        },
                    },
                },
                "type": "statement",
                "rank": "normal",
            }
        elif data_type == "external-id":
            statement = {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {
                        "type": "string",
                        "value": value,
                    },
                },
                "rank": "normal",
                "type": "statement",
            }

        if qualifiers:
            statement["qualifiers"] = qualifiers

        self.claims.append(statement)
        return self

    def create(self):
        pass

    def update(self):
        qid = self.exists()
        data = {}
        data["claims"] = self.claims
        return self.wb_connection.edit_entity(qid, data)

    def exists(self):
        pass

    def process_qualifiers(self, qualifiers):
        qualifier_dict = {}
        for key, value in qualifiers.items():
            if key[0:3] == "WD_":
                property_qualifier = get_wbs_local_id(key[3:])
            else:
                property_qualifier = key
            data_type_qualifier = self.get_datatype(property_qualifier)
            if data_type_qualifier == "string":
                qualifier_dict[property_qualifier] = [
                    {
                        "snaktype": "value",
                        "property": property_qualifier,
                        "datatype": "string",
                        "datavalue": {"type": "string", "value": value},
                    }
                ]
            elif data_type_qualifier == "wikibase-item":
                if value[0:3] == "WD_":
                    value = get_wbs_local_id(value[3:])
                qualifier_dict[property_qualifier] = [
                    {
                        "snaktype": "value",
                        "property": property_qualifier,
                        "datatype": "wikibase-item",
                        "datavalue": {
                            "type": "wikibase-entityid",
                            "value": {"entity-type": "item", "id": value},
                        },
                    }
                ]
            elif data_type_qualifier == "time":
                qualifier_dict[property_qualifier] = [
                    {
                        "snaktype": "value",
                        "property": property_qualifier,
                        "datatype": "time",
                        "datavalue": {
                            "value": {
                                "time": value,
                                "timezone": 0,
                                "before": 0,
                                "after": 0,
                                "precision": 11,
                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                            },
                            "type": "time",
                        },
                    }
                ]
        return qualifier_dict

    def get_datatype(self, property):
        params = {"action": "wbgetentities", "ids": property, "props": "datatype"}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if "entities" in r1.json.keys():
            if len(r1.json["entities"]) > 0:
                return r1.json["entities"][property]["datatype"]
        return None
