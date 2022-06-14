#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIConnection
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
import configparser


class WBEntity:
    """
    Wikibase parent class to provide base functionaliy to 
    :class:`mardi_importer.wikibase.WBItem` and
    :class:`mardi_importer.wikibase.WBProperty`. 
    Each entity must be instantiated with a label.

    Establishes a connection to the local Wikibase instance using
    :class:`mardi_importer.wikibase.WBAPIConnection`.

    Attributes:
        label (String): Entity label.
        description (String): Entity description.
        claims (List): Statements of the instantiated entity, i.e. 
            list of properties and values describing the entity.
        wb_connection: Connection to the Wikibase API.
    """
    def __init__(self, label=None, *, id=None):
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
        self.id = self.get_ID(id)

    def add_description(self, description):
        """Adds a description to the instantiated entity.

        Args:
            description (String): Description of the entity.

        Returns:
            WBEntity: Entity
        """
        self.description = description
        return self

    def get_description(self):
        params = {"action": "wbgetentities", "ids": self.id}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if "entities" in r1.json.keys():
            if len(r1.json["entities"][self.id]["descriptions"]) > 0:
                return r1.json["entities"][self.id]["descriptions"]["en"]["value"]
        return None

    def add_statement(self, property, value, **qualifiers):
        """Adds a statement to the instantiated entity.

        A statement consists of a property and its value. 
        Additional qualifiers can be added to the statement. 

        Args:
            Property (String): Property ID in the local wikibase instance.
              Wikidata ID can also be used using the prefix *WD_*.
            Value: Value corresponding to the indicated property, in the
              corresponding format (i.e. string, time, wikibase-entity, ...)
            Qualifiers: Qualifiers to the statement. These must be introduced
              following the format *property=value*, where in turn the property
              is introduced using its ID, prefixed with *WD_* if necessary.

        Returns:
            WBEntity: Entity
        """
        statement = self.return_statement(property, value)

        if qualifiers:
            qualifiers = self.process_qualifiers(qualifiers)
            statement["qualifiers"] = qualifiers

        if not self.statement_exists(property,value):
            self.claims.append(statement)

        return self

    def return_statement(self, property, value):
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        data_type = self.wb_connection.get_datatype(property)
        if data_type == "string" or data_type == "url":
            return {
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
            return {
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
            return {
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
            return {
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
        return None
    
    def statement_exists(self, property, value):
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        values = self.get_value(property)
        if value[0:3] == "WD_":
            value = get_wbs_local_id(value[3:])
        for value_item in values:
            if value_item == value:
                return True
        return False

    def exists(self):
        """Abstract method for checking the existence of an entity.
        """
        pass

    def create(self):
        """Abstract method for creating entities in a local Wikibase.

        Method for creating Items or Properties is defined in the
        corresponding subclasses.
        """
        pass

    def update(self):
        """Updates an existing entity with additional statements.

        In order for the method to update the entity with a new statement, 
        this must have been previously added with :meth:`add_statement`.

        Returns:
            String: ID of the new entity
        """
        data = {}
        if len(self.description) > 0:
            data["descriptions"] = {"en": {"language": "en", "value": self.description}}
        if len(self.claims) > 0:
            data["claims"] = self.claims
        return self.wb_connection.edit_entity(self.id, data)

    def remove_claim(self,claim_guid):
        if type(claim_guid) is not list:
            claim_guid = [claim_guid]
        for guid in claim_guid:
            self.wb_connection.remove_claim(guid)

    def update_claim(self, claim_guid, statement):
        claim = {"id":claim_guid,
                 "type":"claim"}
        claim['mainsnak'] = statement['mainsnak']
        return self.wb_connection.edit_claim(claim)

    def get_claim_guid(self, property):
        params = {"action": "wbgetentities", "ids": self.id, "props": "claims"}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        data_type = self.wb_connection.get_datatype(property)
        values = []
        if "entities" in r1.json.keys():
            if property in r1.json["entities"][self.id]["claims"].keys():
                for statement in r1.json["entities"][self.id]["claims"][property]:
                    values.append(statement['id'])
        return values

    def get_ID(self, id):
        if id:
            self.id = id
            self.label = self.get_label_by_ID()
        else:
            self.id = self.SQL_exists()
        return self.id

    def get_label_by_ID(self):
        params = {
            'action': 'wbsearchentities',
            'search': self.id,
            'language': 'en',
            'type': 'item',
            'limit': 1
        }
        r1 = self.wb_connection.session.post(self.wb_connection.WIKIBASE_API, data=params)
        r1.json = r1.json()
        if 'search' in r1.json.keys():
            if len(r1.json['search']) > 0:
                for matches in r1.json['search']:
                    return matches['label']
        return None

    def get_value(self, property):
        params = {"action": "wbgetentities", "ids": self.id, "props": "claims"}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        data_type = self.wb_connection.get_datatype(property)
        values = []
        if "entities" in r1.json.keys():
            if property in r1.json["entities"][self.id]["claims"].keys():
                for statement in r1.json["entities"][self.id]["claims"][property]:
                    if data_type == "string" or data_type == "url" or data_type == "external-id":
                        values.append(statement['mainsnak']['datavalue']['value'])
                    elif data_type == "wikibase-item":
                        value_id = statement['mainsnak']['datavalue']['value']['id']
                        values.append(value_id)
                        #params = {"action": "wbgetentities", "ids": value_id, "props": "labels"}
                        #r2 = self.wb_connection.session.post(
                        #    self.wb_connection.WIKIBASE_API, data=params
                        #)
                        #r2.json = r2.json()
                        #if "entities" in r2.json.keys():
                        #    values.append(r2.json['entities'][value_id]['labels']['en']['value'])
                    elif data_type == "time":
                        values.append(statement['mainsnak']['datavalue']['value']['time'][1:11])
        return values

    def process_qualifiers(self, qualifiers):
        """Processes the qualifiers that can optionally be added to each
        statement.

        Qualifiers are passed using the nomenclature *property=value*. 
        Properties are indicated using the ID in the local wikibase. The prefix
        *WD_* can be used to indicate that the ID refers to the Wikidata instance.

        Returns:
            dict: Qualifier dictionary following the Wikibase API nomenclature.
        """
        qualifier_dict = {}
        for key, value in qualifiers.items():
            if key[0:3] == "WD_":
                property_qualifier = get_wbs_local_id(key[3:])
            else:
                property_qualifier = key
            data_type_qualifier = self.wb_connection.get_datatype(property_qualifier)
            if data_type_qualifier == "string" or data_type_qualifier == "url":
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
